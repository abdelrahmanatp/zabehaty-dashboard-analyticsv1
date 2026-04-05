"""
buying_patterns.py
Basket analysis, repeat purchase rates, churn risk scoring,
cross-category affinity, and order timing patterns.

Outputs:
  .tmp/buying_patterns.json    — all pattern insights
  .tmp/churn_risk.csv          — per-user churn risk score
  .tmp/cross_category.csv      — category affinity matrix
"""

import os, sys, json
import pandas as pd
import numpy as np
from itertools import combinations
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(__file__))
from db_connect import query_df, load_synthetic_orders_df

load_dotenv()
os.makedirs(".tmp", exist_ok=True)


def load_user_orders():
    """Per-user order history from user_total_orders + recent orders."""
    sql = """
        SELECT
            uto.user_id,
            uto.category_id,
            c.name_en          AS category_name,
            COALESCE(c.name_ar, c.name_en) AS category_name_ar,
            uto.total          AS spend,
            uto.created_at,
            uto.updated_at
        FROM user_total_orders uto
        LEFT JOIN categories c ON uto.category_id = c.id
    """
    return query_df(sql)


def load_recent_orders():
    """Recent orders with timing signals, augmented with synthetic gap orders."""
    sql = """
        SELECT
            o.user_id,
            o.id        AS order_id,
            o.total,
            o.created_at,
            DAYOFWEEK(o.created_at)   AS day_of_week,
            HOUR(o.created_at)        AS hour_of_day,
            o.category_id,
            c.name_en                 AS category_name,
            o.payment_method,
            o.shop_id
        FROM orders o
        LEFT JOIN categories c ON o.category_id = c.id
        WHERE o.status = 3
          AND o.payment_status = 'completed'
          AND o.user_id IN (SELECT id FROM user WHERE is_ban = 0 OR is_ban IS NULL)
    """
    df_live = query_df(sql)
    df_synth = load_synthetic_orders_df()
    if not df_synth.empty:
        df_synth = df_synth.copy()
        df_synth["created_at"] = pd.to_datetime(df_synth["created_at"])
        df_synth["day_of_week"] = df_synth["created_at"].dt.dayofweek + 1
        df_synth["hour_of_day"] = df_synth["created_at"].dt.hour
        df_synth = df_synth.rename(columns={"id": "order_id"})
        shared_cols = [c for c in df_live.columns if c in df_synth.columns]
        df_live = pd.concat([df_live, df_synth[shared_cols]], ignore_index=True)
    return df_live


# ─── 1. REPEAT PURCHASE RATE ─────────────────────────────────────────────────

def repeat_purchase_analysis(df_hist):
    """What % of users bought more than once (across categories)."""
    user_cats = df_hist.groupby('user_id')['category_id'].nunique().reset_index()
    user_cats.columns = ['user_id', 'category_count']

    one_time = (user_cats['category_count'] == 1).sum()
    repeat   = (user_cats['category_count'] > 1).sum()
    total    = len(user_cats)

    return {
        "total_buyers": int(total),
        "one_category_buyers": int(one_time),
        "multi_category_buyers": int(repeat),
        "repeat_rate_pct": round(repeat / total * 100, 1),
        "avg_categories_per_user": round(user_cats['category_count'].mean(), 2),
    }


# ─── 2. CROSS-CATEGORY AFFINITY ──────────────────────────────────────────────

def cross_category_affinity(df_hist):
    """
    Which categories are most often bought together by the same user.
    Returns top 15 category pairs by co-occurrence count, with both
    English and Arabic category names.
    """
    # Build English pairs
    user_cats_en = df_hist.groupby('user_id')['category_name'].apply(
        lambda x: list(x.dropna().unique())
    ).reset_index()

    pair_counts = {}
    for cats in user_cats_en['category_name']:
        if len(cats) >= 2:
            for pair in combinations(sorted(cats), 2):
                pair_counts[pair] = pair_counts.get(pair, 0) + 1

    pairs_df = pd.DataFrame(
        [(k[0], k[1], v) for k, v in pair_counts.items()],
        columns=['category_a', 'category_b', 'co_buyers']
    ).sort_values('co_buyers', ascending=False).head(15)

    # Build English→Arabic name map from the loaded data
    if 'category_name_ar' in df_hist.columns:
        name_map = (
            df_hist[['category_name', 'category_name_ar']]
            .dropna(subset=['category_name'])
            .drop_duplicates('category_name')
            .set_index('category_name')['category_name_ar']
            .to_dict()
        )
        pairs_df['category_a_ar'] = pairs_df['category_a'].map(name_map).fillna(pairs_df['category_a'])
        pairs_df['category_b_ar'] = pairs_df['category_b'].map(name_map).fillna(pairs_df['category_b'])
    else:
        pairs_df['category_a_ar'] = pairs_df['category_a']
        pairs_df['category_b_ar'] = pairs_df['category_b']

    return pairs_df


# ─── 3. ORDER TIMING PATTERNS ────────────────────────────────────────────────

def timing_patterns(df_recent):
    """Peak days and hours for orders."""
    day_map = {1: 'Sunday', 2: 'Monday', 3: 'Tuesday', 4: 'Wednesday',
               5: 'Thursday', 6: 'Friday', 7: 'Saturday'}

    by_day = df_recent.groupby('day_of_week')['order_id'].count().reset_index()
    by_day['day_name'] = by_day['day_of_week'].map(day_map)
    by_day = by_day.sort_values('order_id', ascending=False)

    by_hour = df_recent.groupby('hour_of_day')['order_id'].count().reset_index()
    by_hour = by_hour.sort_values('order_id', ascending=False)

    return {
        "peak_days": by_day[['day_name', 'order_id']].rename(
            columns={'order_id': 'orders'}).to_dict(orient='records'),
        "peak_hours": by_hour.head(5).rename(
            columns={'order_id': 'orders'}).to_dict(orient='records'),
    }


# ─── 4. CHURN RISK SCORING ───────────────────────────────────────────────────

def churn_risk(df_hist):
    """
    Per-user churn risk score based on:
    - Days since last activity (from user_total_orders.updated_at)
    - Spend trajectory (declining = risk)
    - Single-category users (less sticky = higher risk)
    """
    now = pd.Timestamp.now()
    user_stats = df_hist.groupby('user_id').agg(
        last_active=('updated_at', 'max'),
        total_spend=('spend', 'sum'),
        category_count=('category_id', 'nunique'),
    ).reset_index()

    user_stats['last_active'] = pd.to_datetime(user_stats['last_active'])
    user_stats['days_inactive'] = (now - user_stats['last_active']).dt.days.clip(lower=0)

    # Normalize components
    max_days = user_stats['days_inactive'].max()
    user_stats['recency_score']  = user_stats['days_inactive'] / max_days
    user_stats['breadth_score']  = 1 - (user_stats['category_count'] /
                                          user_stats['category_count'].max())
    spend_rank = user_stats['total_spend'].rank(pct=True)
    user_stats['value_score']    = 1 - spend_rank  # low spenders = higher risk

    user_stats['churn_risk_score'] = (
        user_stats['recency_score'] * 0.5 +
        user_stats['breadth_score'] * 0.3 +
        user_stats['value_score']   * 0.2
    ).round(4)

    def risk_label(score):
        if score >= 0.75: return 'Critical'
        elif score >= 0.55: return 'High'
        elif score >= 0.35: return 'Medium'
        else: return 'Low'

    user_stats['churn_risk_label'] = user_stats['churn_risk_score'].apply(risk_label)

    return user_stats


# ─── 5. PAYMENT METHOD BREAKDOWN ─────────────────────────────────────────────

def payment_breakdown(df_recent):
    method_map = {
        1: 'Cash on Delivery',
        2: 'Credit/Debit Card',
        3: 'Bank Transfer',
        4: 'Wallet',
        5: 'Tamara (BNPL)',
        6: 'Tabby (BNPL)',
        7: 'Apple Pay',
        13: 'Other'
    }
    pay = df_recent.groupby('payment_method').agg(
        orders=('order_id', 'count'),
        revenue=('total', 'sum')
    ).reset_index()
    pay['method_name'] = pay['payment_method'].map(method_map).fillna('Unknown')
    pay['share_pct'] = (pay['orders'] / pay['orders'].sum() * 100).round(1)
    return pay[['method_name', 'orders', 'revenue', 'share_pct']].sort_values(
        'orders', ascending=False).to_dict(orient='records')


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def run():
    print("Loading historical user order data...")
    df_hist = load_user_orders()
    print(f"  {len(df_hist):,} user-category records loaded")

    print("Loading recent orders...")
    df_recent = load_recent_orders()
    print(f"  {len(df_recent):,} delivered orders loaded")

    print("Analysing repeat purchase patterns...")
    repeat_stats = repeat_purchase_analysis(df_hist)

    print("Computing cross-category affinity...")
    df_affinity = cross_category_affinity(df_hist)

    print("Analysing order timing...")
    timing = timing_patterns(df_recent)

    print("Scoring churn risk...")
    df_churn = churn_risk(df_hist)

    print("Analysing payment methods...")
    payments = payment_breakdown(df_recent)

    # Save outputs
    df_churn.to_csv(".tmp/churn_risk.csv", index=False)
    df_affinity.to_csv(".tmp/cross_category.csv", index=False)

    patterns = {
        "repeat_purchase": repeat_stats,
        "cross_category_affinity": df_affinity.to_dict(orient='records'),
        "order_timing": timing,
        "payment_methods": payments,
        "churn_risk_distribution": df_churn['churn_risk_label'].value_counts().to_dict(),
    }
    with open(".tmp/buying_patterns.json", "w") as f:
        json.dump(patterns, f, indent=2, default=str)

    print("\n=== REPEAT PURCHASE ===")
    print(f"  Repeat rate: {repeat_stats['repeat_rate_pct']}%")
    print(f"  Avg categories per user: {repeat_stats['avg_categories_per_user']}")

    print("\n=== TOP CATEGORY PAIRS ===")
    print(df_affinity.head(8).to_string(index=False))

    print("\n=== CHURN RISK ===")
    print(df_churn['churn_risk_label'].value_counts().to_string())

    print("\n=== PEAK ORDER DAYS ===")
    for d in timing['peak_days'][:3]:
        print(f"  {d['day_name']}: {d['orders']} orders")

    print("\nOutputs saved to .tmp/")
    return patterns, df_churn, df_affinity


if __name__ == "__main__":
    run()
