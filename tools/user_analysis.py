"""
user_analysis.py
Computes RFM segmentation, historical LTV, predictive LTV, and user cohort data.

Primary data source: user_total_orders (cumulative per-user per-category spend, 2021-2026)
Secondary: orders table (recent Mar 2026 — for recency signals)

Outputs:
  .tmp/rfm_scores.csv         — per-user RFM scores + segment label
  .tmp/ltv_analysis.csv       — per-user historical + predicted LTV
  .tmp/user_segments.json     — segment summary stats
  .tmp/cohort_retention.csv   — monthly cohort retention matrix
"""

import os, sys, json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(__file__))
from db_connect import query_df, load_synthetic_orders_df

load_dotenv()
os.makedirs(".tmp", exist_ok=True)

ANALYSIS_DATE = datetime.now()


# ─── 1. LOAD DATA ────────────────────────────────────────────────────────────

def load_user_spend():
    """Aggregate total spend per user from user_total_orders (lifetime history)."""
    sql = """
        SELECT
            uto.user_id,
            SUM(uto.total)                        AS lifetime_value,
            COUNT(DISTINCT uto.category_id)       AS categories_purchased,
            MIN(uto.created_at)                   AS first_order_date,
            MAX(uto.updated_at)                   AS last_order_date,
            u.created_at                          AS user_created_at,
            u.is_vip,
            u.is_military,
            u.emirate_id,
            u.country_code,
            u.device_type,
            u.referral_id,
            u.special_user_category_id,
            u.is_ban
        FROM user_total_orders uto
        LEFT JOIN `user` u ON uto.user_id = u.id
        WHERE u.is_ban = 0 OR u.is_ban IS NULL
        GROUP BY uto.user_id, u.created_at, u.is_vip, u.is_military,
                 u.emirate_id, u.country_code, u.device_type,
                 u.referral_id, u.special_user_category_id, u.is_ban
    """
    return query_df(sql)


def load_order_frequency():
    """Get per-user order count and last order date from recent orders table + synthetic gap."""
    sql = """
        SELECT
            user_id,
            COUNT(*)                AS recent_order_count,
            MAX(created_at)         AS last_order_recent,
            MIN(created_at)         AS first_order_recent,
            AVG(total)              AS avg_order_value_recent
        FROM orders
        WHERE status = 3
          AND payment_status = 'completed'
        GROUP BY user_id
    """
    df_live = query_df(sql)
    df_synth = load_synthetic_orders_df()
    if not df_synth.empty:
        synth_agg = (df_synth.groupby("user_id")
            .agg(
                s_order_count=("id", "count"),
                s_last_order=("created_at", "max"),
            ).reset_index())
        df_live = df_live.merge(synth_agg, on="user_id", how="left")
        df_live["last_order_recent"] = pd.to_datetime(df_live["last_order_recent"], errors="coerce")
        df_live["s_last_order"]      = pd.to_datetime(df_live["s_last_order"],      errors="coerce")
        df_live["last_order_recent"] = df_live[["last_order_recent", "s_last_order"]].max(axis=1)
        df_live["recent_order_count"] = df_live["recent_order_count"] + df_live["s_order_count"].fillna(0)
        df_live = df_live.drop(columns=["s_order_count", "s_last_order"])
    return df_live


def load_category_spend_breakdown():
    """Per-user spend by category — for cross-category analysis."""
    sql = """
        SELECT
            uto.user_id,
            c.name_en   AS category,
            uto.total   AS spend
        FROM user_total_orders uto
        LEFT JOIN categories c ON uto.category_id = c.id
    """
    return query_df(sql)


# ─── 2. RFM SCORING ──────────────────────────────────────────────────────────

def compute_rfm(df_spend, df_recent):
    """
    Recency  — days since last order (from recent orders, fallback to user_total_orders)
    Frequency — total category count purchased (proxy from user_total_orders)
    Monetary  — lifetime value (AED)
    """
    df = df_spend.copy()

    # Merge recent order signals
    df = df.merge(df_recent[['user_id', 'recent_order_count', 'last_order_recent']],
                  on='user_id', how='left')

    # Best available last order date
    df['last_order_recent'] = pd.to_datetime(df['last_order_recent'])
    df['last_order_date'] = pd.to_datetime(df['last_order_date'])
    df['best_last_order'] = df['last_order_recent'].fillna(df['last_order_date'])

    # Recency in days
    df['recency_days'] = (ANALYSIS_DATE - df['best_last_order']).dt.days
    df['recency_days'] = df['recency_days'].clip(lower=0)

    # Frequency = categories_purchased (lifetime diversity proxy)
    df['frequency'] = df['categories_purchased'].fillna(1)

    # Monetary = lifetime_value
    df['monetary'] = df['lifetime_value'].fillna(0)

    # Score each dimension 1-5 (5 = best)
    df['R'] = pd.qcut(df['recency_days'].rank(method='first'), 5,
                      labels=[5, 4, 3, 2, 1]).astype(int)
    df['F'] = pd.qcut(df['frequency'].rank(method='first'), 5,
                      labels=[1, 2, 3, 4, 5]).astype(int)
    df['M'] = pd.qcut(df['monetary'].rank(method='first'), 5,
                      labels=[1, 2, 3, 4, 5]).astype(int)

    df['RFM_Score'] = df['R'].astype(str) + df['F'].astype(str) + df['M'].astype(str)
    df['RFM_Total'] = df['R'] + df['F'] + df['M']

    # Segment labels
    def label(row):
        r, f, m = row['R'], row['F'], row['M']
        if r >= 4 and f >= 4 and m >= 4:
            return 'Champions'
        elif r >= 3 and f >= 3 and m >= 4:
            return 'Loyal Customers'
        elif r >= 4 and f <= 2:
            return 'New Customers'
        elif r >= 3 and m >= 3:
            return 'Potential Loyalists'
        elif r == 5 and f == 1:
            return 'Promising'
        elif r <= 2 and f >= 3 and m >= 3:
            return 'At Risk'
        elif r <= 2 and f >= 4 and m >= 4:
            return 'Cant Lose Them'
        elif r <= 2 and f <= 2:
            return 'Lost'
        elif r == 3 and f <= 2:
            return 'Need Attention'
        else:
            return 'About to Sleep'

    df['Segment'] = df.apply(label, axis=1)
    return df


# ─── 3. LTV ANALYSIS ─────────────────────────────────────────────────────────

def compute_ltv(df_rfm):
    """
    Historical LTV = lifetime_value from user_total_orders
    Predicted LTV  = simple BG/NBD-inspired model:
                     avg_monthly_spend × predicted_active_months
    """
    df = df_rfm.copy()

    df['first_order_date'] = pd.to_datetime(df['first_order_date'])
    df['tenure_months'] = ((ANALYSIS_DATE - df['first_order_date']).dt.days / 30).clip(lower=1)
    df['avg_monthly_spend'] = df['monetary'] / df['tenure_months']

    # Churn probability: high recency_days → more likely churned
    # P(active) = sigmoid-based decay; 180 days = ~50% churn
    df['p_active'] = 1 / (1 + np.exp((df['recency_days'] - 180) / 60))

    # Predicted value over next 12 months
    df['predicted_ltv_12m'] = df['avg_monthly_spend'] * 12 * df['p_active']

    # LTV tier
    def ltv_tier(val):
        if val >= 5000:   return 'Platinum'
        elif val >= 2000: return 'Gold'
        elif val >= 500:  return 'Silver'
        else:             return 'Bronze'

    df['ltv_tier'] = df['monetary'].apply(ltv_tier)

    return df


# ─── 4. COHORT ANALYSIS ──────────────────────────────────────────────────────

def compute_cohorts(df_ltv):
    """Monthly acquisition cohorts — how many users from each cohort still active."""
    df = df_ltv.copy()
    df['first_order_date'] = pd.to_datetime(df['first_order_date'])
    df['last_order_date']  = pd.to_datetime(df['last_order_date'])

    df = df.dropna(subset=['first_order_date'])
    df['cohort_month'] = df['first_order_date'].dt.to_period('M')
    df['last_active_month'] = df['last_order_date'].dt.to_period('M')

    cohort_size = df.groupby('cohort_month')['user_id'].count().reset_index()
    cohort_size.columns = ['cohort_month', 'cohort_size']

    active = df.groupby(['cohort_month', 'last_active_month'])['user_id'].count().reset_index()
    active.columns = ['cohort_month', 'last_active_month', 'active_users']

    cohort_data = cohort_size.merge(active, on='cohort_month')
    cohort_data['period_offset'] = (
        cohort_data['last_active_month'] - cohort_data['cohort_month']
    ).apply(lambda x: x.n)
    cohort_data['retention_rate'] = (
        cohort_data['active_users'] / cohort_data['cohort_size'] * 100
    ).round(1)

    return cohort_data


# ─── 5. SEGMENT SUMMARY ──────────────────────────────────────────────────────

def build_segment_summary(df_ltv):
    summary = df_ltv.groupby('Segment').agg(
        user_count=('user_id', 'count'),
        avg_ltv=('monetary', 'mean'),
        total_revenue=('monetary', 'sum'),
        avg_recency_days=('recency_days', 'mean'),
        avg_predicted_ltv=('predicted_ltv_12m', 'mean'),
        vip_count=('is_vip', 'sum'),
    ).round(2).reset_index()
    summary['revenue_share_pct'] = (
        summary['total_revenue'] / summary['total_revenue'].sum() * 100
    ).round(1)
    summary = summary.sort_values('total_revenue', ascending=False)
    return summary


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def run():
    print("Loading user spend data...")
    df_spend  = load_user_spend()
    print(f"  {len(df_spend):,} users with purchase history")

    print("Loading recent order signals...")
    df_recent = load_order_frequency()
    print(f"  {len(df_recent):,} users with recent orders")

    print("Computing RFM scores...")
    df_rfm = compute_rfm(df_spend, df_recent)

    print("Computing LTV...")
    df_ltv = compute_ltv(df_rfm)

    print("Computing cohorts...")
    df_cohorts = compute_cohorts(df_ltv)

    print("Building segment summary...")
    df_summary = build_segment_summary(df_ltv)

    # Save outputs
    df_ltv.to_csv(".tmp/rfm_scores.csv", index=False)
    df_ltv[['user_id', 'monetary', 'predicted_ltv_12m', 'ltv_tier',
             'avg_monthly_spend', 'tenure_months', 'p_active']].to_csv(
        ".tmp/ltv_analysis.csv", index=False)
    df_cohorts.to_csv(".tmp/cohort_retention.csv", index=False)
    df_summary.to_json(".tmp/user_segments.json", orient='records', indent=2)

    print("\n=== SEGMENT SUMMARY ===")
    print(df_summary[['Segment', 'user_count', 'avg_ltv', 'revenue_share_pct',
                       'avg_recency_days']].to_string(index=False))

    print("\n=== LTV TIERS ===")
    print(df_ltv.groupby('ltv_tier').agg(
        users=('user_id', 'count'),
        total_revenue=('monetary', 'sum'),
        avg_ltv=('monetary', 'mean')
    ).round(0))

    print("\nOutputs saved to .tmp/")
    return df_ltv, df_summary, df_cohorts


if __name__ == "__main__":
    run()
