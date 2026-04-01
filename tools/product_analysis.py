"""
product_analysis.py
BCG matrix classification, top/bottom products, revenue concentration,
margin analysis, and product promotion/drop recommendations.

Data sources:
  - order_details (line items: product_id, sub_product_id, price, quantity, cost_price)
  - orders (status, created_at — filter to delivered)
  - products (name, category, price, cost_price, stock, is_active)
  - sub_products (variants with own pricing)
  - shops (vendor name)
  - categories (category name)

Outputs:
  .tmp/bcg_matrix.csv            — product BCG classification
  .tmp/top_products.csv          — top 50 products by revenue
  .tmp/product_recommendations.json — promote/drop/maintain lists
  .tmp/category_performance.csv  — revenue + margin by category
"""

import os, sys, json
import pandas as pd
import numpy as np
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(__file__))
from db_connect import query_df

load_dotenv()
os.makedirs(".tmp", exist_ok=True)


# ─── 1. LOAD DATA ────────────────────────────────────────────────────────────

def load_order_items():
    """Join order_details with orders (delivered only) + product metadata."""
    sql = """
        SELECT
            od.id           AS line_id,
            od.order_id,
            od.product_id,
            od.sub_product_id,
            od.price        AS sold_price,
            od.cost_price   AS cost,
            od.quantity,
            od.price * od.quantity                              AS line_revenue,
            (od.price - IFNULL(od.cost_price,0)) * od.quantity AS line_margin,
            o.created_at    AS order_date,
            o.shop_id,
            o.category_id,
            p.name_en       AS product_name,
            p.is_active     AS product_active,
            IFNULL(sp.name_en, '')  AS sub_product_name,
            s.name_en       AS shop_name,
            c.name_en       AS category_name
        FROM order_details od
        JOIN orders o
          ON od.order_id = o.id
         AND o.status = 3
         AND o.payment_status = 'completed'
        LEFT JOIN products p       ON od.product_id = p.id AND p.deleted_at IS NULL
        LEFT JOIN sub_products sp  ON od.sub_product_id = sp.id
        LEFT JOIN shops s          ON o.shop_id = s.id
        LEFT JOIN categories c     ON o.category_id = c.id
    """
    return query_df(sql)


def load_all_products():
    """Full product catalog with current pricing."""
    sql = """
        SELECT
            p.id, p.name_en, p.price, p.cost_price, p.stock,
            p.is_active, p.is_approved, p.shop_id, p.category_id,
            p.created_at, p.deleted_at,
            s.name_en  AS shop_name,
            c.name_en  AS category_name
        FROM products p
        LEFT JOIN shops s      ON p.shop_id = s.id
        LEFT JOIN categories c ON p.category_id = c.id
        WHERE p.deleted_at IS NULL
    """
    return query_df(sql)


# ─── 2. BCG MATRIX ──────────────────────────────────────────────────────────

def compute_bcg(df_items):
    """
    BCG Matrix per product:
      Market Share proxy  = product's revenue share within its category
      Growth proxy        = revenue in last 30 days vs prior 30 days

    Stars       — high share, high growth
    Cash Cows   — high share, low/negative growth
    Question ?  — low share, high growth
    Dogs        — low share, low growth → candidates for dropping
    """
    df = df_items.copy()
    df['order_date'] = pd.to_datetime(df['order_date'])

    ref_date  = df['order_date'].max()
    cutoff_30 = ref_date - pd.Timedelta(days=30)
    cutoff_60 = ref_date - pd.Timedelta(days=60)

    # Revenue per product
    product_rev = df.groupby('product_id').agg(
        total_revenue=('line_revenue', 'sum'),
        total_units=('quantity', 'sum'),
        total_orders=('order_id', 'nunique'),
        avg_margin=('line_margin', 'mean'),
        product_name=('product_name', 'first'),
        shop_name=('shop_name', 'first'),
        category_name=('category_name', 'first'),
        product_active=('product_active', 'first'),
    ).reset_index()

    # Last-30-day revenue
    rev_30 = df[df['order_date'] >= cutoff_30].groupby('product_id')['line_revenue'].sum().reset_index()
    rev_30.columns = ['product_id', 'rev_last_30']

    # Prior-30-day revenue (30-60 days ago)
    rev_30_60 = df[(df['order_date'] >= cutoff_60) & (df['order_date'] < cutoff_30)].groupby(
        'product_id')['line_revenue'].sum().reset_index()
    rev_30_60.columns = ['product_id', 'rev_prior_30']

    product_rev = product_rev.merge(rev_30, on='product_id', how='left')
    product_rev = product_rev.merge(rev_30_60, on='product_id', how='left')
    product_rev['rev_last_30']  = product_rev['rev_last_30'].fillna(0)
    product_rev['rev_prior_30'] = product_rev['rev_prior_30'].fillna(0)

    # ── Gap detection: if the prior window has almost no data (< 5% of recent),
    #    fall back to same-period one year ago for a meaningful growth comparison.
    total_recent = product_rev['rev_last_30'].sum()
    total_prior  = product_rev['rev_prior_30'].sum()
    use_yoy = (total_prior < total_recent * 0.05) and (total_recent > 0)

    if use_yoy:
        # Same 30-day window one year back
        yoy_start = cutoff_30 - pd.DateOffset(years=1)
        yoy_end   = ref_date  - pd.DateOffset(years=1)
        rev_yoy = df[(df['order_date'] >= yoy_start) & (df['order_date'] < yoy_end)].groupby(
            'product_id')['line_revenue'].sum().reset_index()
        rev_yoy.columns = ['product_id', 'rev_yoy_30']
        product_rev = product_rev.merge(rev_yoy, on='product_id', how='left')
        product_rev['rev_yoy_30'] = product_rev['rev_yoy_30'].fillna(0)
        product_rev['growth_rate'] = np.where(
            product_rev['rev_yoy_30'] > 0,
            (product_rev['rev_last_30'] - product_rev['rev_yoy_30']) / product_rev['rev_yoy_30'] * 100,
            np.where(product_rev['rev_last_30'] > 0, 100.0, 0.0)
        )
        product_rev['growth_basis'] = 'YoY (same period last year — consecutive window was in data gap)'
        print(f"  [BCG] Prior-30-day window fell inside data gap — using YoY growth basis instead.")
    else:
        # Normal: consecutive 30-day comparison
        product_rev['growth_rate'] = np.where(
            product_rev['rev_prior_30'] > 0,
            (product_rev['rev_last_30'] - product_rev['rev_prior_30']) / product_rev['rev_prior_30'] * 100,
            np.where(product_rev['rev_last_30'] > 0, 100.0, 0.0)
        )
        product_rev['growth_basis'] = 'MoM (last 30 days vs prior 30 days)'

    # Market share within category
    cat_rev = product_rev.groupby('category_name')['total_revenue'].sum().reset_index()
    cat_rev.columns = ['category_name', 'cat_total_revenue']
    product_rev = product_rev.merge(cat_rev, on='category_name', how='left')
    product_rev['market_share_pct'] = (
        product_rev['total_revenue'] / product_rev['cat_total_revenue'] * 100
    ).round(2)

    # BCG classification
    # Since orders table only covers ~3 weeks, growth_rate has insufficient history.
    # Proxy: market_share = relative category dominance (X-axis)
    #        vitality = order_count × avg_margin (Y-axis: value + demand combined)
    product_rev['vitality'] = product_rev['total_orders'] * product_rev['avg_margin'].clip(lower=0)

    share_median    = product_rev['market_share_pct'].median()
    vitality_median = product_rev['vitality'].median()

    def bcg_label(row):
        high_share    = row['market_share_pct'] >= share_median
        high_vitality = row['vitality'] >= vitality_median
        if high_share and high_vitality:       return 'Star'
        elif high_share and not high_vitality: return 'Cash Cow'
        elif not high_share and high_vitality: return 'Question Mark'
        else:                                  return 'Dog'

    product_rev['bcg_quadrant'] = product_rev.apply(bcg_label, axis=1)

    return product_rev


# ─── 3. RECOMMENDATIONS ──────────────────────────────────────────────────────

def build_recommendations(df_bcg, df_all_products):
    """
    Promote  — Stars + Question Marks with good margin
    Maintain — Cash Cows
    Drop     — Dogs with low/no revenue + inactive products
    Review   — Dogs with declining margin
    """
    promote = df_bcg[
        (df_bcg['bcg_quadrant'].isin(['Star', 'Question Mark'])) &
        (df_bcg['avg_margin'] > 0)
    ][['product_id', 'product_name', 'shop_name', 'category_name',
       'bcg_quadrant', 'total_revenue', 'growth_rate', 'avg_margin']].head(30)

    maintain = df_bcg[
        df_bcg['bcg_quadrant'] == 'Cash Cow'
    ][['product_id', 'product_name', 'shop_name', 'category_name',
       'total_revenue', 'market_share_pct', 'avg_margin']].head(20)

    drop = df_bcg[
        (df_bcg['bcg_quadrant'] == 'Dog') &
        (df_bcg['total_revenue'] < df_bcg['total_revenue'].quantile(0.25))
    ][['product_id', 'product_name', 'shop_name', 'category_name',
       'total_revenue', 'growth_rate']].head(30)

    # Products in catalog with ZERO orders (never sold)
    sold_ids = set(df_bcg['product_id'].dropna().astype(int))
    never_sold = df_all_products[~df_all_products['id'].isin(sold_ids) &
                                  (df_all_products['is_active'] == 1)][
        ['id', 'name_en', 'shop_name', 'category_name', 'price', 'stock']
    ].head(30)

    recs = {
        "promote": promote.to_dict(orient='records'),
        "maintain": maintain.to_dict(orient='records'),
        "drop_candidates": drop.to_dict(orient='records'),
        "never_sold_active": never_sold.to_dict(orient='records'),
    }
    return recs


# ─── 4. CATEGORY PERFORMANCE ─────────────────────────────────────────────────

def category_performance(df_items):
    cat = df_items.groupby('category_name').agg(
        total_revenue=('line_revenue', 'sum'),
        total_orders=('order_id', 'nunique'),
        total_units=('quantity', 'sum'),
        avg_margin=('line_margin', 'mean'),
        unique_products=('product_id', 'nunique'),
    ).round(2).reset_index().sort_values('total_revenue', ascending=False)
    cat['revenue_share_pct'] = (cat['total_revenue'] / cat['total_revenue'].sum() * 100).round(1)
    return cat


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def run():
    print("Loading order line items...")
    df_items = load_order_items()
    print(f"  {len(df_items):,} line items loaded")

    print("Loading full product catalog...")
    df_all = load_all_products()
    print(f"  {len(df_all):,} active products in catalog")

    print("Computing BCG matrix...")
    df_bcg = compute_bcg(df_items)

    print("Building recommendations...")
    recs = build_recommendations(df_bcg, df_all)

    print("Computing category performance...")
    df_cat = category_performance(df_items)

    # Save outputs
    df_bcg.to_csv(".tmp/bcg_matrix.csv", index=False)
    df_bcg.nlargest(50, 'total_revenue').to_csv(".tmp/top_products.csv", index=False)
    df_cat.to_csv(".tmp/category_performance.csv", index=False)
    with open(".tmp/product_recommendations.json", "w") as f:
        json.dump(recs, f, indent=2, default=str)

    print("\n=== BCG QUADRANT SUMMARY ===")
    print(df_bcg.groupby('bcg_quadrant').agg(
        products=('product_id', 'count'),
        total_revenue=('total_revenue', 'sum'),
        avg_margin=('avg_margin', 'mean')
    ).round(0))

    print("\n=== CATEGORY REVENUE SHARE ===")
    print(df_cat[['category_name', 'total_revenue', 'revenue_share_pct',
                  'total_orders', 'avg_margin']].head(10).to_string(index=False))

    print(f"\n  Promote: {len(recs['promote'])} products")
    print(f"  Maintain: {len(recs['maintain'])} products")
    print(f"  Drop candidates: {len(recs['drop_candidates'])} products")
    print(f"  Never sold (active): {len(recs['never_sold_active'])} products")

    print("\nOutputs saved to .tmp/")
    return df_bcg, df_cat, recs


if __name__ == "__main__":
    run()
