"""
shop_analysis.py
Vendor (shop) performance analysis: revenue, margins, ratings,
commission contribution, fulfilment quality signals, and ranking.

Outputs:
  .tmp/shop_performance.csv    — per-shop KPIs
  .tmp/shop_rankings.json      — top/bottom vendors with narrative
"""

import os, sys, json
import pandas as pd
import numpy as np
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(__file__))
from db_connect import query_df, load_synthetic_orders_df

load_dotenv()
os.makedirs(".tmp", exist_ok=True)


def load_shop_orders():
    sql = """
        SELECT
            o.shop_id,
            s.name_en                   AS shop_name,
            s.name                      AS shop_name_ar,
            s.type                      AS shop_type,
            s.rating                    AS shop_rating,
            s.is_zabehaty               AS is_own_brand,
            s.zabehaty_percentage       AS commission_pct,
            s.is_active                 AS shop_active,
            o.id                        AS order_id,
            o.total,
            o.subtotal,
            o.discount_total,
            o.delivery,
            o.service_fee,
            o.status,
            o.payment_status,
            o.payment_method,
            o.created_at,
            o.rating                    AS order_rating
        FROM orders o
        LEFT JOIN shops s ON o.shop_id = s.id
        WHERE o.shop_id IS NOT NULL
    """
    df_live = query_df(sql)
    df_synth = load_synthetic_orders_df()
    if not df_synth.empty and "shop_id" in df_synth.columns:
        shop_meta = df_live[["shop_id","shop_name","shop_type","shop_rating",
                              "is_own_brand","commission_pct","shop_active"]].drop_duplicates("shop_id")
        df_synth = df_synth.merge(shop_meta, on="shop_id", how="left")
        df_synth = df_synth.rename(columns={"id": "order_id"})
        df_synth["status"] = 3
        df_synth["payment_status"] = "completed"
        df_synth["order_rating"] = None
        shared_cols = [c for c in df_live.columns if c in df_synth.columns]
        df_live = pd.concat([df_live, df_synth[shared_cols]], ignore_index=True)
    return df_live


def load_shop_products():
    """Count active/inactive products per shop."""
    sql = """
        SELECT
            shop_id,
            COUNT(*)                            AS total_products,
            SUM(CASE WHEN is_active=1 THEN 1 ELSE 0 END) AS active_products,
            SUM(CASE WHEN stock <= 0 THEN 1 ELSE 0 END)  AS out_of_stock
        FROM products
        WHERE deleted_at IS NULL
        GROUP BY shop_id
    """
    return query_df(sql)


def run():
    print("Loading shop order data...")
    df = load_shop_orders()
    print(f"  {len(df):,} orders with shop data")

    print("Loading shop product data...")
    df_prod = load_shop_products()

    # ── Per-shop KPIs ────────────────────────────────────────────────────────
    delivered = df[df['status'] == 3]
    cancelled = df[df['status'].isin([4, 5, 8])]

    perf = delivered.groupby(['shop_id', 'shop_name', 'shop_type',
                               'shop_rating', 'is_own_brand',
                               'commission_pct', 'shop_active']).agg(
        total_orders=('order_id', 'nunique'),
        gross_revenue=('total', 'sum'),
        avg_order_value=('total', 'mean'),
        total_discounts=('discount_total', 'sum'),
        avg_order_rating=('order_rating', 'mean'),
        # Sentiment derived from star rating (no review_classification dependency)
        positive_reviews=('order_rating', lambda x: (pd.to_numeric(x, errors='coerce') >= 4).sum()),
        negative_reviews=('order_rating', lambda x: ((pd.to_numeric(x, errors='coerce') > 0) & (pd.to_numeric(x, errors='coerce') < 3)).sum()),
        rated_orders=('order_rating',     lambda x: (pd.to_numeric(x, errors='coerce') > 0).sum()),
    ).reset_index()

    # Cancel rate
    cancel_rate = cancelled.groupby('shop_id')['order_id'].nunique().reset_index()
    cancel_rate.columns = ['shop_id', 'cancelled_orders']
    total_orders = df.groupby('shop_id')['order_id'].nunique().reset_index()
    total_orders.columns = ['shop_id', 'all_orders']
    cancel_info = cancel_rate.merge(total_orders, on='shop_id', how='right').fillna(0)
    cancel_info['cancel_rate_pct'] = (
        cancel_info['cancelled_orders'] / cancel_info['all_orders'] * 100
    ).round(1)

    perf = perf.merge(cancel_info[['shop_id', 'cancel_rate_pct']], on='shop_id', how='left')

    # Estimated platform commission
    perf['commission_pct'] = perf['commission_pct'].fillna(0)
    perf['estimated_commission'] = (
        perf['gross_revenue'] * perf['commission_pct'] / 100
    ).round(2)

    # Commission type classification
    # Own Brand = Zabehaty direct (no commission); Charity = 100% donation pass-through;
    # Marketplace = third-party shops earning platform commission
    perf['commission_type'] = perf.apply(
        lambda r: "Own Brand" if r['is_own_brand'] == 1
                  else ("Charity" if r['commission_pct'] == 100 else "Marketplace"),
        axis=1
    )

    # Product catalog stats
    perf = perf.merge(df_prod, on='shop_id', how='left')

    # Revenue share
    perf['revenue_share_pct'] = (
        perf['gross_revenue'] / perf['gross_revenue'].sum() * 100
    ).round(1)

    # Sentiment ratio
    total_reviews = perf['positive_reviews'] + perf['negative_reviews']
    perf['sentiment_score'] = np.where(
        total_reviews > 0,
        (perf['positive_reviews'] / total_reviews * 100).round(1),
        None
    )

    # Shop health score (0-100): weighted composite
    perf['health_score'] = (
        perf['revenue_share_pct'].rank(pct=True) * 30 +
        (100 - perf['cancel_rate_pct'].fillna(50)).rank(pct=True) * 25 +
        perf['avg_order_rating'].fillna(0).rank(pct=True) * 25 +
        perf['total_orders'].rank(pct=True) * 20
    ).round(1)

    perf = perf.sort_values('gross_revenue', ascending=False)
    perf.to_csv(".tmp/shop_performance.csv", index=False)

    # ── Rankings JSON ────────────────────────────────────────────────────────
    top5 = perf.head(5)[['shop_id', 'shop_name', 'total_orders', 'gross_revenue',
                           'avg_order_value', 'revenue_share_pct', 'cancel_rate_pct',
                           'sentiment_score', 'health_score']].to_dict(orient='records')
    bottom5 = perf[perf['total_orders'] >= 5].tail(5)[
        ['shop_id', 'shop_name', 'total_orders', 'gross_revenue',
         'cancel_rate_pct', 'health_score']
    ].to_dict(orient='records')
    own_brand = perf[perf['is_own_brand'] == 1][
        ['shop_id', 'shop_name', 'total_orders', 'gross_revenue',
         'revenue_share_pct', 'estimated_commission']
    ].to_dict(orient='records')

    own_brand_rev   = float(perf[perf['commission_type'] == 'Own Brand']['gross_revenue'].sum())
    charity_rev     = float(perf[perf['commission_type'] == 'Charity']['gross_revenue'].sum())
    marketplace_rev = float(perf[perf['commission_type'] == 'Marketplace']['gross_revenue'].sum())
    marketplace_commission = float(perf[perf['commission_type'] == 'Marketplace']['estimated_commission'].sum())

    # Direct Zabehaty revenue (orders with no shop_id — own-brand sold directly)
    direct_sql = """
        SELECT COUNT(DISTINCT id) AS orders, SUM(total) AS revenue, COUNT(DISTINCT user_id) AS users
        FROM orders
        WHERE shop_id IS NULL AND status = 3 AND payment_status = 'completed'
    """
    direct_df = query_df(direct_sql)
    direct_revenue = float(direct_df['revenue'].iloc[0] or 0)
    direct_orders  = int(direct_df['orders'].iloc[0] or 0)
    direct_users   = int(direct_df['users'].iloc[0] or 0)

    shop_attributed_rev = float(perf['gross_revenue'].sum())
    total_gmv = shop_attributed_rev + direct_revenue

    rankings = {
        "top_performers": top5,
        "underperformers": bottom5,
        "own_brand_shops": own_brand,
        "total_shops_analysed": len(perf),
        # Shop-attributed revenue (orders with shop_id)
        "total_platform_revenue_aed": shop_attributed_rev,
        "total_estimated_commission_aed": float(perf['estimated_commission'].sum()),
        "own_brand_revenue_aed": own_brand_rev,
        "charity_passthrough_aed": charity_rev,
        "marketplace_revenue_aed": marketplace_rev,
        "marketplace_commission_earned_aed": marketplace_commission,
        # Direct Zabehaty revenue (orders without shop_id)
        "direct_zabehaty_revenue_aed": direct_revenue,
        "direct_zabehaty_orders": direct_orders,
        "direct_zabehaty_users": direct_users,
        # True GMV = shop revenue + direct revenue
        "total_gmv_aed": total_gmv,
    }
    with open(".tmp/shop_rankings.json", "w") as f:
        json.dump(rankings, f, indent=2, default=str)

    print("\n=== TOP 10 SHOPS BY REVENUE ===")
    print(perf[['shop_name', 'total_orders', 'gross_revenue', 'revenue_share_pct',
                'cancel_rate_pct', 'health_score']].head(10).to_string(index=False))

    print(f"\nShop-attributed revenue (delivered): AED {shop_attributed_rev:,.0f}")
    print(f"Direct Zabehaty revenue (no shop):   AED {direct_revenue:,.0f}")
    print(f"Total GMV:                           AED {total_gmv:,.0f}")
    print(f"Estimated commissions:               AED {perf['estimated_commission'].sum():,.0f}")
    print("\nOutputs saved to .tmp/")

    return perf, rankings


if __name__ == "__main__":
    run()
