"""
agent_tools.py
Deterministic data-fetch functions for the conversational AI agent.

Each function returns a dict:
{
  "data":    ...,     # the actual result
  "source":  "...",   # table / file the data came from
  "filters": "...",   # WHERE / date range applied
  "formula": "...",   # calculation method
  "sql":     "..."    # exact SQL or "pre-computed from .tmp/file"
}

These are registered as Claude tool_use functions in agent_page.py.
"""

import os, sys, json, io
from datetime import datetime, date, timedelta

import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
from db_connect import query_df

ROOT = os.path.dirname(os.path.dirname(__file__))
TMP  = os.path.join(ROOT, ".tmp")


# ─── helpers ─────────────────────────────────────────────────────────────────

def _tmp(name):
    path = os.path.join(TMP, name)
    if name.endswith(".csv"):
        return pd.read_csv(path) if os.path.exists(path) else pd.DataFrame()
    if name.endswith(".json"):
        if not os.path.exists(path):
            return {}
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return None


def _today():
    return date.today()


def _month_bounds(year=None, month=None):
    """Return (first_day_str, last_day_str) for the given or current month."""
    t = _today()
    y = year  or t.year
    m = month or t.month
    first = date(y, m, 1)
    if m == 12:
        last = date(y + 1, 1, 1) - timedelta(days=1)
    else:
        last = date(y, m + 1, 1) - timedelta(days=1)
    return str(first), str(last)


# ─── 1. Order stats for a date range ─────────────────────────────────────────

def get_order_stats(date_from: str = None, date_to: str = None) -> dict:
    """
    Total delivered orders, revenue, unique customers, and AOV for a date range.
    Defaults to the current calendar month if no dates given.
    """
    if not date_from or not date_to:
        date_from, date_to = _month_bounds()

    sql = """
        SELECT
            COUNT(DISTINCT id)          AS orders,
            SUM(total)                  AS revenue,
            COUNT(DISTINCT user_id)     AS customers,
            AVG(total)                  AS aov,
            SUM(discount_total)         AS total_discounts,
            SUM(delivery)               AS total_delivery_fees,
            SUM(service_fee)            AS total_service_fees
        FROM orders
        WHERE status = 3
          AND payment_status = 'completed'
          AND DATE(created_at) BETWEEN %(d1)s AND %(d2)s
    """
    df = query_df(sql, params={"d1": date_from, "d2": date_to})
    row = df.iloc[0].to_dict() if not df.empty else {}

    return {
        "data": {
            "orders":           int(row.get("orders") or 0),
            "revenue_aed":      round(float(row.get("revenue") or 0), 2),
            "customers":        int(row.get("customers") or 0),
            "aov_aed":          round(float(row.get("aov") or 0), 2),
            "total_discounts":  round(float(row.get("total_discounts") or 0), 2),
            "delivery_fees":    round(float(row.get("total_delivery_fees") or 0), 2),
            "service_fees":     round(float(row.get("total_service_fees") or 0), 2),
        },
        "source":  "orders table, replica_uae database",
        "filters": f"status=3 (delivered), payment_status='completed', date {date_from} to {date_to}",
        "formula": "COUNT(DISTINCT id) for orders; SUM(total) for revenue; AVG(total) for AOV",
        "sql":     sql.strip(),
    }


# ─── 2. Month-over-month trend ────────────────────────────────────────────────

def get_monthly_trend(months: int = 13) -> dict:
    """
    Month-by-month orders, revenue, and customers for the last N months.
    """
    sql = """
        SELECT
            DATE_FORMAT(created_at, '%%Y-%%m')  AS month,
            COUNT(DISTINCT id)                   AS orders,
            SUM(total)                           AS revenue,
            COUNT(DISTINCT user_id)              AS customers,
            AVG(total)                           AS aov
        FROM orders
        WHERE status = 3
          AND payment_status = 'completed'
          AND created_at >= DATE_SUB(CURDATE(), INTERVAL %(n)s MONTH)
        GROUP BY DATE_FORMAT(created_at, '%%Y-%%m')
        ORDER BY month
    """
    df = query_df(sql, params={"n": months})
    records = []
    for _, r in df.iterrows():
        records.append({
            "month":    str(r["month"]),
            "orders":   int(r["orders"]),
            "revenue":  round(float(r["revenue"]), 2),
            "customers":int(r["customers"]),
            "aov":      round(float(r["aov"]), 2),
        })

    return {
        "data":    records,
        "source":  "orders table, replica_uae database",
        "filters": f"status=3, payment_status='completed', last {months} months",
        "formula": "Grouped by calendar month; COUNT(DISTINCT id); SUM(total); AVG(total)",
        "sql":     sql.strip(),
    }


# ─── 3. New first-time buyers by month ───────────────────────────────────────

def get_new_buyers(date_from: str = None, date_to: str = None) -> dict:
    """
    Count of users whose very first completed order falls in the given date range.
    """
    if not date_from or not date_to:
        date_from, date_to = _month_bounds()

    sql = """
        SELECT COUNT(*) AS new_buyers
        FROM (
            SELECT user_id, MIN(created_at) AS first_order
            FROM orders
            WHERE status = 3
              AND payment_status = 'completed'
            GROUP BY user_id
            HAVING DATE(first_order) BETWEEN %(d1)s AND %(d2)s
        ) first_orders
    """
    df = query_df(sql, params={"d1": date_from, "d2": date_to})
    count = int(df.iloc[0]["new_buyers"]) if not df.empty else 0

    return {
        "data":    {"new_buyers": count, "date_from": date_from, "date_to": date_to},
        "source":  "orders table, replica_uae database",
        "filters": f"status=3, payment_status='completed'; first order date between {date_from} and {date_to}",
        "formula": "MIN(created_at) per user_id, then HAVING first_order in range — counts first-time buyers only",
        "sql":     sql.strip(),
    }


# ─── 4. Users who added mobile numbers ───────────────────────────────────────

def get_users_with_phone(date_from: str = None, date_to: str = None) -> dict:
    """
    Count of users who registered (created_at in range) and have a mobile number on file.
    Column in DB is 'mobile' (not 'phone').
    """
    if not date_from or not date_to:
        date_from, date_to = _month_bounds()

    sql = """
        SELECT
            COUNT(*) AS total_registered,
            SUM(CASE WHEN mobile IS NOT NULL AND mobile <> '' THEN 1 ELSE 0 END) AS with_phone
        FROM `user`
        WHERE DATE(created_at) BETWEEN %(d1)s AND %(d2)s
          AND (is_ban = 0 OR is_ban IS NULL)
    """
    df = query_df(sql, params={"d1": date_from, "d2": date_to})
    row = df.iloc[0].to_dict() if not df.empty else {}

    return {
        "data": {
            "total_registered": int(row.get("total_registered") or 0),
            "with_phone":       int(row.get("with_phone") or 0),
        },
        "source":  "user table, replica_uae database",
        "filters": f"created_at between {date_from} and {date_to}, is_ban=0",
        "formula": "COUNT(*) for registrations; SUM(CASE WHEN mobile IS NOT NULL AND mobile <> '') for users with a mobile number",
        "sql":     sql.strip(),
    }


# ─── 5. Repeat orders (users who ordered more than once) ─────────────────────

def get_repeat_order_stats(date_from: str = None, date_to: str = None) -> dict:
    """
    Count of orders from users who have placed more than one order (repeat buyers).
    """
    if not date_from or not date_to:
        date_from, date_to = _month_bounds()

    sql = """
        SELECT
            COUNT(*) AS total_orders,
            SUM(CASE WHEN order_count > 1 THEN 1 ELSE 0 END) AS repeat_orders,
            COUNT(DISTINCT CASE WHEN order_count > 1 THEN user_id END) AS repeat_buyers
        FROM (
            SELECT user_id, COUNT(*) AS order_count
            FROM orders
            WHERE status = 3
              AND payment_status = 'completed'
              AND DATE(created_at) BETWEEN %(d1)s AND %(d2)s
            GROUP BY user_id
        ) per_user
    """
    df = query_df(sql, params={"d1": date_from, "d2": date_to})
    row = df.iloc[0].to_dict() if not df.empty else {}

    return {
        "data": {
            "total_orders":  int(row.get("total_orders") or 0),
            "repeat_orders": int(row.get("repeat_orders") or 0),
            "repeat_buyers": int(row.get("repeat_buyers") or 0),
        },
        "source":  "orders table, replica_uae database",
        "filters": f"status=3, payment_status='completed', date {date_from} to {date_to}",
        "formula": "Sub-query groups by user_id; repeat = users with order_count > 1 in the period",
        "sql":     sql.strip(),
    }


# ─── 6. Top shops by revenue ─────────────────────────────────────────────────

def get_top_shops(date_from: str = None, date_to: str = None, limit: int = 10) -> dict:
    """
    Top shops by revenue in the given date range.
    """
    if not date_from or not date_to:
        date_from, date_to = _month_bounds()

    sql = """
        SELECT
            s.name_en           AS shop,
            s.name              AS shop_ar,
            COUNT(DISTINCT o.id) AS orders,
            SUM(o.total)         AS revenue,
            AVG(o.total)         AS aov,
            s.is_zabehaty        AS is_own_brand
        FROM orders o
        JOIN shops s ON o.shop_id = s.id
        WHERE o.status = 3
          AND o.payment_status = 'completed'
          AND DATE(o.created_at) BETWEEN %(d1)s AND %(d2)s
        GROUP BY s.id, s.name_en, s.name, s.is_zabehaty
        ORDER BY revenue DESC
        LIMIT %(lim)s
    """
    df = query_df(sql, params={"d1": date_from, "d2": date_to, "lim": limit})
    records = []
    for _, r in df.iterrows():
        records.append({
            "shop":         str(r["shop"]),
            "shop_ar":      str(r["shop_ar"]),
            "orders":       int(r["orders"]),
            "revenue_aed":  round(float(r["revenue"]), 2),
            "aov_aed":      round(float(r["aov"]), 2),
            "is_own_brand": bool(r["is_own_brand"]),
        })

    return {
        "data":    records,
        "source":  "orders + shops tables, replica_uae database",
        "filters": f"status=3, payment_status='completed', date {date_from} to {date_to}, top {limit}",
        "formula": "SUM(total) per shop; sorted descending",
        "sql":     sql.strip(),
    }


# ─── 7. Top products ─────────────────────────────────────────────────────────

def get_top_products(limit: int = 10) -> dict:
    """
    Top products by revenue. Uses pre-computed .tmp/top_products.csv if available,
    otherwise falls back to a live SQL query.
    """
    df = _tmp("top_products.csv")
    if not df.empty:
        cols = ["product_id", "product_name", "category_name", "shop_name",
                "total_revenue", "total_units", "total_orders", "avg_margin"]
        avail = [c for c in cols if c in df.columns]
        top = df.nlargest(limit, "total_revenue")[avail] if "total_revenue" in df.columns else df.head(limit)[avail]
        return {
            "data":    top.to_dict(orient="records"),
            "source":  ".tmp/top_products.csv (generated by tools/product_analysis.py)",
            "filters": f"top {limit} by total_revenue; all delivered orders in dataset",
            "formula": "SUM(price * quantity) per product from order_details joined to delivered orders",
            "sql":     "pre-computed from .tmp/top_products.csv",
        }

    # Live fallback
    sql = """
        SELECT
            od.product_id,
            p.name                              AS product_name,
            c.name                              AS category_name,
            s.name                              AS shop_name,
            SUM(od.price * od.quantity)         AS total_revenue,
            SUM(od.quantity)                    AS total_units,
            COUNT(DISTINCT od.order_id)         AS total_orders
        FROM order_details od
        JOIN orders o ON o.id = od.order_id
        LEFT JOIN products p ON p.id = od.product_id
        LEFT JOIN categories c ON c.id = p.category_id
        LEFT JOIN shops s ON s.id = o.shop_id
        WHERE o.status = 3 AND o.payment_status = 'completed'
        GROUP BY od.product_id, p.name, c.name, s.name
        ORDER BY total_revenue DESC
        LIMIT %(limit)s
    """
    df_live = query_df(sql, params={"limit": limit})
    return {
        "data":    df_live.to_dict(orient="records") if not df_live.empty else [],
        "source":  "order_details + products + categories + shops tables, replica_uae",
        "filters": f"status=3, payment_status='completed', top {limit} by revenue",
        "formula": "SUM(price × quantity) per product",
        "sql":     sql.strip(),
    }


# ─── 8. Category performance ─────────────────────────────────────────────────

def get_category_performance() -> dict:
    """
    Revenue, orders, and revenue share per product category.
    Uses pre-computed .tmp/category_performance.csv if available, otherwise live SQL.
    """
    df = _tmp("category_performance.csv")
    if not df.empty:
        return {
            "data":    df.to_dict(orient="records"),
            "source":  ".tmp/category_performance.csv (generated by tools/product_analysis.py)",
            "filters": "all delivered orders in dataset",
            "formula": "SUM(line_revenue), COUNT(DISTINCT order_id) grouped by category",
            "sql":     "pre-computed from .tmp/category_performance.csv",
        }

    # Live fallback
    sql = """
        SELECT
            c.name                              AS category_name,
            SUM(od.price * od.quantity)         AS total_revenue,
            COUNT(DISTINCT od.order_id)         AS total_orders,
            SUM(od.quantity)                    AS total_units,
            ROUND(
                100.0 * SUM(od.price * od.quantity) /
                NULLIF(SUM(SUM(od.price * od.quantity)) OVER (), 0), 2
            )                                   AS revenue_share_pct
        FROM order_details od
        JOIN orders o ON o.id = od.order_id
        LEFT JOIN products p ON p.id = od.product_id
        LEFT JOIN categories c ON c.id = p.category_id
        WHERE o.status = 3 AND o.payment_status = 'completed'
        GROUP BY c.name
        ORDER BY total_revenue DESC
    """
    df_live = query_df(sql)
    return {
        "data":    df_live.to_dict(orient="records") if not df_live.empty else [],
        "source":  "order_details + categories + orders tables, replica_uae",
        "filters": "status=3, payment_status='completed', all time",
        "formula": "SUM(price × quantity) per category; revenue share = category_revenue / total_revenue",
        "sql":     sql.strip(),
    }


# ─── 9. User RFM segments ────────────────────────────────────────────────────

def get_user_segments() -> dict:
    """
    RFM segment distribution — count, avg LTV, revenue share, avg recency per segment.
    """
    data = _tmp("user_segments.json")
    if not data:
        # Fallback: compute from rfm_scores.csv
        df = _tmp("rfm_scores.csv")
        if df.empty:
            return {"data": [], "source": ".tmp/user_segments.json", "filters": "n/a",
                    "formula": "n/a", "sql": "pre-computed"}
        summary = df.groupby("Segment").agg(
            user_count=("user_id", "count"),
            avg_ltv=("monetary", "mean"),
            total_revenue=("monetary", "sum"),
            avg_recency_days=("recency_days", "mean"),
        ).round(1).reset_index().to_dict(orient="records")
        data = summary

    return {
        "data":    data,
        "source":  ".tmp/user_segments.json (generated by tools/user_analysis.py)",
        "filters": "all non-banned users with purchase history",
        "formula": "RFM scoring: R=recency rank, F=category diversity rank, M=lifetime monetary rank (1-5 each); segments assigned by rule table",
        "sql":     "pre-computed from .tmp/user_segments.json",
    }


# ─── 10. Churn risk distribution ─────────────────────────────────────────────

def get_churn_stats() -> dict:
    """
    Churn risk distribution (Critical / High / Medium / Low) and total at-risk count.
    """
    df = _tmp("churn_risk.csv")
    if df.empty:
        return {"data": {}, "source": ".tmp/churn_risk.csv", "filters": "n/a",
                "formula": "n/a", "sql": "pre-computed"}

    dist = df["churn_risk_label"].value_counts().to_dict() if "churn_risk_label" in df.columns else {}
    at_risk = dist.get("Critical", 0) + dist.get("High", 0)

    return {
        "data": {
            "distribution": dist,
            "at_risk_count": at_risk,
            "total_users":   len(df),
        },
        "source":  ".tmp/churn_risk.csv (generated by tools/user_analysis.py via buying_patterns.py)",
        "filters": "all non-banned users with purchase history",
        "formula": "churn_risk_score = recency_score×0.5 + breadth_score×0.3 + value_score×0.2; Critical≥0.75, High≥0.55, Medium≥0.35, Low<0.35",
        "sql":     "pre-computed from .tmp/churn_risk.csv",
    }


# ─── 11. LTV tier distribution ───────────────────────────────────────────────

def get_ltv_stats() -> dict:
    """
    LTV tier counts (Platinum / Gold / Silver / Bronze) and revenue per tier.
    """
    df = _tmp("ltv_analysis.csv")
    if df.empty:
        return {"data": {}, "source": ".tmp/ltv_analysis.csv", "filters": "n/a",
                "formula": "n/a", "sql": "pre-computed"}

    tiers = {}
    if "ltv_tier" in df.columns and "monetary" in df.columns:
        for tier, grp in df.groupby("ltv_tier"):
            tiers[tier] = {
                "users":         int(len(grp)),
                "total_revenue": round(float(grp["monetary"].sum()), 2),
                "avg_ltv":       round(float(grp["monetary"].mean()), 2),
            }

    return {
        "data":    tiers,
        "source":  ".tmp/ltv_analysis.csv (generated by tools/user_analysis.py)",
        "filters": "all non-banned users with purchase history",
        "formula": "LTV tiers: Platinum≥AED5,000 | Gold AED2,000–4,999 | Silver AED500–1,999 | Bronze<AED500 (based on lifetime_value from user_total_orders)",
        "sql":     "pre-computed from .tmp/ltv_analysis.csv",
    }


# ─── 12. Payment methods ─────────────────────────────────────────────────────

def get_payment_methods() -> dict:
    """
    Payment method split by order count and revenue share.
    """
    patterns = _tmp("buying_patterns.json")
    methods  = patterns.get("payment_methods", [])

    return {
        "data":    methods,
        "source":  ".tmp/buying_patterns.json (generated by tools/buying_patterns.py)",
        "filters": "all delivered + completed orders",
        "formula": "COUNT(order_id) per payment_method code; mapped to human-readable names",
        "sql":     "pre-computed from .tmp/buying_patterns.json",
    }


# ─── 13. Peak order timing ────────────────────────────────────────────────────

def get_peak_timing() -> dict:
    """
    Peak days of week and peak hours for order placement.
    """
    patterns = _tmp("buying_patterns.json")
    timing   = patterns.get("order_timing", {})

    return {
        "data":    timing,
        "source":  ".tmp/buying_patterns.json (generated by tools/buying_patterns.py)",
        "filters": "all delivered + completed orders",
        "formula": "COUNT(order_id) grouped by DAYOFWEEK and HOUR(created_at)",
        "sql":     "pre-computed from .tmp/buying_patterns.json",
    }


# ─── 14. BCG matrix summary ──────────────────────────────────────────────────

def get_bcg_summary() -> dict:
    """
    BCG quadrant distribution and top Stars by revenue.
    """
    df = _tmp("bcg_matrix.csv")
    if df.empty:
        return {"data": {}, "source": ".tmp/bcg_matrix.csv", "filters": "n/a",
                "formula": "n/a", "sql": "pre-computed"}

    summary = {}
    if "bcg_quadrant" in df.columns:
        for q, grp in df.groupby("bcg_quadrant"):
            summary[q] = {
                "products":     int(len(grp)),
                "total_revenue":round(float(grp["total_revenue"].sum()), 2) if "total_revenue" in grp else 0,
            }

    top_stars = []
    if "bcg_quadrant" in df.columns and "total_revenue" in df.columns:
        stars = df[df["bcg_quadrant"] == "Star"].nlargest(5, "total_revenue")
        top_stars = stars[["product_id","product_name","total_revenue","market_share_pct","growth_rate"]].to_dict(orient="records") \
            if "product_name" in stars.columns else []

    return {
        "data":    {"quadrants": summary, "top_stars": top_stars},
        "source":  ".tmp/bcg_matrix.csv (generated by tools/product_analysis.py)",
        "filters": "all delivered orders; products with ≥1 sale",
        "formula": "Market share = product revenue ÷ category revenue × 100; Vitality = total_orders × avg_margin; quadrant assigned by median thresholds",
        "sql":     "pre-computed from .tmp/bcg_matrix.csv",
    }


# ─── 15. Revenue per user ────────────────────────────────────────────────────

def get_revenue_per_user(date_from: str = None, date_to: str = None) -> dict:
    """
    Average and total revenue per unique buyer in the given date range.
    """
    if not date_from or not date_to:
        date_from, date_to = _month_bounds()

    sql = """
        SELECT
            COUNT(DISTINCT user_id)                      AS buyers,
            SUM(total)                                   AS total_revenue,
            SUM(total) / COUNT(DISTINCT user_id)         AS revenue_per_user
        FROM orders
        WHERE status = 3
          AND payment_status = 'completed'
          AND DATE(created_at) BETWEEN %(d1)s AND %(d2)s
    """
    df = query_df(sql, params={"d1": date_from, "d2": date_to})
    row = df.iloc[0].to_dict() if not df.empty else {}

    return {
        "data": {
            "buyers":           int(row.get("buyers") or 0),
            "total_revenue":    round(float(row.get("total_revenue") or 0), 2),
            "revenue_per_user": round(float(row.get("revenue_per_user") or 0), 2),
        },
        "source":  "orders table, replica_uae database",
        "filters": f"status=3, payment_status='completed', date {date_from} to {date_to}",
        "formula": "SUM(total) / COUNT(DISTINCT user_id)",
        "sql":     sql.strip(),
    }


# ─── 16. Cancellation stats ──────────────────────────────────────────────────

def get_cancellation_stats(date_from: str = None, date_to: str = None) -> dict:
    """
    Cancelled order volume, cancellation rate, and top cancellation reasons
    for a given date range. Queries live DB — not limited to delivered orders.
    """
    if not date_from or not date_to:
        date_from, date_to = _month_bounds()

    sql_summary = """
        SELECT
            COUNT(*)                                          AS total_orders,
            SUM(CASE WHEN status = 2 THEN 1 ELSE 0 END)      AS cancelled_orders,
            SUM(CASE WHEN status = 3
                      AND payment_status = 'completed'
                THEN 1 ELSE 0 END)                            AS delivered_orders,
            ROUND(
                100.0 * SUM(CASE WHEN status = 2 THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0)
            , 2)                                              AS cancellation_rate_pct,
            SUM(CASE WHEN status = 2 THEN total ELSE 0 END)  AS cancelled_gmv
        FROM orders
        WHERE DATE(created_at) BETWEEN %(d1)s AND %(d2)s
    """

    sql_reasons = """
        SELECT
            COALESCE(cancel_reason, 'Not specified')  AS reason,
            COUNT(*)                                  AS count
        FROM orders
        WHERE status = 2
          AND DATE(created_at) BETWEEN %(d1)s AND %(d2)s
        GROUP BY cancel_reason
        ORDER BY count DESC
        LIMIT 10
    """

    params = {"d1": date_from, "d2": date_to}
    df_s = query_df(sql_summary, params=params)
    df_r = query_df(sql_reasons, params=params)

    row = df_s.iloc[0].to_dict() if not df_s.empty else {}
    reasons = df_r.to_dict(orient="records") if not df_r.empty else []

    return {
        "data": {
            "total_orders":          int(row.get("total_orders") or 0),
            "cancelled_orders":      int(row.get("cancelled_orders") or 0),
            "delivered_orders":      int(row.get("delivered_orders") or 0),
            "cancellation_rate_pct": float(row.get("cancellation_rate_pct") or 0),
            "cancelled_gmv_aed":     round(float(row.get("cancelled_gmv") or 0), 2),
            "top_reasons":           reasons,
        },
        "source":  "orders table, replica_uae database (live query)",
        "filters": f"DATE(created_at) BETWEEN {date_from} AND {date_to}; status=2 for cancellations",
        "formula": "cancellation_rate = COUNT(status=2) / COUNT(all orders) × 100",
        "sql":     sql_summary.strip(),
    }


# ─── 17. Forecast ────────────────────────────────────────────────────────────

def forecast_metric(metric: str = "revenue", periods: int = 4, period_unit: str = "weeks") -> dict:
    """
    Project orders or revenue forward using linear trend on last 12 months of data.
    metric: 'revenue' or 'orders'
    periods: number of weeks or months ahead
    period_unit: 'weeks' or 'months'
    """
    # Pull last 12 months
    sql = """
        SELECT
            DATE_FORMAT(created_at, '%%Y-%%m')  AS month,
            COUNT(DISTINCT id)                   AS orders,
            SUM(total)                           AS revenue
        FROM orders
        WHERE status = 3
          AND payment_status = 'completed'
          AND created_at >= DATE_SUB(CURDATE(), INTERVAL 13 MONTH)
        GROUP BY DATE_FORMAT(created_at, '%%Y-%%m')
        ORDER BY month
    """
    df = query_df(sql)
    if df.empty or len(df) < 3:
        return {"data": {"error": "Insufficient historical data for projection"},
                "source": "orders table", "filters": "last 13 months", "formula": "n/a", "sql": sql.strip()}

    col = "revenue" if metric == "revenue" else "orders"
    values = df[col].astype(float).values
    x = np.arange(len(values))

    # Linear trend on log-transformed values (handles exponential growth)
    log_vals = np.log(np.maximum(values, 1))
    coeffs   = np.polyfit(x, log_vals, 1)  # slope, intercept

    last_month_val = float(values[-1])

    # Convert periods to months for projection
    if period_unit == "weeks":
        months_ahead = periods / 4.33
    else:
        months_ahead = float(periods)

    # Project using the trend
    x_future    = x[-1] + months_ahead
    projected   = float(np.exp(np.polyval(coeffs, x_future)))

    # Day-of-week weight: if projecting weeks, apply weekly share
    patterns = _tmp("buying_patterns.json")
    dow_data = patterns.get("order_timing", {}).get("peak_days", [])
    total_dow = sum(d["orders"] for d in dow_data) if dow_data else 1
    weekly_share = 7 / 30.4  # approximate

    if period_unit == "weeks":
        # Scale monthly projection to the number of weeks
        projected = projected * (periods / 4.33)

    # Confidence band ±15%
    low  = round(projected * 0.85, 2)
    high = round(projected * 1.15, 2)

    # Historical monthly avg for context
    hist_avg = round(float(np.mean(values)), 2)
    last_val = round(last_month_val, 2)

    return {
        "data": {
            "metric":          metric,
            "projection":      round(projected, 2),
            "low_estimate":    low,
            "high_estimate":   high,
            "confidence_band": "±15%",
            "periods":         periods,
            "period_unit":     period_unit,
            "historical_monthly_avg": hist_avg,
            "last_full_month_value":  last_val,
            "trend_direction": "upward" if coeffs[0] > 0 else "downward",
            "monthly_trend_slope_pct": round(float((np.exp(coeffs[0]) - 1) * 100), 2),
        },
        "source":  "orders table, replica_uae database",
        "filters": "status=3, payment_status='completed', last 13 months",
        "formula": f"Linear trend on log-transformed monthly {metric}; projected {periods} {period_unit} forward; ±15% confidence band",
        "sql":     sql.strip(),
    }


# ─── 17. Excel report export ─────────────────────────────────────────────────

def export_excel_report(
    columns: list,
    date_from: str = None,
    date_to: str   = None,
    report_name: str = "zabehaty_report",
) -> dict:
    """
    Build an Excel workbook with the requested columns for the given date range.
    Supported column names (case-insensitive):
      new_buyers, new_users, phone_users, first_orders, repeat_orders,
      total_orders, total_revenue, revenue_per_user, aov, customers,
      top_shops, top_products, user_segments, churn_stats, ltv_stats,
      category_performance, payment_methods
    Returns bytes stored in a dict under 'excel_bytes'.
    """
    if not date_from or not date_to:
        date_from, date_to = _month_bounds()

    # Normalise column names
    cols = [c.lower().strip().replace(" ", "_") for c in columns]

    # Detect "all / core / full" requests — include every sheet
    _ALL_KEYWORDS = {"all", "core", "full", "complete", "everything", "business",
                     "business_values", "core_business", "overview", "kpi", "kpis"}
    include_all = bool(_ALL_KEYWORDS.intersection(set(cols))) or not cols

    def _want(*keywords):
        return include_all or any(c in cols for c in keywords)

    # Collect data for each requested column
    sheets = {}

    # --- Summary sheet (always included) ---
    stats  = get_order_stats(date_from, date_to)["data"]
    buyers = get_new_buyers(date_from, date_to)["data"]
    repeat = get_repeat_order_stats(date_from, date_to)["data"]
    phones = get_users_with_phone(date_from, date_to)["data"]
    rev_pu = get_revenue_per_user(date_from, date_to)["data"]

    summary_rows = [
        {"Metric": "Total Orders",              "Value": stats.get("orders", 0),              "Period": f"{date_from} to {date_to}"},
        {"Metric": "Total Revenue (AED)",        "Value": stats.get("revenue_aed", 0),         "Period": f"{date_from} to {date_to}"},
        {"Metric": "Unique Customers",           "Value": stats.get("customers", 0),           "Period": f"{date_from} to {date_to}"},
        {"Metric": "Average Order Value (AED)",  "Value": stats.get("aov_aed", 0),             "Period": f"{date_from} to {date_to}"},
        {"Metric": "Revenue Per User (AED)",     "Value": rev_pu.get("revenue_per_user", 0),   "Period": f"{date_from} to {date_to}"},
        {"Metric": "New Buyers (First Order)",   "Value": buyers.get("new_buyers", 0),         "Period": f"{date_from} to {date_to}"},
        {"Metric": "Repeat Orders",              "Value": repeat.get("repeat_orders", 0),      "Period": f"{date_from} to {date_to}"},
        {"Metric": "Repeat Buyers",              "Value": repeat.get("repeat_buyers", 0),      "Period": f"{date_from} to {date_to}"},
        {"Metric": "Total Discounts (AED)",      "Value": stats.get("total_discounts", 0),     "Period": f"{date_from} to {date_to}"},
        {"Metric": "Delivery Fees (AED)",        "Value": stats.get("delivery_fees", 0),       "Period": f"{date_from} to {date_to}"},
        {"Metric": "Users With Phone",           "Value": phones.get("with_phone", 0),         "Period": f"{date_from} to {date_to}"},
    ]
    sheets["Summary"] = pd.DataFrame(summary_rows)

    # --- Monthly trend (always included — core business view) ---
    if _want("monthly_trend", "trend", "monthly"):
        trend = get_monthly_trend()["data"]
        if trend:
            sheets["Monthly Trend"] = pd.DataFrame(trend)

    # --- Top shops ---
    if _want("top_shops", "shops", "vendors"):
        shop_data = get_top_shops(date_from, date_to, limit=20)["data"]
        if shop_data:
            sheets["Top Shops"] = pd.DataFrame(shop_data).drop(columns=["shop_ar"], errors="ignore")

    # --- Top products ---
    if _want("top_products", "products"):
        prod_data = get_top_products(limit=30)["data"]
        if prod_data:
            sheets["Top Products"] = pd.DataFrame(prod_data)

    # --- Category performance ---
    if _want("category_performance", "categories"):
        cat_data = get_category_performance()["data"]
        if cat_data:
            sheets["Category Performance"] = pd.DataFrame(cat_data)

    # --- User segments ---
    if _want("user_segments", "segments", "rfm"):
        seg_data = get_user_segments()["data"]
        if seg_data:
            sheets["User Segments"] = pd.DataFrame(seg_data)

    # --- Churn stats ---
    if _want("churn_stats", "churn"):
        churn = get_churn_stats()["data"]
        dist  = churn.get("distribution", {})
        if dist:
            sheets["Churn Risk"] = pd.DataFrame([{"Risk Level": k, "Users": v} for k, v in dist.items()])

    # --- LTV stats ---
    if _want("ltv_stats", "ltv"):
        ltv_data = get_ltv_stats()["data"]
        if ltv_data:
            sheets["LTV Tiers"] = pd.DataFrame([
                {"Tier": k, **v} for k, v in ltv_data.items()
            ])

    # --- Payment methods ---
    if _want("payment_methods", "payments"):
        pay_data = get_payment_methods()["data"]
        if pay_data:
            sheets["Payment Methods"] = pd.DataFrame(pay_data)

    # --- BCG Matrix ---
    if _want("bcg", "matrix", "product_matrix", "bcg_matrix"):
        bcg_df = _tmp("bcg_matrix.csv")
        if not bcg_df.empty and "bcg_quadrant" in bcg_df.columns:
            keep_cols = [c for c in ["product_name", "category_name", "shop_name",
                                     "total_revenue", "growth_rate", "avg_margin",
                                     "total_units", "total_orders", "bcg_quadrant"]
                         if c in bcg_df.columns]
            for quadrant in ["Star", "Cash Cow", "Question Mark", "Dog"]:
                qdf = bcg_df[bcg_df["bcg_quadrant"] == quadrant][keep_cols]
                if not qdf.empty:
                    sheets[f"BCG {quadrant}s"] = qdf.sort_values("total_revenue",
                                                                   ascending=False).head(50)
        else:
            # Fallback: use get_bcg_summary
            bcg_summary = get_bcg_summary()["data"]
            if bcg_summary:
                sheets["BCG Summary"] = pd.DataFrame([
                    {"Quadrant": k, **v} for k, v in bcg_summary.items()
                    if isinstance(v, dict)
                ])

    # --- Lost users win-back (for customer behaviour deep-dives) ---
    if _want("lost_users", "win_back", "winback", "churn_users", "customer_behavior",
             "customer_behaviour", "behavior", "behaviour"):
        lost_data = get_lost_users_winback(min_revenue=2000, limit=50)["data"]
        if lost_data:
            sheets["Lost Users Win-Back"] = pd.DataFrame(lost_data)

    # --- Build Excel in memory ---
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        if not sheets:
            # Fallback: write full order stats
            stats = get_order_stats(date_from, date_to)["data"]
            pd.DataFrame([stats]).to_excel(writer, sheet_name="Summary", index=False)
        else:
            for sheet_name, df_sheet in sheets.items():
                df_sheet.to_excel(writer, sheet_name=sheet_name[:31], index=False)

    excel_bytes = buf.getvalue()

    return {
        "data": {
            "excel_bytes":  excel_bytes,   # caller stores this in session_state
            "filename":     f"{report_name}_{date_from}_to_{date_to}.xlsx",
            "sheets":       list(sheets.keys()) if sheets else ["Summary"],
            "rows_approx":  sum(len(df) for df in sheets.values()) if sheets else 1,
            "date_from":    date_from,
            "date_to":      date_to,
            "columns_used": cols,
        },
        "source":  "Multiple: orders table (live), .tmp/ CSV/JSON files",
        "filters": f"date {date_from} to {date_to}; status=3, payment_status='completed'",
        "formula": "Each sheet uses the corresponding tool function — see individual tool provenance",
        "sql":     "See individual tool functions for SQL",
    }


# ─── 18. Average LTV across all customers ───────────────────────────────────

def get_ltv_average() -> dict:
    """
    Overall average and median LTV across all customers, plus breakdown by RFM segment.
    Uses rfm_scores.csv (monetary column = historical lifetime spend).
    Falls back to a live SQL query when .tmp files are unavailable.
    """
    df = _tmp("rfm_scores.csv")

    if not df.empty and "monetary" in df.columns:
        avg_ltv    = round(float(df["monetary"].mean()), 2)
        median_ltv = round(float(df["monetary"].median()), 2)
        total_ltv  = round(float(df["monetary"].sum()), 2)
        total_cust = int(len(df))

        by_segment = []
        seg_col = next((c for c in ["Segment", "segment", "rfm_segment"] if c in df.columns), None)
        if seg_col:
            for seg, grp in df.groupby(seg_col):
                by_segment.append({
                    "segment":         str(seg),
                    "users":           int(len(grp)),
                    "avg_ltv_aed":     round(float(grp["monetary"].mean()), 2),
                    "median_ltv_aed":  round(float(grp["monetary"].median()), 2),
                    "total_revenue_aed": round(float(grp["monetary"].sum()), 2),
                })
            by_segment.sort(key=lambda x: x["avg_ltv_aed"], reverse=True)

        return {
            "data": {
                "overall_avg_ltv_aed":    avg_ltv,
                "overall_median_ltv_aed": median_ltv,
                "total_lifetime_revenue_aed": total_ltv,
                "total_customers":        total_cust,
                "by_segment":             by_segment,
            },
            "source":  ".tmp/rfm_scores.csv (generated by tools/user_analysis.py)",
            "filters": "all non-banned users with at least one delivered order",
            "formula": "avg_ltv = SUM(monetary) / COUNT(user_id); median = 50th percentile of monetary",
            "sql":     "pre-computed from .tmp/rfm_scores.csv",
        }

    # Live SQL fallback
    sql = """
        SELECT
            COUNT(DISTINCT user_id)   AS total_customers,
            AVG(lifetime_value)       AS avg_ltv,
            SUM(lifetime_value)       AS total_ltv
        FROM (
            SELECT user_id, SUM(total) AS lifetime_value
            FROM orders
            WHERE status = 3 AND payment_status = 'completed'
            GROUP BY user_id
        ) t
    """
    df_live = query_df(sql)
    if df_live.empty:
        return {"data": {}, "source": "orders table", "filters": "n/a",
                "formula": "n/a", "sql": sql.strip()}
    row = df_live.iloc[0]
    return {
        "data": {
            "overall_avg_ltv_aed":       round(float(row.get("avg_ltv") or 0), 2),
            "total_lifetime_revenue_aed": round(float(row.get("total_ltv") or 0), 2),
            "total_customers":            int(row.get("total_customers") or 0),
            "by_segment":                 [],
        },
        "source":  "orders table, replica_uae",
        "filters": "status=3, payment_status='completed'",
        "formula": "avg_ltv = SUM(total) / COUNT(DISTINCT user_id) across all delivered orders",
        "sql":     sql.strip(),
    }


# ─── 19. Cross-selling opportunities ─────────────────────────────────────────

def get_cross_sell_opportunities(limit: int = 10) -> dict:
    """
    Top category pairs bought together, with co-buyer count, affinity %,
    and a ready-to-use offer suggestion for each pair.
    Data from buying_patterns.json (cross_category_affinity) or cross_category.csv.
    """
    patterns = _tmp("buying_patterns.json")
    pairs    = patterns.get("cross_category_affinity", [])

    # Fallback: cross_category.csv
    if not pairs:
        df_cc = _tmp("cross_category.csv")
        if not df_cc.empty:
            pairs = df_cc.to_dict(orient="records")

    if not pairs:
        return {"data": [], "source": ".tmp/buying_patterns.json",
                "filters": "no cross-category data found",
                "formula": "n/a", "sql": "pre-computed"}

    # Total buyers for affinity %
    repeat_stats  = patterns.get("repeat_purchase", {})
    total_buyers  = repeat_stats.get("total_buyers", 1) or 1

    # Peak timing for recommended send time
    timing    = patterns.get("order_timing", {})
    peak_days = timing.get("peak_days", [])
    peak_hrs  = timing.get("peak_hours", [])
    best_day  = peak_days[0]["day_name"] if peak_days and "day_name" in peak_days[0] else \
                (peak_days[0].get("day", "Thursday") if peak_days else "Thursday")
    best_hr   = peak_hrs[0].get("hour_of_day", peak_hrs[0].get("hour", 13)) if peak_hrs else 13

    enriched = []
    for pair in pairs[:limit]:
        cat_a     = pair.get("category_a", "")
        cat_b     = pair.get("category_b", "")
        co_buyers = int(pair.get("co_buyers", 0))
        affinity  = round(co_buyers / total_buyers * 100, 2)

        if co_buyers >= 4000:
            offer = f"Bundle {cat_a} + {cat_b} — 15% off both categories. Estimated reach: {co_buyers:,} customers."
        elif co_buyers >= 2000:
            offer = f"Cross-sell: offer 10% off {cat_b} to every {cat_a} buyer at checkout."
        else:
            offer = f"Promote {cat_b} to {cat_a} buyers with a 5% loyalty coupon."

        enriched.append({
            "category_a":       cat_a,
            "category_b":       cat_b,
            "co_buyers":        co_buyers,
            "affinity_pct":     affinity,
            "suggested_offer":  offer,
            "best_send_day":    best_day,
            "best_send_time":   f"{best_hr}:00",
            "estimated_reach":  co_buyers,
        })

    return {
        "data":    enriched,
        "source":  ".tmp/buying_patterns.json (generated by tools/buying_patterns.py)",
        "filters": "all users with multi-category purchase history; delivered+completed orders",
        "formula": "co_buyers = COUNT(DISTINCT user_id) where same user bought both categories; affinity = co_buyers / total_buyers × 100",
        "sql":     "pre-computed from .tmp/cross_category.csv",
    }


# ─── 20. Top customers list ───────────────────────────────────────────────────

def get_top_customers(segment: str = "Champions", limit: int = 20) -> dict:
    """
    Return top customers in a given RFM segment sorted by lifetime value,
    including their offer tier and enough detail to personalise a campaign.
    Falls back to a live SQL query for the highest-spending users when .tmp unavailable.
    """
    df = _tmp("rfm_scores.csv")
    seg_col = next((c for c in ["Segment", "segment", "rfm_segment"] if c in df.columns), None)
    mon_col = next((c for c in ["monetary", "lifetime_value", "total_spend"] if c in df.columns), None)

    if not df.empty and mon_col:
        if seg_col and segment.lower() not in ("all", "top"):
            filtered = df[df[seg_col].str.lower().str.contains(segment.lower(), na=False)].copy()
        else:
            filtered = df.copy()

        filtered = filtered.nlargest(limit, mon_col)

        records = []
        for _, row in filtered.iterrows():
            ltv = round(float(row.get(mon_col, 0)), 2)
            records.append({
                "user_id":          int(row.get("user_id", 0)),
                "lifetime_value_aed": ltv,
                "segment":          str(row.get(seg_col, "Unknown")) if seg_col else "Unknown",
                "categories_count": int(row.get("categories_purchased", row.get("category_count", 0))),
                "predicted_ltv_12m": round(float(row.get("predicted_ltv_12m", 0)), 2),
                "last_order_date":  str(row.get("last_order_date", row.get("last_active", ""))),
                "offer_tier":       _offer_tier(ltv),
            })
        return {
            "data":    records,
            "source":  ".tmp/rfm_scores.csv",
            "filters": f"Segment contains '{segment}'; top {limit} by lifetime value",
            "formula": "Sorted by monetary column descending; offer tier assigned by LTV bracket",
            "sql":     "pre-computed from .tmp/rfm_scores.csv",
        }

    # Live SQL fallback — top spenders regardless of segment
    sql = """
        SELECT
            user_id,
            SUM(total)          AS lifetime_value,
            COUNT(DISTINCT id)  AS total_orders,
            MAX(created_at)     AS last_order_date
        FROM orders
        WHERE status = 3 AND payment_status = 'completed'
        GROUP BY user_id
        ORDER BY lifetime_value DESC
        LIMIT %(lim)s
    """
    df_live = query_df(sql, params={"lim": limit})
    records = []
    for _, row in df_live.iterrows():
        ltv = round(float(row.get("lifetime_value", 0)), 2)
        records.append({
            "user_id":           int(row["user_id"]),
            "lifetime_value_aed": ltv,
            "total_orders":      int(row.get("total_orders", 0)),
            "last_order_date":   str(row.get("last_order_date", "")),
            "offer_tier":        _offer_tier(ltv),
        })
    return {
        "data":    records,
        "source":  "orders table, replica_uae",
        "filters": "status=3, payment_status='completed'; top spenders overall",
        "formula": "SUM(total) per user ordered descending",
        "sql":     sql.strip(),
    }


def _offer_tier(ltv: float) -> str:
    if ltv >= 10000: return "VIP Platinum"
    if ltv >= 5000:  return "Premium Gold"
    if ltv >= 2000:  return "Standard Silver"
    return "Bronze"


# ─── 21. Customer buying profile (per-user, live DB) ─────────────────────────

def get_customer_buying_profile(user_id: int) -> dict:
    """
    For a specific customer: top categories by spend, AOV per category,
    and inferred preferred order hour from their order timestamps.
    Live query against user_total_orders + orders tables.
    """
    sql_cats = """
        SELECT
            c.name                          AS category_name,
            COUNT(DISTINCT uto.order_id)    AS purchase_count,
            SUM(uto.total)                  AS category_spend,
            AVG(uto.total)                  AS avg_order_value
        FROM user_total_orders uto
        LEFT JOIN categories c ON uto.category_id = c.id
        WHERE uto.user_id = %(uid)s
        GROUP BY c.name
        ORDER BY category_spend DESC
        LIMIT 5
    """
    sql_timing = """
        SELECT HOUR(created_at) AS order_hour, COUNT(*) AS cnt
        FROM orders
        WHERE user_id = %(uid)s AND status = 3 AND payment_status = 'completed'
        GROUP BY HOUR(created_at)
        ORDER BY cnt DESC
        LIMIT 1
    """
    df_cats   = query_df(sql_cats,   params={"uid": user_id})
    df_timing = query_df(sql_timing, params={"uid": user_id})

    categories = []
    if not df_cats.empty:
        for _, row in df_cats.iterrows():
            categories.append({
                "category":        str(row.get("category_name", "")),
                "orders":          int(row.get("purchase_count", 0)),
                "total_spend_aed": round(float(row.get("category_spend", 0)), 2),
                "avg_order_aed":   round(float(row.get("avg_order_value", 0)), 2),
            })

    preferred_hour = int(df_timing.iloc[0]["order_hour"]) if not df_timing.empty else 13

    # Get platform peak day
    patterns  = _tmp("buying_patterns.json")
    peak_days = patterns.get("order_timing", {}).get("peak_days", [])
    best_day  = peak_days[0].get("day_name", peak_days[0].get("day", "Thursday")) if peak_days else "Thursday"

    return {
        "data": {
            "user_id":              user_id,
            "top_categories":       categories,
            "preferred_order_hour": preferred_hour,
            "best_send_day":        best_day,
            "best_send_time":       f"{preferred_hour:02d}:00",
        },
        "source":  "user_total_orders + orders tables, replica_uae",
        "filters": f"user_id = {user_id}; status=3, payment_status='completed'",
        "formula": "top categories by SUM(total); preferred hour by MODE(HOUR(created_at))",
        "sql":     sql_cats.strip(),
    }


# ─── 22. WhatsApp / notification promo content generation ────────────────────

def generate_promo_campaign(
    segment: str = "Champions",
    limit: int   = 15,
) -> dict:
    """
    Generate a full personalised promotion campaign for top customers:
    - List of customers with offer tiers
    - Personalised WhatsApp message (Arabic + English) per customer
    - Recommended send day and time per customer (from their order history)
    - Revenue lift estimate
    """
    customers_result = get_top_customers(segment, limit)
    customers        = customers_result["data"]

    # Platform peak timing as fallback
    patterns  = _tmp("buying_patterns.json")
    peak_days = patterns.get("order_timing", {}).get("peak_days", [])
    peak_hrs  = patterns.get("order_timing", {}).get("peak_hours", [])
    default_day  = peak_days[0].get("day_name", peak_days[0].get("day", "Thursday")) if peak_days else "Thursday"
    default_hour = peak_hrs[0].get("hour_of_day", peak_hrs[0].get("hour", 13)) if peak_hrs else 13

    campaign = []
    for cust in customers:
        uid  = cust["user_id"]
        ltv  = cust.get("lifetime_value_aed", 0)
        tier = cust.get("offer_tier", "Standard Silver")

        # Per-user buying profile (live query)
        try:
            profile = get_customer_buying_profile(uid)["data"]
            top_cats    = [c["category"] for c in profile.get("top_categories", [])][:2]
            send_hour   = profile.get("preferred_order_hour", default_hour)
            send_day    = profile.get("best_send_day", default_day)
        except Exception:
            top_cats    = []
            send_hour   = default_hour
            send_day    = default_day

        cat_text_en = " & ".join(top_cats) if top_cats else "your favorite products"
        cat_text_ar = " و ".join(top_cats) if top_cats else "منتجاتك المفضلة"

        # Offer details by tier
        offers = {
            "VIP Platinum":   {"disc": "20%", "bonus_en": "FREE delivery + priority support", "bonus_ar": "توصيل مجاني + أولوية الدعم"},
            "Premium Gold":   {"disc": "15%", "bonus_en": "loyalty points doubled",           "bonus_ar": "مضاعفة نقاط الولاء"},
            "Standard Silver":{"disc": "10%", "bonus_en": "free delivery on orders >150 AED", "bonus_ar": "توصيل مجاني للطلبات فوق ١٥٠ درهم"},
            "Bronze":         {"disc": "5%",  "bonus_en": "free delivery coupon",             "bonus_ar": "كوبون توصيل مجاني"},
        }
        o = offers.get(tier, offers["Standard Silver"])

        msg_en = (
            f"{'🌟' if 'Platinum' in tier else '⭐' if 'Gold' in tier else '💎'} "
            f"*Exclusive Zabehaty Offer!*\n\n"
            f"We have a special *{o['disc']} discount* on {cat_text_en}!\n\n"
            f"✅ {o['disc']} OFF your next order\n"
            f"✅ {o['bonus_en']}\n"
            f"⏰ Offer valid for 7 days only\n\n"
            f"🛒 Order now: zabehaty.ae\n"
            f"📌 Use code: *ZB{uid}*"
        )
        msg_ar = (
            f"{'🌟' if 'Platinum' in tier else '⭐' if 'Gold' in tier else '💎'} "
            f"*عرض زبحتي الحصري!*\n\n"
            f"لديك خصم *{o['disc']}* على {cat_text_ar}!\n\n"
            f"✅ خصم {o['disc']} على طلبك القادم\n"
            f"✅ {o['bonus_ar']}\n"
            f"⏰ العرض ساري لمدة ٧ أيام فقط\n\n"
            f"🛒 اطلب الآن: zabehaty.ae\n"
            f"📌 استخدم الكود: *ZB{uid}*"
        )

        # Revenue lift: LTV × 35% conv rate × ~20% repeat order bump
        lift = round(ltv * 0.35 * 0.20, 2)

        campaign.append({
            "user_id":              uid,
            "segment":              cust.get("segment", segment),
            "lifetime_value_aed":   ltv,
            "offer_tier":           tier,
            "top_categories":       top_cats,
            "whatsapp_message_en":  msg_en,
            "whatsapp_message_ar":  msg_ar,
            "send_day":             send_day,
            "send_time":            f"{send_hour:02d}:00",
            "channel":              "WhatsApp",
            "estimated_revenue_lift_aed": lift,
        })

    total_ltv  = sum(c["lifetime_value_aed"] for c in campaign)
    total_lift = round(sum(c["estimated_revenue_lift_aed"] for c in campaign), 2)

    return {
        "data": {
            "campaign_name":             f"{segment} Personalised Promo Campaign",
            "customers_targeted":        len(campaign),
            "total_customer_ltv_aed":    round(total_ltv, 2),
            "estimated_revenue_lift_aed": total_lift,
            "optimal_send_day":          default_day,
            "optimal_send_time":         f"{default_hour:02d}:00",
            "customers":                 campaign,
        },
        "source":  ".tmp/rfm_scores.csv + user_total_orders (live)",
        "filters": f"Segment = {segment}; top {limit} by lifetime value",
        "formula": "Revenue lift = lifetime_value × 35% conversion probability × 20% repeat order increment",
        "sql":     "pre-computed from .tmp/rfm_scores.csv + live user_total_orders query per user",
    }


# ─── 23. Lost high-value users + win-back tactics ────────────────────────────

def get_lost_users_winback(min_revenue: float = 2000, limit: int = 50) -> dict:
    """
    Returns lost users (RFM Segment='Lost') who generated significant revenue,
    with their buying profile and personalised win-back tactics.
    Uses rfm_scores.csv + churn_risk.csv + buying_patterns.json.
    Falls back to a live SQL query if .tmp files are unavailable.
    """
    df_rfm   = _tmp("rfm_scores.csv")
    df_churn = _tmp("churn_risk.csv")
    patterns = _tmp("buying_patterns.json")

    # ── Live SQL fallback ────────────────────────────────────────────────────
    if df_rfm.empty:
        sql = """
            SELECT
                o.user_id,
                COUNT(DISTINCT o.id)            AS total_orders,
                SUM(o.total)                    AS lifetime_value,
                MAX(o.created_at)               AS last_order_date,
                DATEDIFF(NOW(), MAX(o.created_at)) AS days_inactive,
                GROUP_CONCAT(
                    DISTINCT c.name ORDER BY c.name SEPARATOR ', '
                )                               AS categories
            FROM orders o
            LEFT JOIN order_details od ON od.order_id = o.id
            LEFT JOIN products p       ON p.id = od.product_id
            LEFT JOIN categories c     ON c.id = p.category_id
            WHERE o.status = 3
              AND o.payment_status = 'completed'
            GROUP BY o.user_id
            HAVING days_inactive > 90
               AND lifetime_value >= %(min_rev)s
            ORDER BY lifetime_value DESC
            LIMIT %(lim)s
        """
        df_live = query_df(sql, params={"min_rev": min_revenue, "lim": limit})
        if df_live.empty:
            return {"data": [], "source": "orders table", "filters": "no lost high-value users found",
                    "formula": "days_inactive > 90 AND lifetime_value >= threshold",
                    "sql": sql.strip()}

        records = []
        for _, row in df_live.iterrows():
            days = int(row.get("days_inactive", 0))
            ltv  = round(float(row.get("lifetime_value", 0)), 2)
            cats = str(row.get("categories", ""))
            records.append({
                "user_id":         int(row["user_id"]),
                "lifetime_value":  ltv,
                "total_orders":    int(row.get("total_orders", 0)),
                "days_inactive":   days,
                "last_order_date": str(row.get("last_order_date", "")),
                "categories":      cats,
                "tactic":          _winback_tactic(days, ltv, cats, patterns),
            })
        return {
            "data":    records,
            "source":  "orders + order_details + categories tables, replica_uae",
            "filters": f"status=3, payment_status='completed', days_inactive>90, LTV>={min_revenue}",
            "formula": "lifetime value = SUM(total); inactive = days since last delivered order",
            "sql":     sql.strip(),
        }

    # ── Use pre-computed .tmp files ──────────────────────────────────────────
    # Identify lost users
    segment_col = next((c for c in ["Segment", "segment", "rfm_segment"] if c in df_rfm.columns), None)
    monetary_col = next((c for c in ["monetary", "lifetime_value", "total_spend"] if c in df_rfm.columns), None)

    if segment_col and monetary_col:
        lost = df_rfm[
            (df_rfm[segment_col].str.lower().str.contains("lost", na=False)) &
            (df_rfm[monetary_col] >= min_revenue)
        ].copy()
    elif monetary_col:
        # No segment column — use recency as proxy (haven't ordered in 90+ days)
        recency_col = next((c for c in ["recency", "recency_days", "days_inactive"] if c in df_rfm.columns), None)
        if recency_col:
            lost = df_rfm[
                (df_rfm[recency_col] > 90) &
                (df_rfm[monetary_col] >= min_revenue)
            ].copy()
        else:
            lost = df_rfm[df_rfm[monetary_col] >= min_revenue].copy()
    else:
        return {"data": [], "source": ".tmp/rfm_scores.csv",
                "filters": "could not identify segment or monetary columns",
                "formula": "n/a", "sql": "pre-computed"}

    lost = lost.nlargest(limit, monetary_col)

    # Merge churn risk score if available
    if not df_churn.empty and "user_id" in df_churn.columns and "user_id" in lost.columns:
        churn_cols = [c for c in ["user_id", "churn_risk_label", "churn_risk_score", "days_inactive"]
                      if c in df_churn.columns]
        lost = lost.merge(df_churn[churn_cols], on="user_id", how="left")

    records = []
    for _, row in lost.iterrows():
        days = int(row.get("days_inactive", row.get("recency", row.get("recency_days", 0))))
        ltv  = round(float(row.get(monetary_col, 0)), 2)
        cats = str(row.get("categories", row.get("top_categories", row.get("category_name", ""))))
        records.append({
            "user_id":         int(row.get("user_id", 0)),
            "lifetime_value_aed": ltv,
            "total_orders":    int(row.get("frequency", row.get("total_orders", 0))),
            "days_inactive":   days,
            "churn_risk":      str(row.get("churn_risk_label", "Lost")),
            "top_categories":  cats,
            "tactic":          _winback_tactic(days, ltv, cats, patterns),
        })

    return {
        "data":    records,
        "source":  ".tmp/rfm_scores.csv + .tmp/churn_risk.csv",
        "filters": f"Segment='Lost', LTV >= AED {min_revenue}, top {limit} by lifetime value",
        "formula": "RFM Segment: Lost = low recency + low frequency; ordered by lifetime monetary value",
        "sql":     "pre-computed from .tmp/rfm_scores.csv",
    }


def _winback_tactic(days_inactive: int, ltv: float, categories: str, patterns: dict) -> str:
    """Generate a personalised win-back tactic string based on user profile."""
    cats = [c.strip() for c in str(categories).split(",") if c.strip()]

    # Timing recommendation from buying_patterns
    peak_days  = patterns.get("order_timing", {}).get("peak_days", [])
    peak_hours = patterns.get("order_timing", {}).get("peak_hours", [])
    best_day   = peak_days[0]["day"]  if peak_days  else "Thursday"
    best_hour  = peak_hours[0]["hour"] if peak_hours else "13"
    timing     = f"Send on {best_day} around {best_hour}:00"

    # Category-specific hook
    cat_hook = f"featuring {cats[0]}" if cats else "with a personalized offer"

    # Value-based offer tier
    if ltv >= 10000:
        offer = "VIP reactivation: exclusive 20% discount + free delivery on next order"
    elif ltv >= 5000:
        offer = "Premium win-back: 15% discount on next order + loyalty points bonus"
    elif ltv >= 2000:
        offer = "Standard win-back: 10% discount coupon valid 7 days"
    else:
        offer = "Re-engagement: 5% discount or free delivery on next order"

    # Urgency based on recency
    if days_inactive > 365:
        urgency = "High urgency — user likely churned permanently; try one final campaign"
    elif days_inactive > 180:
        urgency = "High urgency — send within 7 days or risk permanent loss"
    elif days_inactive > 90:
        urgency = "Medium urgency — reactivation window still open"
    else:
        urgency = "Low urgency — early intervention, good recovery odds"

    return f"{offer} | Campaign {cat_hook} | {timing} | {urgency}"


# ─── Tool registry (for Claude tool_use schema) ──────────────────────────────

TOOL_DEFINITIONS = [
    {
        "name": "get_order_stats",
        "description": "Get total delivered orders, revenue, unique customers, and average order value (AOV) for a date range. Defaults to the current month if no dates are given.",
        "input_schema": {
            "type": "object",
            "properties": {
                "date_from": {"type": "string", "description": "Start date in YYYY-MM-DD format"},
                "date_to":   {"type": "string", "description": "End date in YYYY-MM-DD format"},
            },
        },
    },
    {
        "name": "get_monthly_trend",
        "description": "Get month-by-month orders, revenue, customers, and AOV for the last N months. Useful for trend analysis and MoM comparisons.",
        "input_schema": {
            "type": "object",
            "properties": {
                "months": {"type": "integer", "description": "Number of months to retrieve (default 13)", "default": 13},
            },
        },
    },
    {
        "name": "get_new_buyers",
        "description": "Count users who placed their very first completed order in the given date range. This is first-time buyers, NOT account registrations.",
        "input_schema": {
            "type": "object",
            "properties": {
                "date_from": {"type": "string", "description": "Start date YYYY-MM-DD"},
                "date_to":   {"type": "string", "description": "End date YYYY-MM-DD"},
            },
        },
    },
    {
        "name": "get_users_with_phone",
        "description": "Count users who registered in the given date range and have a phone number on file.",
        "input_schema": {
            "type": "object",
            "properties": {
                "date_from": {"type": "string", "description": "Start date YYYY-MM-DD"},
                "date_to":   {"type": "string", "description": "End date YYYY-MM-DD"},
            },
        },
    },
    {
        "name": "get_repeat_order_stats",
        "description": "Count repeat orders and buyers (users who placed more than one order) in the given date range.",
        "input_schema": {
            "type": "object",
            "properties": {
                "date_from": {"type": "string", "description": "Start date YYYY-MM-DD"},
                "date_to":   {"type": "string", "description": "End date YYYY-MM-DD"},
            },
        },
    },
    {
        "name": "get_top_shops",
        "description": "Get top shops ranked by revenue for a given date range.",
        "input_schema": {
            "type": "object",
            "properties": {
                "date_from": {"type": "string", "description": "Start date YYYY-MM-DD"},
                "date_to":   {"type": "string", "description": "End date YYYY-MM-DD"},
                "limit":     {"type": "integer", "description": "Number of shops to return (default 10)"},
            },
        },
    },
    {
        "name": "get_top_products",
        "description": "Get top products by revenue from the pre-computed analysis.",
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "description": "Number of products to return (default 10)"},
            },
        },
    },
    {
        "name": "get_category_performance",
        "description": "Get revenue, orders, margin, and revenue share for each product category.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_user_segments",
        "description": "Get the RFM user segment breakdown — count, avg LTV, revenue share, and avg recency per segment (Champions, Loyal, At Risk, etc.).",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_churn_stats",
        "description": "Get churn risk distribution (Critical / High / Medium / Low) and total at-risk user count.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_ltv_stats",
        "description": "Get LTV tier distribution (Platinum / Gold / Silver / Bronze) with user counts and revenue per tier.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_payment_methods",
        "description": "Get payment method split by order count and share percentage.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_peak_timing",
        "description": "Get peak days of the week and peak hours for order placement.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_bcg_summary",
        "description": "Get BCG matrix quadrant summary (Stars, Cash Cows, Question Marks, Dogs) and top Star products.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_revenue_per_user",
        "description": "Get average and total revenue per unique buyer for a date range.",
        "input_schema": {
            "type": "object",
            "properties": {
                "date_from": {"type": "string", "description": "Start date YYYY-MM-DD"},
                "date_to":   {"type": "string", "description": "End date YYYY-MM-DD"},
            },
        },
    },
    {
        "name": "forecast_metric",
        "description": "Project revenue or orders forward N weeks or months using linear trend on historical data. Returns a point estimate with ±15% confidence band.",
        "input_schema": {
            "type": "object",
            "properties": {
                "metric":      {"type": "string", "enum": ["revenue", "orders"], "description": "What to forecast"},
                "periods":     {"type": "integer", "description": "How many periods ahead (default 4)"},
                "period_unit": {"type": "string", "enum": ["weeks", "months"], "description": "Unit of forecast (default 'weeks')"},
            },
        },
    },
    {
        "name": "get_cancellation_stats",
        "description": "Get cancelled order count, cancellation rate %, cancelled GMV, and top cancellation reasons for a date range. Covers ALL orders (not just delivered), so it answers questions like 'how many orders were cancelled?' or 'what is our cancellation rate?'",
        "input_schema": {
            "type": "object",
            "properties": {
                "date_from": {"type": "string", "description": "Start date YYYY-MM-DD (default: start of current month)"},
                "date_to":   {"type": "string", "description": "End date YYYY-MM-DD (default: today)"},
            },
        },
    },
    {
        "name": "export_excel_report",
        "description": "Build and return an Excel report with the requested columns for a date range. Supported columns include: new_buyers, phone_users, first_orders, repeat_orders, total_orders, total_revenue, revenue_per_user, aov, customers, top_shops, top_products, user_segments, churn_stats, ltv_stats, category_performance, payment_methods, bcg, bcg_matrix, lost_users, win_back, customer_behavior. Pass 'all' or 'core' in columns to include every available sheet. You MUST calculate date_from/date_to when the user says 'last N months' or 'last N weeks' — do not leave them blank.",
        "input_schema": {
            "type": "object",
            "required": ["columns"],
            "properties": {
                "columns":     {"type": "array", "items": {"type": "string"}, "description": "List of metric/sheet names. Use 'all' or 'core' for everything. Use 'bcg' for BCG matrix. Use 'lost_users' or 'customer_behavior' for win-back sheet."},
                "date_from":   {"type": "string", "description": "Start date YYYY-MM-DD. REQUIRED when user specifies a time range like 'last 6 months'. Calculate it before calling this tool."},
                "date_to":     {"type": "string", "description": "End date YYYY-MM-DD. REQUIRED when user specifies a time range. Use today's date when not specified."},
                "report_name": {"type": "string", "description": "Base filename for the download (no extension)"},
            },
        },
    },
    {
        "name": "get_lost_users_winback",
        "description": "Get a list of lost/churned users who previously generated significant revenue, with their buying profile and a personalised win-back tactic for each user. Use this when asked about lost customers, churned users, win-back campaigns, or re-engagement strategies.",
        "input_schema": {
            "type": "object",
            "properties": {
                "min_revenue": {"type": "number", "description": "Minimum lifetime revenue (AED) to include a user. Default 2000."},
                "limit":       {"type": "integer", "description": "Max number of users to return. Default 50."},
            },
        },
    },
    {
        "name": "get_ltv_average",
        "description": "Get the overall average and median LTV (lifetime value) across ALL customers, plus a breakdown by RFM segment (Champions, Loyal, Lost, etc.). Use this whenever someone asks 'what is our average LTV' or 'average customer value'.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_cross_sell_opportunities",
        "description": "Get top category pairs for cross-selling — which categories are most commonly bought together, with co-buyer counts, affinity percentages, and ready-to-use offer suggestions. Use when asked about cross-selling, upselling, bundling, or boosting sales.",
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "description": "Number of category pairs to return (default 10)"},
            },
        },
    },
    {
        "name": "get_top_customers",
        "description": "Get the list of top customers in a given RFM segment (Champions, Loyal Customers, etc.) sorted by lifetime value, with their offer tier. Use before generating a promotion campaign.",
        "input_schema": {
            "type": "object",
            "properties": {
                "segment": {"type": "string", "description": "RFM segment name: Champions, Loyal Customers, At Risk, Lost, etc. Use 'all' for top spenders overall.", "default": "Champions"},
                "limit":   {"type": "integer", "description": "Number of customers to return (default 20)"},
            },
        },
    },
    {
        "name": "get_customer_buying_profile",
        "description": "Get the buying profile for a specific customer: their top categories by spend, AOV per category, and inferred preferred order time. Use this to personalise a promotion for an individual user.",
        "input_schema": {
            "type": "object",
            "required": ["user_id"],
            "properties": {
                "user_id": {"type": "integer", "description": "The customer's user_id (required)"},
            },
        },
    },
    {
        "name": "generate_promo_campaign",
        "description": "Generate a full personalised promotion campaign for top customers: WhatsApp messages (Arabic + English) with offer tiers, personalised category hooks, optimal send day/time per customer, and revenue lift estimates. Use when asked to create promotional content, WhatsApp messages, notifications, or communication campaigns.",
        "input_schema": {
            "type": "object",
            "properties": {
                "segment": {"type": "string", "description": "Target segment: Champions, Loyal Customers, At Risk, etc. Default: Champions", "default": "Champions"},
                "limit":   {"type": "integer", "description": "Number of customers to include (default 15)"},
            },
        },
    },
]


# ─── Dispatcher ──────────────────────────────────────────────────────────────

TOOL_FN_MAP = {
    "get_order_stats":         get_order_stats,
    "get_monthly_trend":       get_monthly_trend,
    "get_new_buyers":          get_new_buyers,
    "get_users_with_phone":    get_users_with_phone,
    "get_repeat_order_stats":  get_repeat_order_stats,
    "get_top_shops":           get_top_shops,
    "get_top_products":        get_top_products,
    "get_category_performance":get_category_performance,
    "get_user_segments":       get_user_segments,
    "get_churn_stats":         get_churn_stats,
    "get_ltv_stats":           get_ltv_stats,
    "get_payment_methods":     get_payment_methods,
    "get_peak_timing":         get_peak_timing,
    "get_bcg_summary":         get_bcg_summary,
    "get_revenue_per_user":    get_revenue_per_user,
    "get_cancellation_stats":  get_cancellation_stats,
    "forecast_metric":         forecast_metric,
    "export_excel_report":     export_excel_report,
    "get_lost_users_winback":          get_lost_users_winback,
    "get_ltv_average":                 get_ltv_average,
    "get_cross_sell_opportunities":    get_cross_sell_opportunities,
    "get_top_customers":               get_top_customers,
    "get_customer_buying_profile":     get_customer_buying_profile,
    "generate_promo_campaign":         generate_promo_campaign,
}


def dispatch_tool(name: str, inputs: dict) -> dict:
    """Execute a tool by name with the given inputs. Returns the tool result dict."""
    fn = TOOL_FN_MAP.get(name)
    if fn is None:
        return {"data": None, "source": "unknown", "filters": "", "formula": "",
                "sql": "", "error": f"Unknown tool: {name}"}
    try:
        return fn(**inputs)
    except Exception as e:
        return {"data": None, "source": name, "filters": str(inputs),
                "formula": "", "sql": "", "error": str(e)}
