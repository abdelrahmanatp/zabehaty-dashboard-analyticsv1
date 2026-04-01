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
    Top products by revenue from the pre-computed BCG analysis.
    """
    df = _tmp("top_products.csv")
    if df.empty:
        return {"data": [], "source": ".tmp/top_products.csv", "filters": "pre-computed, all delivered orders",
                "formula": "SUM(line_revenue) per product", "sql": "pre-computed from .tmp/top_products.csv"}

    cols = ["product_id", "product_name", "category_name", "shop_name",
            "total_revenue", "total_units", "total_orders", "avg_margin"]
    avail = [c for c in cols if c in df.columns]
    top = df.nlargest(limit, "total_revenue")[avail] if "total_revenue" in df.columns else df.head(limit)[avail]

    records = top.to_dict(orient="records")
    return {
        "data":    records,
        "source":  ".tmp/top_products.csv (generated by tools/product_analysis.py)",
        "filters": f"top {limit} by total_revenue; all delivered orders in dataset",
        "formula": "SUM(price * quantity) per product from order_details joined to delivered orders",
        "sql":     "pre-computed from .tmp/top_products.csv",
    }


# ─── 8. Category performance ─────────────────────────────────────────────────

def get_category_performance() -> dict:
    """
    Revenue, orders, margin, and revenue share per product category.
    """
    df = _tmp("category_performance.csv")
    if df.empty:
        return {"data": [], "source": ".tmp/category_performance.csv",
                "filters": "pre-computed", "formula": "SUM per category", "sql": "pre-computed"}

    records = df.to_dict(orient="records")
    return {
        "data":    records,
        "source":  ".tmp/category_performance.csv (generated by tools/product_analysis.py)",
        "filters": "all delivered orders in dataset",
        "formula": "SUM(line_revenue), COUNT(DISTINCT order_id), AVG(line_margin) grouped by category",
        "sql":     "pre-computed from .tmp/category_performance.csv",
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


# ─── 16. Forecast ────────────────────────────────────────────────────────────

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

    # Collect data for each requested column
    sheets = {}

    # --- Summary sheet ---
    summary_rows = []

    ORDER_COLS = {"total_orders", "total_revenue", "revenue_per_user", "aov", "customers",
                  "new_buyers", "new_users", "first_orders", "repeat_orders", "phone_users"}

    if ORDER_COLS.intersection(set(cols)):
        stats   = get_order_stats(date_from, date_to)["data"]
        buyers  = get_new_buyers(date_from, date_to)["data"]
        repeat  = get_repeat_order_stats(date_from, date_to)["data"]
        phones  = get_users_with_phone(date_from, date_to)["data"]
        rev_pu  = get_revenue_per_user(date_from, date_to)["data"]

        col_map = {
            "total_orders":    ("Total Orders",          stats.get("orders", 0)),
            "total_revenue":   ("Total Revenue (AED)",   stats.get("revenue_aed", 0)),
            "revenue_per_user":("Revenue Per User (AED)",rev_pu.get("revenue_per_user", 0)),
            "aov":             ("Average Order Value (AED)", stats.get("aov_aed", 0)),
            "customers":       ("Unique Customers",      stats.get("customers", 0)),
            "new_buyers":      ("New Buyers",            buyers.get("new_buyers", 0)),
            "new_users":       ("New Buyers",            buyers.get("new_buyers", 0)),
            "first_orders":    ("New Buyers",            buyers.get("new_buyers", 0)),
            "repeat_orders":   ("Repeat Orders",         repeat.get("repeat_orders", 0)),
            "phone_users":     ("Users With Phone",      phones.get("with_phone", 0)),
        }

        for col_key in cols:
            if col_key in col_map:
                label, value = col_map[col_key]
                summary_rows.append({"Metric": label, "Value": value,
                                     "Period": f"{date_from} to {date_to}"})

    if summary_rows:
        sheets["Summary"] = pd.DataFrame(summary_rows)

    # --- Monthly trend sheet ---
    if any(c in cols for c in ("monthly_trend", "trend")):
        trend = get_monthly_trend()["data"]
        sheets["Monthly Trend"] = pd.DataFrame(trend)

    # --- Top shops sheet ---
    if any(c in cols for c in ("top_shops", "shops")):
        shop_data = get_top_shops(date_from, date_to, limit=20)["data"]
        sheets["Top Shops"] = pd.DataFrame(shop_data).drop(columns=["shop_ar"], errors="ignore")

    # --- Top products sheet ---
    if any(c in cols for c in ("top_products", "products")):
        prod_data = get_top_products(limit=30)["data"]
        sheets["Top Products"] = pd.DataFrame(prod_data)

    # --- Category performance ---
    if any(c in cols for c in ("category_performance", "categories")):
        cat_data = get_category_performance()["data"]
        sheets["Category Performance"] = pd.DataFrame(cat_data)

    # --- User segments ---
    if any(c in cols for c in ("user_segments", "segments", "rfm")):
        seg_data = get_user_segments()["data"]
        sheets["User Segments"] = pd.DataFrame(seg_data)

    # --- Churn stats ---
    if any(c in cols for c in ("churn_stats", "churn")):
        churn = get_churn_stats()["data"]
        dist  = churn.get("distribution", {})
        sheets["Churn Risk"] = pd.DataFrame([{"Risk Level": k, "Users": v} for k, v in dist.items()])

    # --- LTV stats ---
    if any(c in cols for c in ("ltv_stats", "ltv")):
        ltv_data = get_ltv_stats()["data"]
        sheets["LTV Tiers"] = pd.DataFrame([
            {"Tier": k, **v} for k, v in ltv_data.items()
        ])

    # --- Payment methods ---
    if any(c in cols for c in ("payment_methods", "payments")):
        pay_data = get_payment_methods()["data"]
        sheets["Payment Methods"] = pd.DataFrame(pay_data)

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
        "name": "export_excel_report",
        "description": "Build and return an Excel report with the requested columns for a date range. Supported columns include: new_buyers, phone_users, first_orders, repeat_orders, total_orders, total_revenue, revenue_per_user, aov, customers, top_shops, top_products, user_segments, churn_stats, ltv_stats, category_performance, payment_methods.",
        "input_schema": {
            "type": "object",
            "required": ["columns"],
            "properties": {
                "columns":     {"type": "array", "items": {"type": "string"}, "description": "List of metric names to include"},
                "date_from":   {"type": "string", "description": "Start date YYYY-MM-DD"},
                "date_to":     {"type": "string", "description": "End date YYYY-MM-DD"},
                "report_name": {"type": "string", "description": "Base filename for the download (no extension)"},
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
    "forecast_metric":         forecast_metric,
    "export_excel_report":     export_excel_report,
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
