"""
generate_synthetic_gap.py
=========================
Generates synthetic order records for the 97-day data gap:
  December 2025, January 2026, February 2026

Methodology:
  1. Pull historical patterns from live MySQL (read-only)
  2. Predict monthly volumes using YoY growth factor
     predicted_orders(M, Y) = base_orders(M, Y-1) × yoy_factor
     yoy_factor = median of [Jul–Oct 2025 / Jul–Oct 2024] ratios
  3. Distribute orders across days using historical day-of-week weights
  4. Sample user_id, shop_id, category_id, payment_method from historical distributions
  5. Sample order totals from log-normal distribution per category
  6. Write to .tmp/synthetic_gap.db (SQLite) + .tmp/synthetic_gap_summary.json

Run:
  python tools/generate_synthetic_gap.py
"""

import os, sys, json, sqlite3, warnings
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

warnings.filterwarnings("ignore")
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, os.path.dirname(__file__))
from db_connect import query_df

load_dotenv()

ROOT = os.path.dirname(os.path.dirname(__file__))
TMP  = os.path.join(ROOT, ".tmp")
os.makedirs(TMP, exist_ok=True)

DB_PATH      = os.path.join(TMP, "synthetic_gap.db")
SUMMARY_PATH = os.path.join(TMP, "synthetic_gap_summary.json")

GAP_MONTHS   = ["2025-12", "2026-01", "2026-02"]
SAME_LY      = {"2025-12": "2024-12", "2026-01": "2025-01", "2026-02": "2025-02"}
YOY_BASE_MONTHS = [("2025-07","2024-07"), ("2025-08","2024-08"),
                   ("2025-09","2024-09"), ("2025-10","2024-10")]

SEED = 42
rng = np.random.default_rng(SEED)

GREEN = "\033[92m"; YELLOW = "\033[93m"; BOLD = "\033[1m"; RESET = "\033[0m"

def hdr(t):
    print(f"\n{BOLD}── {t} ──{RESET}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — PULL PATTERNS
# ══════════════════════════════════════════════════════════════════════════════
hdr("STEP 1: Pulling historical patterns from live DB")

# Monthly aggregates
df_monthly = query_df("""
    SELECT DATE_FORMAT(created_at,'%Y-%m') AS month,
           COUNT(DISTINCT id) AS orders,
           COUNT(DISTINCT user_id) AS customers,
           SUM(total) AS revenue,
           AVG(total) AS aov
    FROM orders
    WHERE status=3 AND payment_status='completed'
    GROUP BY DATE_FORMAT(created_at,'%Y-%m')
    ORDER BY month
""")
monthly = {r.month: r for _, r in df_monthly.iterrows()}
print(f"  Monthly records loaded: {len(df_monthly)}")

# Max order ID (synthetic IDs start well above this)
max_id_row = query_df("SELECT MAX(id) AS max_id FROM orders")
max_live_id = int(max_id_row.iloc[0]["max_id"] or 0)
synth_id_start = max_live_id + 50_000
print(f"  Max live order ID: {max_live_id:,}  →  synthetic IDs start at {synth_id_start:,}")

# User pool (active users with ≥1 completed order, with order-count weights)
df_users = query_df("""
    SELECT user_id, COUNT(*) AS order_count
    FROM orders
    WHERE status=3 AND payment_status='completed'
    GROUP BY user_id
    HAVING order_count >= 1
""")
user_ids    = df_users["user_id"].astype(int).values
user_weights = df_users["order_count"].astype(float).values
user_weights /= user_weights.sum()
print(f"  User pool: {len(user_ids):,} active users")

# Shop distribution
df_shops = query_df("""
    SELECT shop_id, COUNT(*) AS orders
    FROM orders
    WHERE status=3 AND payment_status='completed' AND shop_id IS NOT NULL
    GROUP BY shop_id
""")
# Add NULL (direct orders) — historically ~7% of orders have no shop_id
direct_count = int(df_shops["orders"].sum() * 0.07 / 0.93)
shop_ids_raw    = df_shops["shop_id"].astype(float).values.tolist() + [None]
shop_weights_raw = df_shops["orders"].astype(float).values.tolist() + [direct_count]
shop_weights_arr = np.array(shop_weights_raw, dtype=float)
shop_weights_arr /= shop_weights_arr.sum()
print(f"  Shop pool: {len(shop_ids_raw):,} shops (+1 direct)")

# Category distribution
df_cats = query_df("""
    SELECT category_id, COUNT(*) AS orders
    FROM orders
    WHERE status=3 AND payment_status='completed' AND category_id IS NOT NULL
    GROUP BY category_id
""")
cat_ids     = df_cats["category_id"].astype(int).values
cat_weights = df_cats["orders"].astype(float).values
cat_weights /= cat_weights.sum()
print(f"  Category pool: {len(cat_ids):,} categories")

# Payment method distribution
df_pay = query_df("""
    SELECT payment_method, COUNT(*) AS cnt
    FROM orders
    WHERE status=3 AND payment_status='completed'
    GROUP BY payment_method
""")
pay_methods = df_pay["payment_method"].astype(int).values
pay_weights = df_pay["cnt"].astype(float).values
pay_weights /= pay_weights.sum()
print(f"  Payment methods: {len(pay_methods)}")

# Day-of-week distribution (1=Sun … 7=Sat in MySQL DAYOFWEEK)
df_dow = query_df("""
    SELECT DAYOFWEEK(created_at) AS dow, COUNT(*) AS cnt
    FROM orders
    WHERE status=3 AND payment_status='completed'
    GROUP BY DAYOFWEEK(created_at)
""")
dow_map = dict(zip(df_dow["dow"].astype(int), df_dow["cnt"].astype(float)))
dow_weights = np.array([dow_map.get(d, 1.0) for d in range(1, 8)], dtype=float)
dow_weights /= dow_weights.sum()
# dow_weights[0] = Sunday, [1] = Monday, … [6] = Saturday

# AOV statistics by category (for log-normal sampling)
df_aov = query_df("""
    SELECT category_id,
           AVG(total) AS mean_total,
           STDDEV(total) AS std_total
    FROM orders
    WHERE status=3 AND payment_status='completed' AND total > 0
    GROUP BY category_id
""")
aov_by_cat = {}
for _, r in df_aov.iterrows():
    cid = int(r["category_id"])
    mu  = float(r["mean_total"] or 900)
    sd  = float(r["std_total"]  or 180)
    # Convert to log-normal params
    variance = sd**2
    lnmu  = np.log(mu**2 / np.sqrt(variance + mu**2))
    lnsig = np.sqrt(np.log(1 + variance / mu**2))
    aov_by_cat[cid] = (lnmu, max(lnsig, 0.01))
# Fallback params (platform-wide)
mu_all = float(df_monthly["aov"].mean() or 900)
sd_all = 180.0
lnmu_fb  = np.log(mu_all**2 / np.sqrt(sd_all**2 + mu_all**2))
lnsig_fb = np.sqrt(np.log(1 + sd_all**2 / mu_all**2))
aov_fallback = (lnmu_fb, lnsig_fb)
print(f"  AOV params computed for {len(aov_by_cat):,} categories")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — PREDICT MONTHLY VOLUMES
# ══════════════════════════════════════════════════════════════════════════════
hdr("STEP 2: Predicting monthly volumes")

# Compute YoY growth factor from Jul–Oct 2025 vs 2024 (excludes Nov outlier)
yoy_factors = []
for cy, py in YOY_BASE_MONTHS:
    if cy in monthly and py in monthly:
        cy_orders = float(monthly[cy].orders)
        py_orders = float(monthly[py].orders)
        if py_orders > 0:
            f = cy_orders / py_orders
            yoy_factors.append(f)
            print(f"  YoY {cy}/{py}: {cy_orders:.0f}/{py_orders:.0f} = {f:.3f}")

yoy_factor = float(np.median(yoy_factors)) if yoy_factors else 1.05
# Dampen by 15% to account for Nov 2025 weakness signal
yoy_factor = yoy_factor * 0.85
print(f"\n  Median YoY factor (dampened): {yoy_factor:.3f}")

# Predict each gap month
predictions = {}
for gap_month in GAP_MONTHS:
    ly_month = SAME_LY[gap_month]
    if ly_month in monthly:
        ly = monthly[ly_month]
        pred_orders    = max(int(round(float(ly.orders)    * yoy_factor)), 100)
        pred_customers = max(int(round(float(ly.customers) * yoy_factor)), 50)
        pred_aov       = float(ly.aov) * 1.03   # slight AOV uplift trend
        pred_revenue   = pred_orders * pred_aov
    else:
        # Fallback: use platform average for that month
        pred_orders    = 5000
        pred_customers = 4000
        pred_aov       = mu_all
        pred_revenue   = pred_orders * pred_aov

    predictions[gap_month] = {
        "month":     gap_month,
        "orders":    pred_orders,
        "customers": pred_customers,
        "revenue":   round(pred_revenue, 2),
        "aov":       round(pred_aov, 2),
        "ly_month":  ly_month,
    }
    ly_row = monthly.get(ly_month)
    ly_orders = float(ly_row.orders) if ly_row is not None else 0
    print(f"\n  {gap_month}:")
    print(f"    Base (last year {ly_month}):  {ly_orders:,.0f} orders")
    print(f"    Predicted:                   {pred_orders:,} orders | AED {pred_revenue:,.0f} GMV | AOV {pred_aov:.0f}")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — GENERATE ORDER RECORDS
# ══════════════════════════════════════════════════════════════════════════════
hdr("STEP 3: Generating synthetic order records")

all_orders = []
order_id_counter = synth_id_start

for gap_month in GAP_MONTHS:
    pred   = predictions[gap_month]
    year, mo = int(gap_month[:4]), int(gap_month[5:])
    # Build list of calendar days in this month
    from calendar import monthrange
    days_in_month = monthrange(year, mo)[1]
    cal_days = [datetime(year, mo, d) for d in range(1, days_in_month + 1)]

    # Weight each calendar day by its day-of-week weight
    # Python weekday(): Mon=0 … Sun=6
    # MySQL DAYOFWEEK: Sun=1 … Sat=7
    # Convert: mysql_dow = (python_weekday + 1) % 7 + 1
    day_weights = np.array([dow_weights[(d.weekday() + 1) % 7] for d in cal_days])
    day_weights /= day_weights.sum()

    # How many orders per day
    n_total = pred["orders"]
    day_counts = rng.multinomial(n_total, day_weights)

    month_orders = []
    for day_idx, n_orders in enumerate(day_counts):
        if n_orders == 0:
            continue
        day = cal_days[day_idx]
        # Random hours for each order (weighted by hour-of-day pattern)
        # Using a simplified distribution: peak 12-14, secondary 19-21, trough 02-06
        hour_probs = np.array([
            0.005, 0.003, 0.003, 0.003, 0.005, 0.010,  # 00-05
            0.020, 0.035, 0.045, 0.055, 0.060, 0.070,  # 06-11
            0.085, 0.085, 0.075, 0.060, 0.055, 0.055,  # 12-17
            0.060, 0.065, 0.060, 0.050, 0.030, 0.010,  # 18-23
        ], dtype=float)
        hour_probs /= hour_probs.sum()
        hours   = rng.choice(24, size=n_orders, p=hour_probs)
        minutes = rng.integers(0, 60, size=n_orders)
        seconds = rng.integers(0, 60, size=n_orders)

        # Sample demographics
        sampled_users    = rng.choice(user_ids, size=n_orders, p=user_weights)
        sampled_shops    = rng.choice(len(shop_ids_raw), size=n_orders, p=shop_weights_arr)
        sampled_cats     = rng.choice(cat_ids, size=n_orders, p=cat_weights)
        sampled_pays     = rng.choice(pay_methods, size=n_orders, p=pay_weights)

        for i in range(n_orders):
            cid = int(sampled_cats[i])
            lnmu, lnsig = aov_by_cat.get(cid, aov_fallback)
            total_raw = float(rng.lognormal(lnmu, lnsig))
            # Round to nearest 0.25 (common price granularity)
            total = round(total_raw / 0.25) * 0.25
            total = max(total, 50.0)

            # Discount: 4.5% of orders
            has_discount = rng.random() < 0.045
            discount     = round(float(rng.uniform(30, 100)), 2) if has_discount else 0.0

            # Service fee: ~2.5% of total
            service_fee = round(total * 0.025, 2)

            # Delivery: 70% pay AED 10, 30% free
            delivery = 10.0 if rng.random() < 0.70 else 0.0

            # Rating: 60% give 5 stars, 25% give 4, 10% give 3, 5% no rating
            r = rng.random()
            rating = 5 if r < 0.60 else (4 if r < 0.85 else (3 if r < 0.95 else 0))

            shop_idx = int(sampled_shops[i])
            shop_id_val = shop_ids_raw[shop_idx]  # may be None (direct)

            created_dt = day + timedelta(
                hours=int(hours[i]), minutes=int(minutes[i]), seconds=int(seconds[i])
            )
            updated_dt = created_dt + timedelta(minutes=int(rng.integers(15, 120)))

            month_orders.append({
                "id":             order_id_counter,
                "user_id":        int(sampled_users[i]),
                "shop_id":        int(shop_id_val) if shop_id_val is not None else None,
                "category_id":    cid,
                "total":          round(total, 2),
                "subtotal":       round(total - service_fee - delivery, 2),
                "discount_total": discount,
                "delivery":       delivery,
                "service_fee":    service_fee,
                "status":         3,
                "payment_status": "completed",
                "payment_method": int(sampled_pays[i]),
                "rating":         int(rating),
                "created_at":     created_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "updated_at":     updated_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "is_synthetic":   1,
            })
            order_id_counter += 1

    all_orders.extend(month_orders)
    print(f"  {gap_month}: generated {len(month_orders):,} orders")

df_synthetic = pd.DataFrame(all_orders)
print(f"\n  Total synthetic orders: {len(df_synthetic):,}")
print(f"  ID range: {df_synthetic['id'].min():,} – {df_synthetic['id'].max():,}")
print(f"  Total synthetic GMV: AED {df_synthetic['total'].sum():,.0f}")
print(f"  Unique users: {df_synthetic['user_id'].nunique():,}")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — WRITE TO SQLITE
# ══════════════════════════════════════════════════════════════════════════════
hdr("STEP 4: Writing to SQLite")

# Remove old DB if exists
if os.path.exists(DB_PATH):
    os.remove(DB_PATH)

conn = sqlite3.connect(DB_PATH)
cur  = conn.cursor()

cur.execute("""
CREATE TABLE orders (
    id              INTEGER PRIMARY KEY,
    user_id         INTEGER NOT NULL,
    shop_id         INTEGER,
    category_id     INTEGER,
    total           REAL,
    subtotal        REAL,
    discount_total  REAL DEFAULT 0,
    delivery        REAL DEFAULT 0,
    service_fee     REAL DEFAULT 0,
    status          INTEGER DEFAULT 3,
    payment_status  TEXT DEFAULT 'completed',
    payment_method  INTEGER,
    rating          INTEGER,
    created_at      TEXT,
    updated_at      TEXT,
    is_synthetic    INTEGER DEFAULT 1
)
""")

cur.execute("""
CREATE TABLE monthly_summary (
    month       TEXT PRIMARY KEY,
    orders      INTEGER,
    customers   INTEGER,
    revenue     REAL,
    aov         REAL
)
""")

# Insert orders in batches
batch_size = 5000
rows = df_synthetic.to_dict(orient="records")
for i in range(0, len(rows), batch_size):
    batch = rows[i:i + batch_size]
    cur.executemany("""
        INSERT INTO orders (id,user_id,shop_id,category_id,total,subtotal,
            discount_total,delivery,service_fee,status,payment_status,
            payment_method,rating,created_at,updated_at,is_synthetic)
        VALUES (:id,:user_id,:shop_id,:category_id,:total,:subtotal,
            :discount_total,:delivery,:service_fee,:status,:payment_status,
            :payment_method,:rating,:created_at,:updated_at,:is_synthetic)
    """, batch)

# Insert monthly summaries
for gap_month in GAP_MONTHS:
    sub = df_synthetic[df_synthetic["created_at"].str.startswith(gap_month.replace("-", "-")
                                                                   .replace("20", "20"))]
    # Use created_at prefix match
    sub = df_synthetic[df_synthetic["created_at"].str[:7] == gap_month]
    n_orders    = len(sub)
    n_customers = sub["user_id"].nunique()
    revenue     = round(float(sub["total"].sum()), 2)
    aov         = round(float(sub["total"].mean()), 2) if n_orders else 0
    cur.execute(
        "INSERT INTO monthly_summary VALUES (?,?,?,?,?)",
        (gap_month, n_orders, n_customers, revenue, aov)
    )

conn.commit()
conn.close()

db_size_kb = os.path.getsize(DB_PATH) // 1024
print(f"  {GREEN}✓ SQLite DB written: {DB_PATH} ({db_size_kb:,} KB){RESET}")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 5 — WRITE SUMMARY JSON
# ══════════════════════════════════════════════════════════════════════════════
hdr("STEP 5: Writing summary JSON")

monthly_summary = []
for gap_month in GAP_MONTHS:
    sub = df_synthetic[df_synthetic["created_at"].str[:7] == gap_month]
    monthly_summary.append({
        "month":     gap_month,
        "orders":    int(len(sub)),
        "customers": int(sub["user_id"].nunique()),
        "revenue":   round(float(sub["total"].sum()), 2),
        "aov":       round(float(sub["total"].mean()), 2) if len(sub) else 0,
        "is_synthetic": True,
    })

summary = {
    "generated_at":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "methodology":   "YoY growth factor applied to same month prior year, "
                     "distributed by historical day-of-week weights, "
                     "log-normal order value sampling per category.",
    "yoy_factor_used": round(yoy_factor, 4),
    "gap_months":    GAP_MONTHS,
    "monthly_summary": monthly_summary,
    "has_synthetic": True,
}

with open(SUMMARY_PATH, "w", encoding="utf-8") as f:
    json.dump(summary, f, indent=2, default=str)
print(f"  {GREEN}✓ Summary JSON written: {SUMMARY_PATH}{RESET}")


# ══════════════════════════════════════════════════════════════════════════════
# FINAL REPORT
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n{BOLD}══════════════════════════════════════════════════════════{RESET}")
print(f"{BOLD}  SYNTHETIC DATA GENERATION COMPLETE{RESET}")
print(f"{BOLD}══════════════════════════════════════════════════════════{RESET}")
print(f"\n  {'Month':<12}  {'Pred Orders':>12}  {'Actual LY':>12}  {'GMV (AED)':>14}  {'AOV':>8}")
print(f"  {'─'*12}  {'─'*12}  {'─'*12}  {'─'*14}  {'─'*8}")
for s in monthly_summary:
    ly_m  = SAME_LY[s["month"]]
    ly_r  = monthly.get(ly_m)
    ly_ord = int(float(ly_r.orders)) if ly_r is not None else 0
    print(f"  {s['month']:<12}  {s['orders']:>12,}  {ly_ord:>12,}  {s['revenue']:>14,.0f}  {s['aov']:>8.0f}")
print(f"\n  YoY factor applied : {yoy_factor:.3f}")
print(f"  Total synthetic GMV: AED {df_synthetic['total'].sum():,.0f}")
print(f"  Total synth orders : {len(df_synthetic):,}")
print(f"\n  Files created:")
print(f"    {DB_PATH}")
print(f"    {SUMMARY_PATH}")
print()

if __name__ == "__main__":
    pass
