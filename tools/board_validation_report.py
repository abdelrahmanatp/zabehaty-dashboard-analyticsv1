"""
board_validation_report.py
===========================
Board-level validation + monthly health report.

Scenario: Board member wants to understand how the business is performing
this month (March 2026) vs last 3 months (Dec 2025, Jan 2026, Feb 2026)
and vs same season last year (March 2025).

Checks:
  [A] Data currency — confirm the DB contains March 2026 data (not stale)
  [B] Monthly GMV trend   — Dec 25, Jan 26, Feb 26, Mar 26 + Mar 25 YoY
  [C] Orders & AOV trend
  [D] Active buyers trend (unique users who ordered)
  [E] New user acquisition trend
  [F] Revenue by channel (Own Brand vs Marketplace vs Direct)
  [G] Top categories this month vs 3-month avg
  [H] Churn signal — users who ordered in Jan/Feb but NOT yet in March
  [I] Summary scorecard

Run:
  python tools/board_validation_report.py
"""

import os, sys
import pandas as pd
import numpy as np
from datetime import datetime
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, os.path.dirname(__file__))
from db_connect import query_df

load_dotenv()

# ── colours ─────────────────────────────────────────────────────────────────
GREEN  = "\033[92m"; YELLOW = "\033[93m"; RED = "\033[91m"
BLUE   = "\033[94m"; BOLD   = "\033[1m";  RESET = "\033[0m"
CYAN   = "\033[96m"; MAG    = "\033[95m"

def hdr(title):
    print(f"\n{BOLD}{CYAN}{'─'*62}")
    print(f"  {title}")
    print(f"{'─'*62}{RESET}")

def arrow(val, reverse=False):
    """Green up-arrow if positive (or reverse: green for negative)."""
    if val is None or np.isnan(val): return "  n/a"
    if reverse:
        col = GREEN if val <= 0 else RED
    else:
        col = GREEN if val >= 0 else RED
    sym = "▲" if val >= 0 else "▼"
    return f"{col}{sym} {abs(val):.1f}%{RESET}"

now = datetime.now()
print()
print(f"{BOLD}╔══════════════════════════════════════════════════════════════╗")
print(f"║  ZABEHATY BOARD VALIDATION & MONTHLY HEALTH REPORT          ║")
print(f"║  Generated: {now.strftime('%Y-%m-%d %H:%M')}  |  Analysis Date: March 2026    ║")
print(f"╚══════════════════════════════════════════════════════════════╝{RESET}")


# ════════════════════════════════════════════════════════════════════════════
# [A] DATA CURRENCY CHECK
# ════════════════════════════════════════════════════════════════════════════
hdr("[A] DATA CURRENCY CHECK — Is the DB showing March 2026?")

currency_sql = """
SELECT
    MAX(created_at)                          AS latest_order_date,
    MIN(created_at)                          AS earliest_order_date,
    DATE_FORMAT(MAX(created_at), '%Y-%m')    AS latest_month,
    COUNT(*)                                 AS total_orders_in_db,
    SUM(CASE WHEN created_at >= '2026-03-01' THEN 1 ELSE 0 END) AS mar26_orders,
    SUM(CASE WHEN created_at >= '2026-02-01'
              AND created_at  < '2026-03-01' THEN 1 ELSE 0 END) AS feb26_orders,
    SUM(CASE WHEN created_at >= '2026-01-01'
              AND created_at  < '2026-02-01' THEN 1 ELSE 0 END) AS jan26_orders,
    SUM(CASE WHEN created_at >= '2025-12-01'
              AND created_at  < '2026-01-01' THEN 1 ELSE 0 END) AS dec25_orders,
    SUM(CASE WHEN created_at >= '2025-03-01'
              AND created_at  < '2025-04-01' THEN 1 ELSE 0 END) AS mar25_orders
FROM orders
"""
cur = query_df(currency_sql).iloc[0]

latest_dt  = pd.to_datetime(cur['latest_order_date'])
latest_mon = str(cur['latest_month'])
mar26_n    = int(cur['mar26_orders'] or 0)
feb26_n    = int(cur['feb26_orders'] or 0)
jan26_n    = int(cur['jan26_orders'] or 0)
dec25_n    = int(cur['dec25_orders'] or 0)
mar25_n    = int(cur['mar25_orders'] or 0)

print(f"  Latest order date in DB : {BOLD}{latest_dt.strftime('%Y-%m-%d %H:%M')}{RESET}")
print(f"  Latest month in DB      : {BOLD}{latest_mon}{RESET}")
print(f"  Total orders in DB      : {int(cur['total_orders_in_db']):,}")
print()
print(f"  Order counts by period  :")
print(f"    March  2025 (YoY base) : {mar25_n:>8,}")
print(f"    Dec    2025            : {dec25_n:>8,}")
print(f"    Jan    2026            : {jan26_n:>8,}")
print(f"    Feb    2026            : {feb26_n:>8,}")
print(f"    March  2026 (current)  : {mar26_n:>8,}")

if latest_mon == "2026-03":
    print(f"\n  {GREEN}{BOLD}✅ CURRENT — DB contains March 2026 data. Dashboard is up to date.{RESET}")
    DATA_CURRENT = True
elif latest_mon >= "2026-02":
    print(f"\n  {YELLOW}{BOLD}⚠️  NEAR-CURRENT — Latest data is {latest_mon}. March 2026 data not yet in DB.{RESET}")
    DATA_CURRENT = False
else:
    print(f"\n  {RED}{BOLD}❌ STALE — Latest data is {latest_mon}. Dashboard is NOT showing current month.{RESET}")
    DATA_CURRENT = False

days_into_march = (now - datetime(2026, 3, 1)).days
pct_month_elapsed = days_into_march / 31 * 100
print(f"\n  March progress          : Day {days_into_march}/31 ({pct_month_elapsed:.0f}% of month elapsed)")
if mar26_n > 0:
    run_rate_march = mar26_n / max(days_into_march, 1) * 31
    print(f"  March run-rate (orders) : ~{run_rate_march:,.0f} orders projected for full month")


# ════════════════════════════════════════════════════════════════════════════
# [B] MONTHLY GMV TREND
# ════════════════════════════════════════════════════════════════════════════
hdr("[B] MONTHLY GMV (Gross Merchandise Value) TREND  — AED")

gmv_sql = """
SELECT
    DATE_FORMAT(created_at, '%Y-%m')  AS month,
    COUNT(DISTINCT id)                AS orders,
    COUNT(DISTINCT user_id)           AS active_buyers,
    SUM(total)                        AS gmv,
    AVG(total)                        AS aov,
    SUM(discount_total)               AS total_discounts,
    SUM(service_fee)                  AS total_service_fees
FROM orders
WHERE status = 3
  AND payment_status = 'completed'
  AND created_at >= '2025-03-01'
  AND created_at  < '2026-04-01'
GROUP BY DATE_FORMAT(created_at, '%Y-%m')
ORDER BY month
"""
df_monthly = query_df(gmv_sql)
df_monthly = df_monthly[df_monthly['month'].isin(
    ['2025-03', '2025-12', '2026-01', '2026-02', '2026-03']
)]
df_monthly['gmv']              = df_monthly['gmv'].astype(float)
df_monthly['aov']              = df_monthly['aov'].astype(float)
df_monthly['total_discounts']  = df_monthly['total_discounts'].astype(float)
df_monthly['orders']           = df_monthly['orders'].astype(int)
df_monthly['active_buyers']    = df_monthly['active_buyers'].astype(int)

def get_month(m):
    r = df_monthly[df_monthly['month'] == m]
    return r.iloc[0] if len(r) else None

mar25 = get_month('2025-03')
dec25 = get_month('2025-12')
jan26 = get_month('2026-01')
feb26 = get_month('2026-02')
mar26 = get_month('2026-03')

def pct_change(new, old, col):
    if old is None or new is None: return None
    o = float(old[col] or 0); n = float(new[col] or 0)
    if o == 0: return None
    return (n - o) / o * 100

print(f"\n  {'Period':<14} {'GMV (AED)':>14} {'Orders':>8} {'Active Buyers':>14} {'AOV (AED)':>10}")
print(f"  {'─'*14} {'─'*14} {'─'*8} {'─'*14} {'─'*10}")

labels = [('2025-03', 'Mar 2025'), ('2025-12', 'Dec 2025'),
          ('2026-01', 'Jan 2026'), ('2026-02', 'Feb 2026'), ('2026-03', 'Mar 2026')]
for mon_code, label in labels:
    r = get_month(mon_code)
    if r is None:
        print(f"  {label:<14} {'no data':>14}")
        continue
    flag = f"  {YELLOW}◄ CURRENT MONTH{RESET}" if mon_code == '2026-03' else ""
    print(f"  {label:<14} {r['gmv']:>14,.0f} {r['orders']:>8,} {r['active_buyers']:>14,} {r['aov']:>10.0f}{flag}")

# MoM and YoY changes
print(f"\n  {BOLD}Month-over-Month changes (vs Feb 2026):{RESET}")
for col, label in [('gmv','GMV'), ('orders','Orders'), ('active_buyers','Active Buyers'), ('aov','AOV')]:
    chg = pct_change(mar26, feb26, col)
    print(f"    {label:<16}: {arrow(chg)}")

print(f"\n  {BOLD}Year-over-Year (March 2026 vs March 2025):{RESET}")
for col, label in [('gmv','GMV'), ('orders','Orders'), ('active_buyers','Active Buyers'), ('aov','AOV')]:
    chg = pct_change(mar26, mar25, col)
    print(f"    {label:<16}: {arrow(chg)}")

# 3-month average (Dec, Jan, Feb) as baseline
months_3 = [m for m in [dec25, jan26, feb26] if m is not None]
if months_3 and mar26 is not None:
    avg_gmv_3m = np.mean([float(m['gmv']) for m in months_3])
    avg_ord_3m = np.mean([float(m['orders']) for m in months_3])
    avg_buy_3m = np.mean([float(m['active_buyers']) for m in months_3])
    avg_aov_3m = np.mean([float(m['aov']) for m in months_3])

    chg_gmv  = (float(mar26['gmv']) - avg_gmv_3m) / avg_gmv_3m * 100 if avg_gmv_3m else None
    chg_ord  = (float(mar26['orders']) - avg_ord_3m) / avg_ord_3m * 100 if avg_ord_3m else None
    chg_buy  = (float(mar26['active_buyers']) - avg_buy_3m) / avg_buy_3m * 100 if avg_buy_3m else None
    chg_aov  = (float(mar26['aov']) - avg_aov_3m) / avg_aov_3m * 100 if avg_aov_3m else None

    print(f"\n  {BOLD}vs 3-Month Average (Dec 25 – Feb 26):{RESET}")
    print(f"    GMV             : {arrow(chg_gmv)}  (3m avg: AED {avg_gmv_3m:,.0f})")
    print(f"    Orders          : {arrow(chg_ord)}  (3m avg: {avg_ord_3m:,.0f})")
    print(f"    Active Buyers   : {arrow(chg_buy)}  (3m avg: {avg_buy_3m:,.0f})")
    print(f"    AOV             : {arrow(chg_aov)}  (3m avg: AED {avg_aov_3m:.0f})")


# ════════════════════════════════════════════════════════════════════════════
# [C] DISCOUNT & SERVICE FEE IMPACT
# ════════════════════════════════════════════════════════════════════════════
hdr("[C] DISCOUNT & SERVICE FEE IMPACT")

if mar26 is not None:
    disc_rate = float(mar26['total_discounts'] or 0) / float(mar26['gmv'] or 1) * 100
    fee_rate  = float(mar26['total_service_fees'] or 0) / float(mar26['gmv'] or 1) * 100
    print(f"  March 2026  — Discount rate     : {disc_rate:.1f}% of GMV  (AED {float(mar26['total_discounts'] or 0):,.0f})")
    print(f"  March 2026  — Service fee rate  : {fee_rate:.1f}% of GMV  (AED {float(mar26['total_service_fees'] or 0):,.0f})")

if feb26 is not None:
    disc_feb = float(feb26['total_discounts'] or 0) / float(feb26['gmv'] or 1) * 100
    print(f"  Feb   2026  — Discount rate     : {disc_feb:.1f}% of GMV  (AED {float(feb26['total_discounts'] or 0):,.0f})")
    if mar26 is not None:
        chg_disc = disc_rate - disc_feb
        col = RED if chg_disc > 0 else GREEN
        print(f"  Discount rate change MoM        : {col}{'+' if chg_disc > 0 else ''}{chg_disc:.1f}pp{RESET}")


# ════════════════════════════════════════════════════════════════════════════
# [D] NEW USER ACQUISITION
# ════════════════════════════════════════════════════════════════════════════
hdr("[D] NEW USER ACQUISITION — First-time buyers per month")

new_user_sql = """
SELECT
    DATE_FORMAT(first_order, '%Y-%m') AS month,
    COUNT(*) AS new_buyers
FROM (
    SELECT user_id, MIN(created_at) AS first_order
    FROM orders
    WHERE status = 3 AND payment_status = 'completed'
    GROUP BY user_id
) sub
WHERE first_order >= '2025-03-01' AND first_order < '2026-04-01'
GROUP BY DATE_FORMAT(first_order, '%Y-%m')
ORDER BY month
"""
df_new = query_df(new_user_sql)
df_new = df_new[df_new['month'].isin(['2025-03','2025-12','2026-01','2026-02','2026-03'])]

print(f"\n  {'Period':<14} {'New Buyers':>12}")
print(f"  {'─'*14} {'─'*12}")
for mon_code, label in labels:
    r = df_new[df_new['month'] == mon_code]
    if r.empty:
        print(f"  {label:<14} {'no data':>12}")
    else:
        n = int(r.iloc[0]['new_buyers'])
        flag = f"  {YELLOW}◄{RESET}" if mon_code == '2026-03' else ""
        print(f"  {label:<14} {n:>12,}{flag}")

# New buyer rate for March 2026
if mar26 is not None and not df_new[df_new['month']=='2026-03'].empty:
    new_mar26 = int(df_new[df_new['month']=='2026-03'].iloc[0]['new_buyers'])
    new_rate  = new_mar26 / float(mar26['active_buyers']) * 100
    print(f"\n  New buyer rate (Mar 2026): {new_mar26:,} new / {int(mar26['active_buyers']):,} active = {new_rate:.1f}%")


# ════════════════════════════════════════════════════════════════════════════
# [E] REVENUE BY CHANNEL
# ════════════════════════════════════════════════════════════════════════════
hdr("[E] REVENUE BY CHANNEL — Own Brand vs Marketplace vs Direct")

channel_sql = """
SELECT
    DATE_FORMAT(o.created_at, '%Y-%m') AS month,
    CASE
        WHEN o.shop_id IS NULL THEN 'Direct (no shop)'
        WHEN s.is_zabehaty = 1 THEN 'Own Brand'
        WHEN s.zabehaty_percentage = 100 THEN 'Charity'
        ELSE 'Marketplace'
    END AS channel,
    COUNT(DISTINCT o.id)  AS orders,
    SUM(o.total)          AS revenue
FROM orders o
LEFT JOIN shops s ON o.shop_id = s.id
WHERE o.status = 3
  AND o.payment_status = 'completed'
  AND o.created_at >= '2025-03-01'
  AND o.created_at  < '2026-04-01'
GROUP BY DATE_FORMAT(o.created_at, '%Y-%m'), channel
ORDER BY month, revenue DESC
"""
df_ch = query_df(channel_sql)

for mon_code, label in [('2026-03', 'March 2026'), ('2026-02', 'Feb 2026'), ('2025-03', 'March 2025 (YoY)')]:
    sub = df_ch[df_ch['month'] == mon_code]
    if sub.empty: continue
    total_rev = float(sub['revenue'].sum())
    flag = f"  {YELLOW}◄ CURRENT{RESET}" if mon_code == '2026-03' else ""
    print(f"\n  {BOLD}{label}{RESET}{flag}  (Total GMV: AED {total_rev:,.0f})")
    for _, row in sub.iterrows():
        share = float(row['revenue']) / total_rev * 100
        print(f"    {row['channel']:<22} AED {float(row['revenue']):>12,.0f}  ({share:.1f}%)  {int(row['orders']):,} orders")


# ════════════════════════════════════════════════════════════════════════════
# [F] TOP CATEGORIES THIS MONTH vs 3-MONTH AVERAGE
# ════════════════════════════════════════════════════════════════════════════
hdr("[F] TOP CATEGORIES — March 2026 vs 3-Month Average")

cat_sql = """
SELECT
    DATE_FORMAT(o.created_at, '%Y-%m') AS month,
    IFNULL(c.name_en, 'Unknown')       AS category,
    SUM(o.total)                       AS revenue,
    COUNT(DISTINCT o.id)               AS orders
FROM orders o
LEFT JOIN categories c ON o.category_id = c.id
WHERE o.status = 3
  AND o.payment_status = 'completed'
  AND o.created_at >= '2025-12-01'
  AND o.created_at  < '2026-04-01'
GROUP BY DATE_FORMAT(o.created_at, '%Y-%m'), c.name_en
"""
df_cat = query_df(cat_sql)

mar26_cat = df_cat[df_cat['month'] == '2026-03'].copy()
prev3_cat = df_cat[df_cat['month'].isin(['2025-12','2026-01','2026-02'])].groupby('category').agg(
    avg_rev=('revenue', 'mean'),
    avg_ord=('orders', 'mean')
).reset_index()

if not mar26_cat.empty:
    mar26_cat = mar26_cat.merge(prev3_cat, on='category', how='left')
    mar26_cat['gmv_chg_pct'] = ((mar26_cat['revenue'].astype(float) - mar26_cat['avg_rev'].astype(float))
                                 / mar26_cat['avg_rev'].astype(float) * 100)
    mar26_cat = mar26_cat.sort_values('revenue', ascending=False)
    total_cat_rev = float(mar26_cat['revenue'].sum())

    print(f"\n  {'Category':<28} {'Mar 26 Rev':>12} {'Share':>6} {'vs 3m avg':>12}")
    print(f"  {'─'*28} {'─'*12} {'─'*6} {'─'*12}")
    for _, row in mar26_cat.head(12).iterrows():
        share = float(row['revenue']) / total_cat_rev * 100
        chg   = row['gmv_chg_pct']
        chg_str = arrow(chg) if not np.isnan(chg) else "  new"
        print(f"  {str(row['category']):<28} {float(row['revenue']):>12,.0f}  {share:>5.1f}%  {chg_str}")


# ════════════════════════════════════════════════════════════════════════════
# [G] CHURN SIGNAL — Users active Jan/Feb but silent in March
# ════════════════════════════════════════════════════════════════════════════
hdr("[G] CHURN SIGNAL — Active last 2 months but not yet in March")

churn_sql = """
SELECT
    COUNT(DISTINCT jan_feb.user_id)              AS active_jan_feb,
    COUNT(DISTINCT mar.user_id)                  AS already_ordered_march,
    COUNT(DISTINCT jan_feb.user_id)
      - COUNT(DISTINCT mar.user_id)              AS silent_in_march
FROM (
    SELECT DISTINCT user_id
    FROM orders
    WHERE status = 3 AND payment_status = 'completed'
      AND created_at >= '2026-01-01' AND created_at < '2026-03-01'
) jan_feb
LEFT JOIN (
    SELECT DISTINCT user_id
    FROM orders
    WHERE status = 3 AND payment_status = 'completed'
      AND created_at >= '2026-03-01' AND created_at < '2026-04-01'
) mar ON jan_feb.user_id = mar.user_id
"""
churn_data = query_df(churn_sql).iloc[0]
active_jf    = int(churn_data['active_jan_feb'])
ordered_mar  = int(churn_data['already_ordered_march'])
silent_mar   = int(churn_data['silent_in_march'])
return_rate  = ordered_mar / active_jf * 100 if active_jf > 0 else 0
silent_pct   = silent_mar / active_jf * 100 if active_jf > 0 else 0

print(f"\n  Users who ordered in Jan or Feb 2026   : {active_jf:,}")
print(f"  Of those, already ordered in March 2026: {ordered_mar:,}  ({return_rate:.1f}% return rate)")
print(f"  Silent in March so far                  : {silent_mar:,}  ({silent_pct:.1f}% not yet back)")
print()
if days_into_march < 20:
    adj_return = return_rate / pct_month_elapsed * 100
    print(f"  {YELLOW}Note: Only {days_into_march} days into March. Adjusted projected return rate: ~{adj_return:.0f}%{RESET}")
if silent_pct > 60:
    print(f"  {RED}⚠️  High silent rate — consider a March re-engagement campaign.{RESET}")
elif silent_pct > 40:
    print(f"  {YELLOW}⚠️  Moderate silent rate — monitor closely for churn acceleration.{RESET}")
else:
    print(f"  {GREEN}✅ Return rate on track given month progress.{RESET}")


# ════════════════════════════════════════════════════════════════════════════
# [H] PAYMENT METHOD MIX — Cash vs Digital Trend
# ════════════════════════════════════════════════════════════════════════════
hdr("[H] PAYMENT METHOD MIX — Cash vs Digital")

pay_sql = """
SELECT
    DATE_FORMAT(created_at, '%Y-%m') AS month,
    CASE payment_method
        WHEN 1  THEN 'Cash on Delivery'
        WHEN 2  THEN 'Card (Online)'
        WHEN 4  THEN 'Wallet'
        WHEN 5  THEN 'Tamara (BNPL)'
        WHEN 6  THEN 'Tabby (BNPL)'
        WHEN 7  THEN 'Apple Pay'
        ELSE         'Other'
    END AS method,
    COUNT(*)   AS orders,
    SUM(total) AS revenue
FROM orders
WHERE status = 3 AND payment_status = 'completed'
  AND created_at >= '2025-12-01' AND created_at < '2026-04-01'
GROUP BY DATE_FORMAT(created_at, '%Y-%m'), payment_method
ORDER BY month, orders DESC
"""
df_pay = query_df(pay_sql)

for mon_code, label in [('2026-03', 'March 2026'), ('2026-02', 'Feb 2026')]:
    sub = df_pay[df_pay['month'] == mon_code]
    if sub.empty: continue
    total_ord = int(sub['orders'].sum())
    print(f"\n  {BOLD}{label}{RESET}  (Total: {total_ord:,} orders)")
    for _, row in sub.sort_values('orders', ascending=False).head(6).iterrows():
        share = int(row['orders']) / total_ord * 100
        print(f"    {str(row['method']):<22} {int(row['orders']):>7,} orders  {share:>5.1f}%")


# ════════════════════════════════════════════════════════════════════════════
# [I] EXECUTIVE SCORECARD
# ════════════════════════════════════════════════════════════════════════════
hdr("[I] EXECUTIVE SCORECARD — March 2026")

print()
print(f"  {BOLD}Metric                   Current (Mar 26)   vs Feb 26     vs Mar 25 (YoY){RESET}")
print(f"  {'─'*75}")

metrics = [
    ('GMV (AED)',        'gmv',           False),
    ('Orders',           'orders',        False),
    ('Active Buyers',    'active_buyers', False),
    ('AOV (AED)',        'aov',           False),
]

def fmt_val(row, col):
    if row is None: return "no data"
    v = float(row[col] or 0)
    if col == 'gmv':    return f"AED {v:>10,.0f}"
    if col == 'aov':    return f"AED {v:>10,.0f}"
    return f"{int(v):>10,}"

for label, col, rev in metrics:
    curr = fmt_val(mar26, col)
    chg_mom = pct_change(mar26, feb26, col)
    chg_yoy = pct_change(mar26, mar25, col)
    mom_str = arrow(chg_mom, rev) if chg_mom is not None else "  n/a"
    yoy_str = arrow(chg_yoy, rev) if chg_yoy is not None else "  n/a"
    print(f"  {label:<24} {curr}   {mom_str:<20} {yoy_str}")

print()
currency_status = f"{GREEN}✅ CURRENT (Mar 2026){RESET}" if DATA_CURRENT else f"{RED}❌ STALE — latest: {latest_mon}{RESET}"
print(f"  Data Currency            : {currency_status}")

# overall health signal
signals = []
if mar26 is not None and feb26 is not None:
    if pct_change(mar26, feb26, 'gmv') is not None:
        signals.append(pct_change(mar26, feb26, 'gmv'))
positive = sum(1 for s in signals if s and s > 0)
if not signals:
    health = f"{YELLOW}⚠️  Insufficient data for health signal{RESET}"
elif positive == len(signals):
    health = f"{GREEN}✅ Business trending UP month-over-month{RESET}"
elif positive == 0:
    health = f"{RED}❌ Business trending DOWN month-over-month{RESET}"
else:
    health = f"{YELLOW}⚠️  Mixed signals — some metrics up, some down{RESET}"

print(f"  Overall Health Signal    : {health}")
print()
print(f"{BOLD}{'═'*62}{RESET}")
print(f"{BOLD}  END OF BOARD VALIDATION REPORT{RESET}")
print(f"{BOLD}{'═'*62}{RESET}")
print()

if __name__ == "__main__":
    pass  # All logic runs at import/script level
