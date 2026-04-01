"""
validate_data.py
QA script — applies the data-validation.md skill checklist to all Zabehaty .tmp/ CSVs
and cross-validates against the live DB.

This script is READ-ONLY: it never writes to the DB or modifies any file.

Checks implemented (15 total):
  1  Source verification          — DB user count vs CSV row count
  2  Join cardinality             — detect join explosion in user_total_orders
  3  NULL rates                   — key columns: monetary, p_active, sentiment_score
  4  Filter verification          — confirm status=3 and payment_status='completed' semantics
  5  Revenue cross-check (orders) — direct DB orders total vs shop_rankings.json
  6  Revenue cross-check (UTO)    — user_total_orders lifetime total vs rfm monetary sum
  7  Segment coherence            — Champions / Lost labels match recency values
  8  LTV tier activity            — Platinum users who haven't ordered in 12+ months
  9  Segment counts sum           — all users accounted for
  10 BCG growth rate quality      — detect fake 100% and extreme growth
  11 Churn score bounds           — all scores in [0, 1]
  12 Revenue share sum            — shop revenue_share_pct sums to ~100%
  13 Magnitude: no negatives      — no negative revenue in BCG or shops
  14 Date sanity                  — no future dates, no pre-2018 records
  15 Cross-file user consistency  — user_id gaps between rfm, churn, ltv CSVs

Run:
  python tools/validate_data.py
"""

import os, sys, json
import pandas as pd
import numpy as np
from datetime import datetime
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

sys.path.insert(0, os.path.dirname(__file__))
from db_connect import query_df

load_dotenv()
TMP = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".tmp")

# ── Colour codes for terminal output ─────────────────────────────────────────
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
BLUE   = "\033[94m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

PASS = f"{GREEN}✅ PASS{RESET}"
WARN = f"{YELLOW}⚠️  WARN{RESET}"
FAIL = f"{RED}❌ FAIL{RESET}"
INFO = f"{BLUE}ℹ️  INFO{RESET}"

results = []   # list of (status, check_id, message)

def record(status, check_id, msg, detail=""):
    tag = {"PASS": PASS, "WARN": WARN, "FAIL": FAIL, "INFO": INFO}[status]
    line = f"  {tag}  CHECK {check_id:>2}  {msg}"
    if detail:
        line += f"\n             {detail}"
    print(line)
    results.append((status, check_id, msg))


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 0 — Load CSVs
# ══════════════════════════════════════════════════════════════════════════════

def load(name):
    path = os.path.join(TMP, name)
    if not os.path.exists(path):
        print(f"  {RED}MISSING{RESET}  {name} not found in .tmp/")
        return pd.DataFrame()
    return pd.read_csv(path, low_memory=False)

print()
print(f"{BOLD}╔══════════════════════════════════════════════════════════╗{RESET}")
print(f"{BOLD}║  ZABEHATY DATA VALIDATION REPORT — {datetime.now().strftime('%Y-%m-%d %H:%M')}      ║{RESET}")
print(f"{BOLD}╚══════════════════════════════════════════════════════════╝{RESET}")
print()
print("Loading .tmp/ files...")
df_rfm   = load("rfm_scores.csv")
df_ltv   = load("ltv_analysis.csv")
df_churn = load("churn_risk.csv")
df_bcg   = load("bcg_matrix.csv")
df_shops = load("shop_performance.csv")

shop_r = {}
rankings_path = os.path.join(TMP, "shop_rankings.json")
if os.path.exists(rankings_path):
    with open(rankings_path, encoding="utf-8") as f:
        shop_r = json.load(f)

print(f"  rfm_scores:      {len(df_rfm):,} rows")
print(f"  ltv_analysis:    {len(df_ltv):,} rows")
print(f"  churn_risk:      {len(df_churn):,} rows")
print(f"  bcg_matrix:      {len(df_bcg):,} rows")
print(f"  shop_performance:{len(df_shops):,} rows")
print()


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — SOURCE VERIFICATION
# ══════════════════════════════════════════════════════════════════════════════
print(f"{BOLD}[SOURCE VERIFICATION]{RESET}")

try:
    db_users = query_df("SELECT COUNT(DISTINCT user_id) AS n FROM user_total_orders")
    db_n = int(db_users.iloc[0]["n"])
    csv_n = len(df_rfm)
    diff = abs(db_n - csv_n)
    pct_diff = diff / max(db_n, 1) * 100
    if pct_diff <= 5:
        record("PASS", 1, f"DB user count matches CSV: {db_n:,} vs {csv_n:,} (diff: {diff})")
    else:
        record("FAIL", 1, f"DB user count diverges: {db_n:,} vs {csv_n:,} ({pct_diff:.1f}% difference)",
               "Re-run user_analysis.py to regenerate rfm_scores.csv")
except Exception as e:
    record("FAIL", 1, f"DB query failed: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — DEDUPLICATION & JOIN CARDINALITY
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n{BOLD}[DEDUPLICATION & JOIN CARDINALITY]{RESET}")

try:
    # Check user_total_orders row count per user (should = num categories they bought)
    uto_check = query_df("""
        SELECT user_id, COUNT(*) AS row_count
        FROM user_total_orders
        GROUP BY user_id
        ORDER BY row_count DESC
        LIMIT 1
    """)
    max_rows = int(uto_check.iloc[0]["row_count"]) if not uto_check.empty else 0
    # More than 50 rows per user would suggest explosion (platform has ~13 categories)
    if max_rows <= 50:
        record("PASS", 2, f"No join explosion in user_total_orders — max rows per user: {max_rows} (≤ 50 categories OK)")
    else:
        record("WARN", 2, f"Possible join explosion: one user has {max_rows} rows in user_total_orders",
               "Investigate: SELECT user_id, COUNT(*) FROM user_total_orders GROUP BY user_id ORDER BY COUNT(*) DESC LIMIT 5")
except Exception as e:
    record("FAIL", 2, f"DB query failed: {e}")

# RFM CSV deduplication
if not df_rfm.empty and "user_id" in df_rfm.columns:
    dup_count = df_rfm["user_id"].duplicated().sum()
    if dup_count == 0:
        record("PASS", "2b", "No duplicate user_id in rfm_scores.csv")
    else:
        record("FAIL", "2b", f"{dup_count:,} duplicate user_ids in rfm_scores.csv — aggregation bug")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — NULL RATES
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n{BOLD}[NULL RATES]{RESET}")

if not df_rfm.empty:
    monetary_pos = (df_rfm["monetary"] > 0).mean() * 100 if "monetary" in df_rfm.columns else 0
    if monetary_pos >= 99:
        record("PASS", "3a", f"RFM monetary > 0: {monetary_pos:.1f}%")
    else:
        record("WARN", "3a", f"RFM monetary > 0: {monetary_pos:.1f}% — {100-monetary_pos:.1f}% of users have zero/negative lifetime spend")

    recency_valid = df_rfm["recency_days"].notna().mean() * 100 if "recency_days" in df_rfm.columns else 0
    if recency_valid >= 99:
        record("PASS", "3b", f"rfm recency_days not-null: {recency_valid:.1f}%")
    else:
        record("WARN", "3b", f"rfm recency_days not-null: {recency_valid:.1f}% — missing recency means RFM scores unreliable for those users")

if not df_ltv.empty and "p_active" in df_ltv.columns:
    p_valid = df_ltv["p_active"].between(0, 1).mean() * 100
    if p_valid >= 99:
        record("PASS", "3c", f"LTV p_active in [0, 1]: {p_valid:.1f}%")
    else:
        record("FAIL", "3c", f"LTV p_active out of bounds: {100-p_valid:.1f}% of rows — churn probability calculation error")

if not df_shops.empty and "sentiment_score" in df_shops.columns:
    sentiment_valid = df_shops["sentiment_score"].notna().mean() * 100
    if sentiment_valid == 0:
        record("WARN", "3d", f"sentiment_score: 0% complete — review_classification column is empty in DB",
               "Safe to hide sentiment from dashboard until DB data is populated")
    elif sentiment_valid < 50:
        record("WARN", "3d", f"sentiment_score: only {sentiment_valid:.1f}% complete")
    else:
        record("PASS", "3d", f"sentiment_score: {sentiment_valid:.1f}% complete")

    cancel_valid = df_shops["cancel_rate_pct"].notna().mean() * 100 if "cancel_rate_pct" in df_shops.columns else 0
    if cancel_valid >= 90:
        record("PASS", "3e", f"shop cancel_rate_pct not-null: {cancel_valid:.1f}%")
    else:
        record("WARN", "3e", f"shop cancel_rate_pct not-null: {cancel_valid:.1f}% — health scores for shops missing cancel data use 50% default (pessimistic)")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — FILTER VERIFICATION
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n{BOLD}[FILTER VERIFICATION]{RESET}")

try:
    status_dist = query_df("SELECT status, COUNT(*) AS n FROM orders GROUP BY status ORDER BY status")
    # status column may be float64 due to NaN rows; normalise to int-string keys
    status_map = {
        str(int(k)) if k == k else "null": int(v)
        for k, v in zip(status_dist["status"], status_dist["n"])
    }
    s3 = int(status_map.get("3", 0))
    total_orders = status_dist["n"].sum()
    if s3 > 0:
        record("PASS", 4, f"status=3 exists and has {s3:,} orders ({s3/total_orders*100:.1f}% of all orders) — used as 'Delivered' filter")
        # Print status distribution for reference
        status_lines = ", ".join(f"status={k}:{v:,}" for k, v in status_map.items())
        print(f"             Status distribution: {status_lines}")
    else:
        record("FAIL", 4, "status=3 has 0 orders — check if delivered status code has changed")
except Exception as e:
    record("FAIL", 4, f"DB query failed: {e}")

try:
    pay_dist = query_df("SELECT payment_status, COUNT(*) AS n FROM orders GROUP BY payment_status")
    pay_map = dict(zip(pay_dist["payment_status"].astype(str), pay_dist["n"]))
    completed = int(pay_map.get("completed", 0))
    if completed > 0:
        record("PASS", "4b", f"payment_status='completed' exists: {completed:,} orders")
    else:
        record("FAIL", "4b", f"payment_status='completed' has 0 orders — filter may be wrong. Values found: {list(pay_map.keys())}")
except Exception as e:
    record("FAIL", "4b", f"DB query failed: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — REVENUE CROSS-CHECK (recent orders)
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n{BOLD}[REVENUE CROSS-CHECK]{RESET}")

try:
    # Match shop_analysis.py scope: only orders with a shop_id (excludes direct/Personal orders)
    rev_check = query_df("""
        SELECT SUM(total) AS db_total, COUNT(DISTINCT user_id) AS db_users
        FROM orders
        WHERE status = 3 AND payment_status = 'completed'
          AND shop_id IS NOT NULL
    """)
    db_total = float(rev_check.iloc[0]["db_total"] or 0)
    db_users = int(rev_check.iloc[0]["db_users"] or 0)
    json_total = float(shop_r.get("total_platform_revenue_aed", 0))

    if json_total > 0:
        diff_pct = abs(db_total - json_total) / json_total * 100
        if diff_pct <= 5:
            record("PASS", 5, f"orders table revenue matches shop_rankings.json: AED {db_total:,.0f} vs AED {json_total:,.0f} (diff: {diff_pct:.1f}%)")
        elif diff_pct <= 20:
            record("WARN", 5, f"Revenue gap: DB AED {db_total:,.0f} vs JSON AED {json_total:,.0f} ({diff_pct:.1f}%)",
                   "May be due to new orders since last tool run — re-run shop_analysis.py")
        else:
            record("FAIL", 5, f"Large revenue gap: DB AED {db_total:,.0f} vs JSON AED {json_total:,.0f} ({diff_pct:.1f}%)",
                   "shop_analysis.py SQL may have changed — investigate")
    else:
        record("INFO", 5, f"Direct DB orders revenue: AED {db_total:,.0f} from {db_users:,} users")
except Exception as e:
    record("FAIL", 5, f"DB query failed: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — USER_TOTAL_ORDERS vs RFM monetary
# ══════════════════════════════════════════════════════════════════════════════
try:
    uto_rev = query_df("SELECT SUM(total) AS uto_total FROM user_total_orders")
    uto_total = float(uto_rev.iloc[0]["uto_total"] or 0)
    rfm_total = float(df_rfm["monetary"].sum()) if not df_rfm.empty and "monetary" in df_rfm.columns else 0

    if rfm_total > 0:
        diff_pct = abs(uto_total - rfm_total) / rfm_total * 100
        if diff_pct <= 10:
            record("PASS", 6, f"user_total_orders lifetime revenue matches rfm_scores monetary: AED {uto_total:,.0f} vs AED {rfm_total:,.0f} (diff: {diff_pct:.1f}%)")
        else:
            record("WARN", 6, f"UTO total AED {uto_total:,.0f} vs rfm monetary AED {rfm_total:,.0f} ({diff_pct:.1f}% diff)",
                   "If UTO total >> rfm sum, some users were dropped by the GROUP BY or ban filter")
    record("INFO", "6b", f"user_total_orders lifetime (AED {uto_total:,.0f}) >> recent orders (AED {db_total:,.0f}) — expected; UTO is all-time cumulative")
except Exception as e:
    record("FAIL", 6, f"DB query failed: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 7 — SEGMENT COHERENCE
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n{BOLD}[SEGMENT COHERENCE]{RESET}")

if not df_rfm.empty and "Segment" in df_rfm.columns and "recency_days" in df_rfm.columns:
    champions = df_rfm[df_rfm["Segment"] == "Champions"]
    if len(champions) > 0:
        bad_champ = champions[champions["recency_days"] > 180]
        pct = len(bad_champ) / len(champions) * 100
        if pct <= 5:
            record("PASS", "7a", f"Champions with recency > 180 days: {len(bad_champ):,} ({pct:.1f}% of Champions — OK)")
        else:
            record("WARN", "7a", f"{len(bad_champ):,} Champions ({pct:.1f}%) have recency_days > 180",
                   "These are dormant users labelled as top customers — survivorship bias in RFM scoring")
    else:
        record("INFO", "7a", "No 'Champions' segment found in rfm_scores.csv")

    lost = df_rfm[df_rfm["Segment"] == "Lost"]
    if len(lost) > 0:
        fresh_lost = lost[lost["recency_days"] < 90]
        pct = len(fresh_lost) / len(lost) * 100
        if pct <= 2:
            record("PASS", "7b", f"Lost segment: {len(fresh_lost):,} users ({pct:.1f}%) with recency < 90 days — OK")
        else:
            record("WARN", "7b", f"{len(fresh_lost):,} 'Lost' users ({pct:.1f}%) have recency < 90 days",
                   "Active users labelled Lost — segment threshold may need adjustment")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 8 — LTV TIER ACTIVITY (survivorship bias check)
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n{BOLD}[LTV TIER ACTIVITY]{RESET}")

if not df_rfm.empty and "ltv_tier" in df_rfm.columns and "recency_days" in df_rfm.columns:
    platinum = df_rfm[df_rfm["ltv_tier"] == "Platinum"]
    if len(platinum) > 0:
        dormant = platinum[platinum["recency_days"] > 365]
        pct = len(dormant) / len(platinum) * 100
        if pct <= 20:
            record("PASS", 8, f"Platinum dormant (recency > 365 days): {len(dormant):,} ({pct:.1f}%)")
        else:
            record("WARN", 8, f"{pct:.1f}% of Platinum users ({len(dormant):,}) have not ordered in 12+ months",
                   "LTV tier is based on lifetime spend, not recent activity. Add dashboard caveat.")

        # Also show distribution of recency among Platinum
        p50 = platinum["recency_days"].median()
        p90 = platinum["recency_days"].quantile(0.9)
        print(f"             Platinum recency: median={p50:.0f} days, p90={p90:.0f} days")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 9 — SEGMENT COUNTS SUM TO TOTAL
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n{BOLD}[AGGREGATION]{RESET}")

if not df_rfm.empty and "Segment" in df_rfm.columns:
    seg_total = df_rfm["Segment"].value_counts().sum()
    csv_total = len(df_rfm)
    if seg_total == csv_total:
        record("PASS", 9, f"Segment counts sum to total: {seg_total:,} users ✓")
        # Print segment breakdown for reference
        seg_counts = df_rfm["Segment"].value_counts()
        for seg, cnt in seg_counts.items():
            print(f"             {seg}: {cnt:,} ({cnt/csv_total*100:.1f}%)")
    else:
        record("FAIL", 9, f"Segment counts ({seg_total:,}) ≠ CSV total ({csv_total:,}) — {csv_total - seg_total} users have no segment")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 10 — BCG GROWTH RATE QUALITY
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n{BOLD}[BCG GROWTH QUALITY]{RESET}")

if not df_bcg.empty and "growth_rate" in df_bcg.columns:
    total_products = len(df_bcg)
    fake_100 = (df_bcg["growth_rate"] == 100.0).sum()
    extreme  = (df_bcg["growth_rate"] > 500).sum()
    negative = (df_bcg["growth_rate"] < -100).sum()

    pct_fake = fake_100 / total_products * 100
    if pct_fake <= 30:
        record("PASS", "10a", f"Products with growth_rate = 100% (new/newly-seen): {fake_100} ({pct_fake:.1f}%)")
    else:
        record("WARN", "10a", f"{fake_100} products ({pct_fake:.1f}%) have growth_rate = 100%",
               "These are products with 0 prior-30-day revenue — not real growth. Only 3 weeks of orders history.")

    if extreme == 0:
        record("PASS", "10b", "No products with growth_rate > 500%")
    else:
        record("WARN", "10b", f"{extreme} products have growth_rate > 500% — likely data spikes, not real trends")

    if negative > 0:
        record("INFO", "10c", f"{negative} products with negative growth rate (declining revenue trend)")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 11 — CHURN SCORE BOUNDS
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n{BOLD}[SCORE BOUNDS]{RESET}")

if not df_churn.empty and "churn_risk_score" in df_churn.columns:
    oob = df_churn[
        (df_churn["churn_risk_score"] < 0) | (df_churn["churn_risk_score"] > 1)
    ]
    if len(oob) == 0:
        record("PASS", 11, "All churn risk scores in [0, 1]")
    else:
        record("FAIL", 11, f"{len(oob):,} churn scores out of [0, 1] bounds — scoring formula error")

# Revenue share sum
if not df_shops.empty and "revenue_share_pct" in df_shops.columns:
    rev_sum = df_shops["revenue_share_pct"].sum()
    if abs(rev_sum - 100) <= 1:
        record("PASS", 12, f"Shop revenue_share_pct sums to {rev_sum:.1f}% (≈ 100%)")
    else:
        record("WARN", 12, f"Shop revenue_share_pct sums to {rev_sum:.1f}% (expected ~100%)")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 12 — MAGNITUDE: NO NEGATIVE REVENUE
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n{BOLD}[MAGNITUDE & DATE SANITY]{RESET}")

if not df_bcg.empty and "total_revenue" in df_bcg.columns:
    neg_bcg = (df_bcg["total_revenue"] < 0).sum()
    if neg_bcg == 0:
        record("PASS", 13, "No negative revenue in bcg_matrix.csv")
    else:
        record("FAIL", 13, f"{neg_bcg} products have negative total_revenue")

if not df_shops.empty and "gross_revenue" in df_shops.columns:
    neg_shops = (df_shops["gross_revenue"] < 0).sum()
    if neg_shops == 0:
        record("PASS", "13b", "No negative revenue in shop_performance.csv")
    else:
        record("FAIL", "13b", f"{neg_shops} shops have negative gross_revenue")

# Date sanity
if not df_rfm.empty:
    now = pd.Timestamp.now()
    for col in ["last_order_date", "first_order_date"]:
        if col in df_rfm.columns:
            dates = pd.to_datetime(df_rfm[col], errors="coerce")
            future = (dates > now).sum()
            old = (dates < pd.Timestamp("2018-01-01")).sum()
            if future == 0:
                record("PASS", 14, f"No future dates in rfm_scores.{col}")
            else:
                record("FAIL", 14, f"{future:,} future dates in rfm_scores.{col} — data corruption")
            if old > 0:
                record("WARN", "14b", f"{old:,} records in rfm_scores.{col} before 2018-01-01",
                       "Platform launched ~2020 — pre-2018 dates may be test/seed data")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 13 — CROSS-FILE USER CONSISTENCY
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n{BOLD}[CROSS-FILE CONSISTENCY]{RESET}")

if not df_rfm.empty and not df_churn.empty and "user_id" in df_rfm.columns and "user_id" in df_churn.columns:
    rfm_ids   = set(df_rfm["user_id"])
    churn_ids = set(df_churn["user_id"])
    ltv_ids   = set(df_ltv["user_id"]) if not df_ltv.empty and "user_id" in df_ltv.columns else set()

    in_churn_not_rfm = len(churn_ids - rfm_ids)
    in_rfm_not_churn = len(rfm_ids - churn_ids)

    if in_churn_not_rfm <= 10:
        record("PASS", 15, f"User ID gap churn→rfm: {in_churn_not_rfm} users (within tolerance)")
    else:
        record("WARN", 15, f"{in_churn_not_rfm:,} users in churn_risk not in rfm_scores",
               "These users have churn scores but no RFM segment — ban filter mismatch")

    if in_rfm_not_churn <= 10:
        record("PASS", "15b", f"User ID gap rfm→churn: {in_rfm_not_churn} users (within tolerance)")
    else:
        record("WARN", "15b", f"{in_rfm_not_churn:,} users in rfm_scores not in churn_risk",
               "These users have RFM segments but no churn score")

    if ltv_ids:
        in_ltv_not_rfm = len(ltv_ids - rfm_ids)
        if in_ltv_not_rfm <= 5:
            record("PASS", "15c", f"User ID gap ltv→rfm: {in_ltv_not_rfm} users (within tolerance)")
        else:
            record("WARN", "15c", f"{in_ltv_not_rfm:,} users in ltv_analysis not in rfm_scores")


# ══════════════════════════════════════════════════════════════════════════════
# FINAL REPORT
# ══════════════════════════════════════════════════════════════════════════════
pass_count = sum(1 for r in results if r[0] == "PASS")
warn_count = sum(1 for r in results if r[0] == "WARN")
fail_count = sum(1 for r in results if r[0] == "FAIL")
info_count = sum(1 for r in results if r[0] == "INFO")

print()
print(f"{BOLD}══════════════════════════════════════════════════════════{RESET}")
pass_col = GREEN if fail_count == 0 else RESET
fail_col = RED   if fail_count > 0  else RESET
warn_col = YELLOW if warn_count > 0 else RESET
print(f"{BOLD}SUMMARY: {pass_col}{pass_count} PASS{RESET}{BOLD}  |  {warn_col}{warn_count} WARN{RESET}{BOLD}  |  {fail_col}{fail_count} FAIL{RESET}{BOLD}  |  {info_count} INFO{RESET}")
print(f"{BOLD}══════════════════════════════════════════════════════════{RESET}")

if warn_count > 0:
    print(f"\n{YELLOW}WARNINGS — review before sharing with stakeholders:{RESET}")
    for status, check_id, msg in results:
        if status == "WARN":
            print(f"  • CHECK {check_id}: {msg}")

if fail_count > 0:
    print(f"\n{RED}FAILURES — do not use dashboard data until resolved:{RESET}")
    for status, check_id, msg in results:
        if status == "FAIL":
            print(f"  • CHECK {check_id}: {msg}")

if fail_count == 0 and warn_count == 0:
    print(f"\n{GREEN}All checks passed. Data is consistent and ready for stakeholder review.{RESET}")

print()


if __name__ == "__main__":
    pass  # All logic runs at module level (script-style)
