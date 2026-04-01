"""
main.py
Zabehaty Analytics Agent — full pipeline orchestrator.

Runs all analysis tools in sequence, then pushes results to Google Sheets
and generates the board report.

Usage:
    python main.py              # full run
    python main.py --skip-llm  # skip Claude API calls (reuse cached narratives)
    python main.py --skip-sheets  # skip Google Sheets push
"""

import sys
import os
import time
import argparse
from datetime import datetime

# Force UTF-8 output on Windows
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

# ── Suppress pandas SQLAlchemy warning ───────────────────────────────────────
import warnings
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")

print("=" * 60)
print("  ZABEHATY ANALYTICS AGENT")
print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
print("=" * 60)

# Add tools to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "tools"))

parser = argparse.ArgumentParser()
parser.add_argument("--skip-llm",    action="store_true", help="Skip Claude API calls")
parser.add_argument("--skip-sheets", action="store_true", help="Skip Google Sheets push")
args = parser.parse_args()


def step(label, fn):
    print(f"\n[-] {label}...")
    t0 = time.time()
    result = fn()
    print(f"     Done in {time.time()-t0:.1f}s")
    return result


# ── STEP 1: User Analysis (RFM + LTV) ────────────────────────────────────────
from user_analysis import run as run_user
step("User Analysis: RFM scoring + LTV calculation", run_user)

# ── STEP 2: Product Analysis (BCG Matrix) ────────────────────────────────────
from product_analysis import run as run_products
step("Product Analysis: BCG matrix + recommendations", run_products)

# ── STEP 3: Shop/Vendor Analysis ─────────────────────────────────────────────
from shop_analysis import run as run_shops
step("Shop Analysis: vendor performance + health scores", run_shops)

# ── STEP 4: Buying Patterns ───────────────────────────────────────────────────
from buying_patterns import run as run_patterns
step("Buying Patterns: churn risk + cross-category affinity", run_patterns)

# ── STEP 5: LLM Interpretation ────────────────────────────────────────────────
if not args.skip_llm:
    from llm_interpreter import run as run_llm
    step("LLM Interpretation: Claude generating narratives", run_llm)
else:
    print("\n[--] Skipping LLM step (--skip-llm flag set)")

# ── STEP 6: Google Sheets Push ───────────────────────────────────────────────
sheets_url = None
if not args.skip_sheets:
    from google_sheets import push_to_sheets
    sheets_url = step("Google Sheets: pushing data", push_to_sheets)
else:
    print("\n[--] Skipping Sheets push (--skip-sheets flag set)")

# ── DONE ──────────────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("  PIPELINE COMPLETE")
print("=" * 60)
print(f"  Board report:  .tmp/board_summary.md")
print(f"  Raw data:      .tmp/")
if sheets_url:
    print(f"  Google Sheets: {sheets_url}")
print("=" * 60)
