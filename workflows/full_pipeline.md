# Workflow: Full Analytics Pipeline

## Objective
Run the complete Zabehaty analytics pipeline end-to-end and deliver results.

## Run Command
```bash
# Full run (all steps)
python main.py

# Skip Claude API calls (use cached narratives)
python main.py --skip-llm

# Skip Google Sheets push
python main.py --skip-sheets

# Start dashboard
streamlit run dashboard/app.py
```

## Pipeline Steps (in order)

| Step | Tool | Output |
|------|------|--------|
| 1 | `tools/user_analysis.py` | RFM scores, LTV, cohorts |
| 2 | `tools/product_analysis.py` | BCG matrix, recommendations |
| 3 | `tools/shop_analysis.py` | Vendor KPIs, health scores |
| 4 | `tools/buying_patterns.py` | Churn risk, affinity, timing |
| 5 | `tools/llm_interpreter.py` | Claude narratives + board report |
| 6 | `tools/google_sheets.py` | Push all data to Google Sheets |

## Typical Runtime
- Steps 1–4: ~30–60 seconds (MySQL queries)
- Step 5: ~60–90 seconds (4 Claude API calls)
- Step 6: ~20–30 seconds (Google Sheets write)

## Scheduling
- **Autonomous agent:** Run via cron or Task Scheduler weekly (Monday 6am)
- **n8n Cloud:** Import `n8n/workflow.json` → activate → schedule triggers automatically

## Environment Requirements
- Python 3.10+ with `.venv` activated
- All keys present in `.env`
- `token.json` generated (first Google run requires browser auth)

## If a Step Fails
1. Read the full error message
2. Check if it's a DB connection issue (VPN? IP whitelist?)
3. Check API key validity
4. Re-run the failing tool directly: `python tools/<tool_name>.py`
5. Update this workflow with any new constraints discovered
