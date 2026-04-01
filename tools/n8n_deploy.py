"""
n8n_deploy.py
Fully autonomous n8n Cloud workflow deployment.

Steps:
  1. Creates Anthropic API credential in n8n (if not exists)
  2. Builds workflow JSON with all credential IDs wired in
  3. Deletes existing workflow by name, imports fresh
  4. Activates the workflow

Architecture:
  - All SQL aggregation happens IN MySQL (tiny result sets flow through n8n)
  - Gate nodes between queries reset item count to 1 (prevents N*M fan-out)
  - Sequential chain: each query runs once, in order
  - Claude invoked via chainLlm + lmChatAnthropic (correct LangChain pattern)

Run: python tools/n8n_deploy.py
"""

import os, requests
from dotenv import load_dotenv

load_dotenv()

BASE            = os.getenv("N8N_INSTANCE_URL", "").rstrip("/")
API_KEY         = os.getenv("N8N_API_KEY", "")
HEADERS         = {"X-N8N-API-KEY": API_KEY, "Content-Type": "application/json"}
ANTHROPIC_KEY   = os.getenv("ANTHROPIC_API_KEY", "")
MYSQL_CRED_ID   = "GWuK2owH1JnWhwvx"
GSHEETS_CRED_ID = "yfa1XXINMZuNmI5x"
SHEETS_ID       = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip()


def step(label):
    print(f"\n[-] {label}...")


# ── 1. Credential ─────────────────────────────────────────────────────────────

def get_or_create_anthropic_cred():
    r = requests.get(f"{BASE}/api/v1/credentials", headers=HEADERS, timeout=15)
    for c in r.json().get("data", []):
        if c.get("type") == "anthropicApi":
            print(f"    Anthropic cred exists: {c['id']}")
            return c["id"]
    payload = {
        "name": "Anthropic — Zabehaty Analytics",
        "type": "anthropicApi",
        "data": {"apiKey": ANTHROPIC_KEY, "headerName": "x-api-key", "headerValue": ANTHROPIC_KEY}
    }
    r = requests.post(f"{BASE}/api/v1/credentials", headers=HEADERS, json=payload, timeout=15)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Credential create failed: {r.status_code} {r.text}")
    cid = r.json()["id"]
    print(f"    Created Anthropic cred: {cid}")
    return cid


# ── 2. SQL Queries (pre-aggregated — small result sets) ───────────────────────

# Returns ~7 rows: one per RFM segment, all scoring done in SQL
SQL_RFM = """
SELECT segment,
       COUNT(*)               AS user_count,
       ROUND(SUM(ltv), 2)     AS total_ltv_aed,
       ROUND(AVG(ltv), 2)     AS avg_ltv_aed
FROM (
  SELECT
    CASE
      WHEN r >= 4 AND f >= 4 AND m >= 4 THEN 'Champions'
      WHEN r >= 3 AND m >= 4             THEN 'Loyal Customers'
      WHEN r >= 4 AND f <= 2             THEN 'New Customers'
      WHEN r <= 2 AND f >= 3 AND m >= 3  THEN 'At Risk'
      WHEN r <= 2 AND f <= 2             THEN 'Lost'
      WHEN r  = 3 AND f <= 2             THEN 'Need Attention'
      ELSE 'About to Sleep'
    END AS segment,
    ltv
  FROM (
    SELECT
      user_id,
      SUM(total)  AS ltv,
      CASE WHEN DATEDIFF(NOW(), MAX(updated_at)) <  30 THEN 5
           WHEN DATEDIFF(NOW(), MAX(updated_at)) <  90 THEN 4
           WHEN DATEDIFF(NOW(), MAX(updated_at)) < 180 THEN 3
           WHEN DATEDIFF(NOW(), MAX(updated_at)) < 365 THEN 2
           ELSE 1 END                                  AS r,
      LEAST(5, GREATEST(1, COUNT(DISTINCT category_id))) AS f,
      CASE WHEN SUM(total) >= 5000 THEN 5
           WHEN SUM(total) >= 1000 THEN 4
           WHEN SUM(total) >=  300 THEN 3
           WHEN SUM(total) >=  100 THEN 2
           ELSE 1 END                                  AS m
    FROM user_total_orders
    GROUP BY user_id
  ) scored
) segmented
GROUP BY segment
ORDER BY total_ltv_aed DESC
""".strip()

# Returns 1 row: platform-level health numbers
SQL_TOTALS = """
SELECT
  COUNT(DISTINCT user_id)                                   AS total_users,
  ROUND(SUM(total), 2)                                      AS total_gmv_aed,
  ROUND(AVG(total), 2)                                      AS avg_order_value_aed,
  COUNT(DISTINCT category_id)                               AS active_categories,
  MIN(YEAR(updated_at))                                     AS data_from_year,
  MAX(YEAR(updated_at))                                     AS data_to_year,
  COUNT(DISTINCT CASE WHEN DATEDIFF(NOW(), updated_at) < 90
                      THEN user_id END)                     AS active_users_90d
FROM user_total_orders
""".strip()

# Returns ≤15 rows: top vendors by revenue
SQL_SHOPS = """
SELECT o.shop_id,
       s.name_en                        AS shop_name,
       COUNT(*)                         AS total_orders,
       ROUND(SUM(o.total), 2)           AS revenue_aed,
       ROUND(AVG(o.rating), 1)          AS avg_rating,
       s.zabehaty_percentage            AS commission_pct
FROM orders o
LEFT JOIN shops s ON o.shop_id = s.id
WHERE o.status = 3
  AND o.payment_status = 'completed'
  AND o.shop_id IS NOT NULL
GROUP BY o.shop_id, s.name_en, s.zabehaty_percentage
ORDER BY revenue_aed DESC
LIMIT 15
""".strip()

# Returns ≤20 rows: top products by revenue (aggregated)
SQL_PRODUCTS = """
SELECT od.product_id,
       p.name_en                                AS product_name,
       c.name_en                                AS category_name,
       COUNT(*)                                 AS order_count,
       ROUND(SUM(od.price * od.quantity), 2)    AS revenue_aed,
       ROUND(AVG(od.price - IFNULL(od.cost_price, 0)), 2) AS avg_margin_aed
FROM order_details od
JOIN orders o ON od.order_id = o.id AND o.status = 3
LEFT JOIN products  p ON od.product_id   = p.id
LEFT JOIN categories c ON o.category_id = c.id
GROUP BY od.product_id, p.name_en, c.name_en
ORDER BY revenue_aed DESC
LIMIT 20
""".strip()

# Returns ≤20 rows: top categories by revenue (named only, LIMIT prevents the 31K row blowup)
SQL_CATEGORIES = """
SELECT uto.category_id,
       c.name_en                          AS category_name,
       COUNT(DISTINCT uto.user_id)        AS buyers,
       ROUND(SUM(uto.total), 2)           AS revenue_aed
FROM user_total_orders uto
LEFT JOIN categories c ON uto.category_id = c.id
WHERE c.name_en IS NOT NULL
GROUP BY uto.category_id, c.name_en
ORDER BY revenue_aed DESC
LIMIT 20
""".strip()


# ── 3. Build workflow JSON ────────────────────────────────────────────────────

def mysql_node(node_id, name, query, x, y):
    return {
        "parameters": {"operation": "executeQuery", "query": query, "options": {}},
        "id": node_id, "name": name,
        "type": "n8n-nodes-base.mySql", "typeVersion": 2.4,
        "position": [x, y],
        "credentials": {"mySql": {"id": MYSQL_CRED_ID, "name": "MySQL account 3"}}
    }


def gate_node(node_id, name, x, y):
    """Resets item count to 1 so next MySQL node doesn't fan-out N times."""
    return {
        "parameters": {"jsCode": "return [{ json: { pass: true } }];"},
        "id": node_id, "name": name,
        "type": "n8n-nodes-base.code", "typeVersion": 2,
        "position": [x, y]
    }


def build_workflow(anthropic_cred_id):
    nodes = [
        # ── Triggers ──
        {
            "parameters": {"rule": {"interval": [{"field": "weeks", "weeksInterval": 1,
                                                   "triggerAtDay": [1], "triggerAtHour": 6}]}},
            "id": "schedule-trigger", "name": "Weekly Schedule (Mon 6am)",
            "type": "n8n-nodes-base.scheduleTrigger", "typeVersion": 1.1,
            "position": [0, 300]
        },
        {
            "parameters": {}, "id": "manual-trigger", "name": "Manual Trigger",
            "type": "n8n-nodes-base.manualTrigger", "typeVersion": 1,
            "position": [0, 460]
        },
        # ── Start ──
        {
            "parameters": {"jsCode": "return [{ json: { triggered_at: new Date().toISOString() } }];"},
            "id": "start", "name": "Start",
            "type": "n8n-nodes-base.code", "typeVersion": 2,
            "position": [220, 380]
        },
        # ── Sequential SQL queries (gates between each prevent fan-out) ──
        mysql_node("q-rfm",        "Query RFM Summary",      SQL_RFM,        440,  380),
        gate_node( "gate-rfm",     "Gate — After RFM",                        660,  380),
        mysql_node("q-totals",     "Query Platform Totals",  SQL_TOTALS,     880,  380),
        gate_node( "gate-totals",  "Gate — After Totals",                    1100,  380),
        mysql_node("q-shops",      "Query Shop Performance", SQL_SHOPS,      1320,  380),
        gate_node( "gate-shops",   "Gate — After Shops",                     1540,  380),
        mysql_node("q-products",   "Query Top Products",     SQL_PRODUCTS,   1760,  380),
        gate_node( "gate-products","Gate — After Products",                  1980,  380),
        mysql_node("q-categories", "Query Category Revenue", SQL_CATEGORIES, 2200,  380),
        # ── Build compact analysis JSON + prompt ──
        {
            "parameters": {
                "jsCode": r"""
const rfm       = $('Query RFM Summary').all().map(i => i.json);
const totals    = $('Query Platform Totals').first().json;
const shops     = $('Query Shop Performance').all().map(i => i.json);
const products  = $('Query Top Products').all().map(i => i.json);
const cats      = $('Query Category Revenue').all().map(i => i.json);

const totalRev  = shops.reduce((s, r) => s + parseFloat(r.revenue_aed || 0), 0);
const champions = rfm.find(s => s.segment === 'Champions') || {};
const atRisk    = rfm.find(s => s.segment === 'At Risk') || {};

const analysis = {
  generated_at:    new Date().toISOString(),
  platform_totals: totals,
  segment_summary: rfm,
  top_shops:       shops.slice(0, 5),
  top_products:    products.slice(0, 8),
  top_categories:  cats.slice(0, 8),
  shop_revenue_aed: totalRev.toFixed(2),
};

const prompt = `You are a senior analyst for Zabehaty, UAE Islamic halal meat marketplace.

PLATFORM DATA (${totals.data_from_year}–${totals.data_to_year}):
- Total users: ${totals.total_users?.toLocaleString()}
- Total GMV: AED ${parseFloat(totals.total_gmv_aed || 0).toLocaleString()}
- Avg order value: AED ${totals.avg_order_value_aed}
- Active last 90 days: ${totals.active_users_90d?.toLocaleString()}

RFM SEGMENTS:
${rfm.map(s => `  ${s.segment}: ${s.user_count} users, AED ${parseFloat(s.total_ltv_aed||0).toLocaleString()} LTV`).join('\n')}

TOP SHOPS (by revenue):
${shops.slice(0,5).map(s => `  ${s.shop_name}: AED ${parseFloat(s.revenue_aed||0).toLocaleString()}, ${s.total_orders} orders, rating ${s.avg_rating}`).join('\n')}

TOP CATEGORIES:
${cats.slice(0,5).map(c => `  ${c.category_name}: AED ${parseFloat(c.revenue_aed||0).toLocaleString()}, ${c.buyers} buyers`).join('\n')}

Write a board report in markdown:
## Business Health Snapshot (3–4 bullets, specific AED numbers)
## Growth Opportunities (top 3, with AED impact)
## Urgent Actions (top 3 for this week)
## Red Flags (risks to flag)

Be direct, evidence-based, AED figures throughout. No filler.`;

return [{ json: { analysis, prompt } }];
"""
            },
            "id": "build-package", "name": "Build Analysis Package",
            "type": "n8n-nodes-base.code", "typeVersion": 2,
            "position": [2420, 380]
        },
        # ── Claude via Basic LLM Chain (correct LangChain pattern) ──
        {
            "parameters": {"promptType": "define", "text": "={{ $json.prompt }}"},
            "id": "claude-chain", "name": "Claude — Board Report",
            "type": "@n8n/n8n-nodes-langchain.chainLlm", "typeVersion": 1.4,
            "position": [2640, 380]
        },
        {
            "parameters": {
                # typeVersion 1.3 requires resource-locator format for model.
                # mode "id" allows any model ID not in n8n's hardcoded dropdown.
                "model": {"__rl": True, "value": "claude-opus-4-6", "mode": "id"},
                "options": {"maxTokens": 1500}
            },
            "id": "claude-model", "name": "Claude Model",
            "type": "@n8n/n8n-nodes-langchain.lmChatAnthropic", "typeVersion": 1.3,
            "position": [2640, 560],
            "credentials": {"anthropicApi": {"id": anthropic_cred_id, "name": "Anthropic — Zabehaty Analytics"}}
        },
        # ── Assemble row for Sheets ──
        {
            "parameters": {
                "jsCode": r"""
const analysis = $('Build Analysis Package').first().json.analysis;
const report   = $json.text || $json.response || '(no response)';
const segs     = analysis.segment_summary || [];
const find     = name => (segs.find(s => s.segment === name) || {});
return [{ json: {
  timestamp:     analysis.generated_at,
  total_users:   analysis.platform_totals?.total_users,
  total_gmv_aed: analysis.platform_totals?.total_gmv_aed,
  champions:     find('Champions').user_count || 0,
  at_risk:       find('At Risk').user_count || 0,
  lost:          find('Lost').user_count || 0,
  report_text:   report.substring(0, 4000),
}}];
"""
            },
            "id": "assemble", "name": "Assemble Report Row",
            "type": "n8n-nodes-base.code", "typeVersion": 2,
            "position": [2860, 380]
        },
        # ── Write headers to A1 (PUT, idempotent, safe to run every time) ──
        {
            "parameters": {
                "method": "PUT",
                "url": f"https://sheets.googleapis.com/v4/spreadsheets/{SHEETS_ID or 'MISSING'}/values/n8n%20Reports!A1:G1?valueInputOption=RAW",
                "authentication": "predefinedCredentialType",
                "nodeCredentialType": "googleSheetsOAuth2Api",
                "sendBody": True,
                "specifyBody": "json",
                "jsonBody": '{"values":[["timestamp","total_users","total_gmv_aed","champions","at_risk","lost","report_text"]]}',
                "options": {}
            },
            "id": "ensure-headers", "name": "Ensure Sheet Headers",
            "type": "n8n-nodes-base.httpRequest", "typeVersion": 4.2,
            "position": [3080, 380],
            "continueOnFail": True,
            "credentials": {"googleSheetsOAuth2Api": {"id": GSHEETS_CRED_ID, "name": "Google Sheets account"}}
        },
        # ── Restore report data lost through the HTTP node ──
        {
            "parameters": {"jsCode": "return [{ json: $('Assemble Report Row').first().json }];"},
            "id": "restore-data", "name": "Restore Report Data",
            "type": "n8n-nodes-base.code", "typeVersion": 2,
            "position": [3300, 380]
        },
        # ── Append data row via Sheets API (POST values.append) ──
        {
            "parameters": {
                "method": "POST",
                "url": f"https://sheets.googleapis.com/v4/spreadsheets/{SHEETS_ID or 'MISSING'}/values/n8n%20Reports!A:G:append?valueInputOption=RAW&insertDataOption=INSERT_ROWS",
                "authentication": "predefinedCredentialType",
                "nodeCredentialType": "googleSheetsOAuth2Api",
                "sendBody": True,
                "specifyBody": "json",
                "jsonBody": '={{ JSON.stringify({values:[[' +
                    '$json.timestamp,' +
                    'String($json.total_users||""),' +
                    'String($json.total_gmv_aed||""),' +
                    'String($json.champions||0),' +
                    'String($json.at_risk||0),' +
                    'String($json.lost||0),' +
                    '($json.report_text||"").substring(0,4000)' +
                    ']]}) }}',
                "options": {}
            },
            "id": "append-row", "name": "Append Row to Sheet",
            "type": "n8n-nodes-base.httpRequest", "typeVersion": 4.2,
            "position": [3520, 380],
            "credentials": {"googleSheetsOAuth2Api": {"id": GSHEETS_CRED_ID, "name": "Google Sheets account"}}
        },
        # ── Done ──
        {
            "parameters": {"jsCode": "console.log('Zabehaty analytics complete:', new Date().toISOString()); return items;"},
            "id": "done", "name": "Done",
            "type": "n8n-nodes-base.code", "typeVersion": 2,
            "position": [3740, 380]
        }
    ]

    connections = {
        # Triggers → Start
        "Weekly Schedule (Mon 6am)": {"main": [[{"node": "Start",                    "type": "main", "index": 0}]]},
        "Manual Trigger":            {"main": [[{"node": "Start",                    "type": "main", "index": 0}]]},
        # Sequential data chain (gate between each query resets item count to 1)
        "Start":                     {"main": [[{"node": "Query RFM Summary",        "type": "main", "index": 0}]]},
        "Query RFM Summary":         {"main": [[{"node": "Gate — After RFM",         "type": "main", "index": 0}]]},
        "Gate — After RFM":          {"main": [[{"node": "Query Platform Totals",    "type": "main", "index": 0}]]},
        "Query Platform Totals":     {"main": [[{"node": "Gate — After Totals",      "type": "main", "index": 0}]]},
        "Gate — After Totals":       {"main": [[{"node": "Query Shop Performance",   "type": "main", "index": 0}]]},
        "Query Shop Performance":    {"main": [[{"node": "Gate — After Shops",       "type": "main", "index": 0}]]},
        "Gate — After Shops":        {"main": [[{"node": "Query Top Products",       "type": "main", "index": 0}]]},
        "Query Top Products":        {"main": [[{"node": "Gate — After Products",    "type": "main", "index": 0}]]},
        "Gate — After Products":     {"main": [[{"node": "Query Category Revenue",   "type": "main", "index": 0}]]},
        "Query Category Revenue":    {"main": [[{"node": "Build Analysis Package",   "type": "main", "index": 0}]]},
        "Build Analysis Package":    {"main": [[{"node": "Claude — Board Report",    "type": "main", "index": 0}]]},
        # Claude LLM model sub-node wired via ai_languageModel
        "Claude Model":              {"ai_languageModel": [[{"node": "Claude — Board Report", "type": "ai_languageModel", "index": 0}]]},
        "Claude — Board Report":     {"main": [[{"node": "Assemble Report Row",      "type": "main", "index": 0}]]},
        "Assemble Report Row":       {"main": [[{"node": "Ensure Sheet Headers",  "type": "main", "index": 0}]]},
        "Ensure Sheet Headers":      {"main": [[{"node": "Restore Report Data",    "type": "main", "index": 0}]]},
        "Restore Report Data":       {"main": [[{"node": "Append Row to Sheet",    "type": "main", "index": 0}]]},
        "Append Row to Sheet":       {"main": [[{"node": "Done",                   "type": "main", "index": 0}]]},
    }

    return {"name": "Zabehaty Analytics Agent", "nodes": nodes,
            "connections": connections, "settings": {"executionOrder": "v1"}}


# ── 4. Deploy + Activate ──────────────────────────────────────────────────────

def deploy_workflow(wf_json):
    r = requests.get(f"{BASE}/api/v1/workflows", headers=HEADERS, timeout=15)
    for wf in r.json().get("data", []):
        if wf.get("name") == "Zabehaty Analytics Agent":
            print(f"    Removing existing workflow {wf['id']}...")
            requests.delete(f"{BASE}/api/v1/workflows/{wf['id']}", headers=HEADERS, timeout=15)

    r = requests.post(f"{BASE}/api/v1/workflows", headers=HEADERS, json=wf_json, timeout=30)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Workflow create failed: {r.status_code} {r.text[:400]}")
    wf_id = r.json()["id"]
    print(f"    Workflow created: {wf_id}")
    return wf_id


def activate_workflow(wf_id):
    r = requests.post(f"{BASE}/api/v1/workflows/{wf_id}/activate", headers=HEADERS, timeout=15)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Activate failed: {r.status_code} {r.text[:200]}")
    print("    Workflow activated.")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def run():
    print("=" * 55)
    print("  ZABEHATY n8n WORKFLOW DEPLOYMENT")
    print("=" * 55)

    step("Ensuring Anthropic credential exists")
    anthropic_id = get_or_create_anthropic_cred()

    step("Building workflow JSON")
    wf_json = build_workflow(anthropic_id)

    step("Deploying workflow to n8n Cloud")
    wf_id = deploy_workflow(wf_json)

    step("Activating workflow")
    activate_workflow(wf_id)

    url = f"{BASE}/workflow/{wf_id}"
    print("\n" + "=" * 55)
    print("  DEPLOYMENT COMPLETE")
    print(f"  URL: {url}")
    print("  Schedule: Every Monday 6:00 AM")
    print("=" * 55)
    return url


if __name__ == "__main__":
    run()
