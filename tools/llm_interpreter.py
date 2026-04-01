"""
llm_interpreter.py
Uses Claude to synthesise all analysis outputs into:
  - Executive narrative per domain
  - Board-ready summary (key decisions + recommendations)
  - Communication strategy per RFM segment
  - Red flags and immediate action items

Reads from .tmp/ files produced by previous tools.
Outputs:
  .tmp/narrative_report.json   — full structured narrative
  .tmp/board_summary.md        — executive markdown report
"""

import os, sys, json, time
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")
import pandas as pd
from dotenv import load_dotenv
import anthropic

sys.path.insert(0, os.path.dirname(__file__))
load_dotenv()
os.makedirs(".tmp", exist_ok=True)

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
MODEL  = "claude-opus-4-6"


def read_json(path):
    if not os.path.exists(path): return {}
    with open(path) as f: return json.load(f)


def read_csv_summary(path, nrows=20):
    if not os.path.exists(path): return "No data available."
    df = pd.read_csv(path)
    return df.head(nrows).to_string(index=False)


# ─── BUILD CONTEXT PACKAGE ───────────────────────────────────────────────────

def build_context():
    """Aggregate all .tmp outputs into a compact context for Claude."""
    from db_connect import query_df

    segments     = read_json(".tmp/user_segments.json")
    patterns     = read_json(".tmp/buying_patterns.json")
    shop_rank    = read_json(".tmp/shop_rankings.json")
    prod_recs    = read_json(".tmp/product_recommendations.json")
    ltv_head     = read_csv_summary(".tmp/ltv_analysis.csv", 5)
    rfm_head     = read_csv_summary(".tmp/rfm_scores.csv", 5)
    bcg_head     = read_csv_summary(".tmp/bcg_matrix.csv", 10)
    cat_perf     = read_csv_summary(".tmp/category_performance.csv")
    churn_dist   = patterns.get("churn_risk_distribution", {})

    # Real-time monthly revenue (last 13 months) for YoY context
    try:
        mom = query_df("""
            SELECT DATE_FORMAT(created_at,'%Y-%m') AS month,
                   COUNT(DISTINCT id) AS orders, SUM(total) AS revenue,
                   COUNT(DISTINCT user_id) AS customers
            FROM orders WHERE status=3 AND payment_status='completed'
              AND created_at >= DATE_SUB(CURDATE(), INTERVAL 13 MONTH)
            GROUP BY DATE_FORMAT(created_at,'%Y-%m') ORDER BY month
        """)
        mom_text = mom.to_string(index=False)

        yoy = query_df("""
            SELECT YEAR(created_at) AS yr, COUNT(DISTINCT id) AS orders,
                   SUM(total) AS revenue, COUNT(DISTINCT user_id) AS users
            FROM orders WHERE status=3 AND payment_status='completed'
              AND ((created_at>='2025-01-01' AND created_at<'2025-04-01')
                OR (created_at>='2024-01-01' AND created_at<'2024-04-01'))
            GROUP BY YEAR(created_at) ORDER BY yr
        """)
        yoy_text = yoy.to_string(index=False)

        one_time = query_df("""
            SELECT COUNT(*) AS n FROM (
                SELECT user_id FROM orders WHERE status=3 AND payment_status='completed'
                GROUP BY user_id HAVING COUNT(*)=1 AND MAX(created_at)<DATE_SUB(NOW(), INTERVAL 90 DAY)
            ) x
        """)
        one_time_n = int(one_time.iloc[0]['n'])

        stopped = query_df("""
            SELECT COUNT(*) AS n FROM (
                SELECT user_id FROM orders WHERE status=3 AND payment_status='completed'
                GROUP BY user_id HAVING COUNT(*)>=5 AND MAX(created_at)<DATE_SUB(NOW(), INTERVAL 90 DAY)
            ) x
        """)
        stopped_n = int(stopped.iloc[0]['n'])

        realtime_section = f"""
--- REAL-TIME BUSINESS METRICS ---
Monthly revenue (last 13 months):
{mom_text}

Q1 YoY comparison (2024 vs 2025):
{yoy_text}

One-time buyers (never returned after 90+ days): {one_time_n:,}
Loyal customers (5+ orders) who stopped buying (90+ days inactive): {stopped_n:,}
"""
    except Exception as e:
        realtime_section = f"--- REAL-TIME METRICS unavailable: {e} ---"

    ctx = f"""
=== ZABEHATY PLATFORM ANALYTICS — DATA SUMMARY ===
Platform: UAE-based Islamic meat/slaughter/food marketplace (currency: AED)
Analysis date: March 2026

--- USER SEGMENTS (RFM) ---
{json.dumps(segments, indent=2, default=str)[:3000]}

--- LTV SAMPLE (top rows) ---
{ltv_head}

--- BUYING PATTERNS ---
Repeat purchase rate: {patterns.get('repeat_purchase', {}).get('repeat_rate_pct', 'N/A')}%
Avg categories per user: {patterns.get('repeat_purchase', {}).get('avg_categories_per_user', 'N/A')}

Top category pairs (cross-sell):
{json.dumps(patterns.get('cross_category_affinity', [])[:8], indent=2, default=str)}

Peak order days:
{json.dumps(patterns.get('order_timing', {}).get('peak_days', [])[:5], indent=2)}

Payment methods:
{json.dumps(patterns.get('payment_methods', []), indent=2, default=str)}

Churn risk distribution:
{json.dumps(churn_dist, indent=2)}

--- CATEGORY PERFORMANCE ---
{cat_perf}

--- SHOP/VENDOR RANKINGS ---
Total GMV (all channels, 3-year): AED {shop_rank.get('total_gmv_aed', 0):,.0f}
Direct Zabehaty sales (own brand, no marketplace shop): AED {shop_rank.get('direct_zabehaty_revenue_aed', 0):,.0f}
Marketplace shop revenue (vendor orders only): AED {shop_rank.get('total_platform_revenue_aed', 0):,.0f}
Marketplace commission earned (third-party shops ~10%): AED {shop_rank.get('marketplace_commission_earned_aed', 0):,.0f}
Charity pass-through (100% donated, not Zabehaty income): AED {shop_rank.get('charity_passthrough_aed', 0):,.0f}
Total shops analysed: {shop_rank.get('total_shops_analysed', 0)}
Top performers:
{json.dumps(shop_rank.get('top_performers', [])[:5], indent=2, default=str)}

--- BCG MATRIX (top 10 products) ---
{bcg_head}

--- PRODUCT RECOMMENDATIONS ---
Promote ({len(prod_recs.get('promote', []))} products): top candidates —
{json.dumps(prod_recs.get('promote', [])[:5], indent=2, default=str)}
Drop candidates ({len(prod_recs.get('drop_candidates', []))} products)
Never sold but active ({len(prod_recs.get('never_sold_active', []))} products)
{realtime_section}
"""
    return ctx


# ─── PROMPTS ─────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a senior data analyst and expert marketing strategist with 15+ years
experience in e-commerce and food/FMCG businesses in the GCC region.
You speak in plain English, no jargon. You write like you're briefing a C-suite board —
direct, evidence-based, decisive. You give specific AED numbers, percentages, and
actionable recommendations. You understand Islamic consumer behaviour and UAE market context.
Never say 'it appears' or 'it seems' — speak with confidence from the data."""

SYSTEM_PROMPT_AR = """أنت محلل بيانات أول واستراتيجي تسويق خبير بخبرة تزيد على 15 عامًا في قطاع التجارة الإلكترونية والسلع الاستهلاكية السريعة في منطقة الخليج العربي.
تكتب بالعربية الفصحى المناسبة لبيئة الأعمال الإماراتية. أسلوبك مباشر وقائم على الأدلة، كأنك تقدم إحاطة لمجلس الإدارة.
تذكر دائمًا ذكر الأرقام بالدرهم الإماراتي والنسب المئوية والتوصيات القابلة للتنفيذ.
لا تقل "يبدو" أو "ربما" — تحدث بثقة من البيانات. اكتب بصيغة المذكر الرسمية."""


def call_claude(prompt, max_tokens=2000, retries=3, system=None):
    """Call Claude with exponential backoff on transient errors."""
    if system is None:
        system = SYSTEM_PROMPT
    for attempt in range(retries):
        try:
            msg = client.messages.create(
                model=MODEL,
                max_tokens=max_tokens,
                messages=[{"role": "user", "content": prompt}],
                system=system
            )
            return msg.content[0].text
        except (anthropic.InternalServerError, anthropic.APIStatusError) as e:
            if attempt < retries - 1:
                wait = 2 ** attempt * 3
                print(f"  API error ({e.__class__.__name__}), retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise


def generate_executive_summary(ctx):
    prompt = f"""Based on this Zabehaty platform analytics data, write a board-ready executive summary.

{ctx}

Write the following sections:

## 1. Business Health Snapshot (3-4 bullet points with specific numbers)
## 2. User Base: Who Are Our Customers? (segment breakdown, LTV tiers, key facts)
## 3. Revenue Concentration Risks (Pareto analysis — which users/products/shops drive revenue)
## 4. Growth Opportunities (top 3 with specific numbers and rationale)
## 5. Urgent Action Items (top 3 things to do THIS WEEK with expected impact)
## 6. Red Flags (risks the board must know about)

Be concise. Use bullet points. Include AED numbers wherever possible."""

    return call_claude(prompt, max_tokens=2000)


def generate_communication_strategy(ctx):
    prompt = f"""Based on the RFM segments and buying patterns in this data:

{ctx}

Write a communication strategy for each of the following segments. For each segment:
- Segment name + size
- What we know about them behaviourally
- Recommended channel (WhatsApp, push notification, SMS, email)
- Best timing (day/hour based on peak order data)
- Message tone and angle
- One example message (in plain English, under 50 words)
- Expected outcome

Segments to cover:
1. Champions
2. At Risk
3. Cant Lose Them
4. Lost
5. New Customers
6. Need Attention

Keep each segment brief. Focus on what will actually move the needle."""

    return call_claude(prompt, max_tokens=2500)


def generate_product_narrative(ctx):
    prompt = f"""Based on the BCG matrix, category performance, and product recommendations:

{ctx}

Write a product portfolio brief for the board covering:
1. Category stars (what's driving the business)
2. Hidden gems (underperforming categories with high potential)
3. Products to promote now (with reasoning)
4. Products/categories to wind down or deprioritise
5. Cross-sell opportunities (based on category affinity data)

Be direct. Use specific product/category names and AED revenue figures."""

    return call_claude(prompt, max_tokens=1500)


def generate_vendor_narrative(ctx):
    prompt = f"""Based on the vendor/shop performance data:

{ctx}

Write a vendor management brief covering:
1. Top vendor health (performance, risks, dependencies)
2. Revenue concentration risk (is the business too dependent on one vendor?)
3. Own-brand vs third-party split and strategic implications
4. Underperforming vendors: keep, improve, or drop?
5. Commission optimisation opportunities

Specific AED numbers required. Be direct."""

    return call_claude(prompt, max_tokens=1200)


# ─── ARABIC GENERATION ───────────────────────────────────────────────────────

def generate_executive_summary_ar(ctx):
    prompt = f"""بناءً على بيانات تحليلات منصة ذبحتي التالية، اكتب ملخصًا تنفيذيًا جاهزًا لمجلس الإدارة.

{ctx}

اكتب الأقسام التالية:

## 1. لمحة عن صحة الأعمال (3-4 نقاط بأرقام محددة)
## 2. قاعدة المستخدمين: من هم عملاؤنا؟ (تفصيل الشرائح، مستويات القيمة الدائمة، الحقائق الرئيسية)
## 3. مخاطر تركّز الإيرادات (تحليل باريتو — أي المستخدمين والمنتجات والمتاجر تحرّك الإيرادات)
## 4. فرص النمو (أبرز 3 فرص بأرقام محددة ومبررات)
## 5. إجراءات عاجلة (أهم 3 إجراءات هذا الأسبوع مع الأثر المتوقع)
## 6. إشارات تحذيرية (مخاطر يجب أن يعلمها مجلس الإدارة)

كن موجزًا. استخدم النقاط. أدرج الأرقام بالدرهم الإماراتي حيثما أمكن."""
    return call_claude(prompt, max_tokens=2000, system=SYSTEM_PROMPT_AR)


def generate_communication_strategy_ar(ctx):
    prompt = f"""بناءً على شرائح RFM وأنماط الشراء في هذه البيانات:

{ctx}

اكتب استراتيجية تواصل لكل شريحة من الشرائح التالية. لكل شريحة:
- اسم الشريحة وحجمها
- ما نعرفه عن سلوكهم
- القناة الموصى بها (واتساب، إشعار فوري، رسائل نصية، بريد إلكتروني)
- أفضل توقيت (يوم/ساعة بناءً على بيانات ذروة الطلبات)
- نبرة الرسالة وزاويتها
- مثال على رسالة واحدة (بالعربية، أقل من 50 كلمة)
- النتيجة المتوقعة

الشرائح المطلوبة:
1. الأبطال
2. في خطر
3. لا يمكن خسارتهم
4. المفقودون
5. العملاء الجدد
6. يحتاجون انتباهاً

أبقِ كل شريحة موجزة. ركّز على ما سيحرّك الإبرة فعلاً."""
    return call_claude(prompt, max_tokens=2500, system=SYSTEM_PROMPT_AR)


def generate_product_narrative_ar(ctx):
    prompt = f"""بناءً على مصفوفة BCG وأداء الفئات وتوصيات المنتجات:

{ctx}

اكتب ملخص محفظة المنتجات لمجلس الإدارة يشمل:
1. نجوم الفئات (ما الذي يحرّك الأعمال)
2. الجواهر الخفية (فئات ضعيفة الأداء لكنها عالية الإمكانية)
3. المنتجات التي يجب الترويج لها الآن (مع المبررات)
4. المنتجات/الفئات التي يجب تقليصها أو إزالتها
5. فرص البيع التكميلي (بناءً على بيانات التقارب بين الفئات)

كن مباشرًا. استخدم أسماء المنتجات/الفئات الفعلية وأرقام الإيرادات بالدرهم."""
    return call_claude(prompt, max_tokens=1500, system=SYSTEM_PROMPT_AR)


def generate_vendor_narrative_ar(ctx):
    prompt = f"""بناءً على بيانات أداء الموردين/المتاجر:

{ctx}

اكتب ملخص إدارة الموردين يشمل:
1. صحة كبار الموردين (الأداء والمخاطر والاعتماديات)
2. مخاطر تركّز الإيرادات (هل العمل معتمد بشكل مفرط على مورد واحد؟)
3. التوازن بين العلامات الذاتية والأطراف الثالثة والانعكاسات الاستراتيجية
4. الموردون ضعيفو الأداء: إبقاء، تطوير، أم إنهاء؟
5. فرص تحسين العمولات

أرقام محددة بالدرهم مطلوبة. كن مباشرًا."""
    return call_claude(prompt, max_tokens=1200, system=SYSTEM_PROMPT_AR)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def run():
    print("Building context from analysis outputs...")
    ctx = build_context()

    # ── English report ────────────────────────────────────────────────────────
    print("Generating executive summary (EN)...")
    exec_summary = generate_executive_summary(ctx)

    print("Generating communication strategy (EN)...")
    comm_strategy = generate_communication_strategy(ctx)

    print("Generating product narrative (EN)...")
    product_narrative = generate_product_narrative(ctx)

    print("Generating vendor narrative (EN)...")
    vendor_narrative = generate_vendor_narrative(ctx)

    report_en = {
        "executive_summary": exec_summary,
        "communication_strategy": comm_strategy,
        "product_narrative": product_narrative,
        "vendor_narrative": vendor_narrative,
    }
    with open(".tmp/narrative_report.json", "w", encoding="utf-8") as f:
        json.dump(report_en, f, indent=2, ensure_ascii=False)

    # ── Arabic report ─────────────────────────────────────────────────────────
    print("Generating executive summary (AR)...")
    exec_summary_ar = generate_executive_summary_ar(ctx)

    print("Generating communication strategy (AR)...")
    comm_strategy_ar = generate_communication_strategy_ar(ctx)

    print("Generating product narrative (AR)...")
    product_narrative_ar = generate_product_narrative_ar(ctx)

    print("Generating vendor narrative (AR)...")
    vendor_narrative_ar = generate_vendor_narrative_ar(ctx)

    report_ar = {
        "executive_summary": exec_summary_ar,
        "communication_strategy": comm_strategy_ar,
        "product_narrative": product_narrative_ar,
        "vendor_narrative": vendor_narrative_ar,
    }
    with open(".tmp/narrative_report_ar.json", "w", encoding="utf-8") as f:
        json.dump(report_ar, f, indent=2, ensure_ascii=False)

    # ── Board markdown (English) ──────────────────────────────────────────────
    board_md = f"""# Zabehaty Analytics Report
*Generated: March 2026*

---

{exec_summary}

---

## Product Portfolio

{product_narrative}

---

## Vendor Performance

{vendor_narrative}

---

## Communication Strategy by Segment

{comm_strategy}
"""
    with open(".tmp/board_summary.md", "w", encoding="utf-8") as f:
        f.write(board_md)

    print("\n=== EXECUTIVE SUMMARY PREVIEW ===")
    print(exec_summary[:800] + "\n...[truncated]")
    print("\nReports saved:")
    print("  .tmp/narrative_report.json    (English)")
    print("  .tmp/narrative_report_ar.json (Arabic)")
    print("  .tmp/board_summary.md         (English markdown)")

    return report_en


if __name__ == "__main__":
    run()
