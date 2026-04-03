"""
dashboard/app.py
Zabehaty Interactive Analytics Dashboard — Streamlit
Run: streamlit run dashboard/app.py
"""

import os, sys, json, warnings
warnings.filterwarnings("ignore")

import streamlit as st
import pandas as pd
import plotly.express as px
from dotenv import load_dotenv
load_dotenv()

# DB access for live queries (Business Health page)
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "tools"))
try:
    from db_connect import query_df as _db_query
    _DB_AVAILABLE = True
except Exception:
    _DB_AVAILABLE = False
try:
    from dateutil.relativedelta import relativedelta
except ImportError:
    relativedelta = None

# ══════════════════════════════════════════════════════════════════════════════
# CLOUD DATA BOOTSTRAP
# Streamlit Cloud has an ephemeral filesystem — .tmp/ files don't survive
# a cold start. On first load we run the analysis pipeline to regenerate them.
# Results are cached for 2 hours so repeated page-switches don't re-query.
# ══════════════════════════════════════════════════════════════════════════════
_APP_ROOT = os.path.dirname(os.path.dirname(__file__))
_PIPELINE_FILES = [
    "rfm_scores.csv", "ltv_analysis.csv", "churn_risk.csv",
    "bcg_matrix.csv", "shop_performance.csv", "shop_rankings.json",
    "buying_patterns.json", "category_performance.csv",
]

def _pipeline_files_exist():
    return all(os.path.exists(os.path.join(_APP_ROOT, ".tmp", f)) for f in _PIPELINE_FILES)

def _run_pipeline():
    """Run all analysis tools to populate .tmp/. Called once on cold start."""
    import sys
    sys.path.insert(0, os.path.join(_APP_ROOT, "tools"))
    os.makedirs(os.path.join(_APP_ROOT, ".tmp"), exist_ok=True)
    from user_analysis    import run as _run_user
    from product_analysis import run as _run_products
    from shop_analysis    import run as _run_shops
    from buying_patterns  import run as _run_patterns
    _run_user()
    _run_products()
    _run_shops()
    _run_patterns()

if "pipeline_ready" not in st.session_state:
    st.session_state.pipeline_ready = _pipeline_files_exist()

try:
    import matplotlib
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Zabehaty Analytics",
    page_icon="🥩",
    layout="wide",
    initial_sidebar_state="auto",
)

ROOT = os.path.dirname(os.path.dirname(__file__))
TMP  = os.path.join(ROOT, ".tmp")

# ══════════════════════════════════════════════════════════════════════════════
# LANGUAGE SYSTEM
# ══════════════════════════════════════════════════════════════════════════════
if "lang"     not in st.session_state: st.session_state.lang     = "en"
if "page_idx" not in st.session_state: st.session_state.page_idx = 0   # 0 = AI Analyst (first in list)

STRINGS = {
    "en": {
        # Sidebar
        "app_title": "Zabehaty Analytics", "subtitle": "Board Intelligence Dashboard",
        "navigation": "Navigation", "refresh": "Refresh Data Cache",
        "data_source": "Data: replica_uae | Currency: AED", "language": "Language",
        # Pages
        "page_health": "Business Snapshot",
        "page_overview": "Overview", "page_segments": "User Segments",
        "page_products": "Products & BCG", "page_vendors": "Vendors",
        "page_patterns": "Buying Patterns", "page_report": "Board Report",
        "page_agent": "🤖 AI Analyst",
        # AI Agent page
        "agent_title": "AI Business Analyst",
        "agent_subtitle": "Ask anything about the business — in English or Arabic. I'll show exactly where every number comes from.",
        "agent_clear": "Clear conversation",
        "agent_placeholder": "Ask a question… e.g. 'How many orders this month?'",
        "agent_voice_label": "Or speak your question:",
        "agent_thinking": "Thinking…",
        "agent_download_ready": "📥 Your report is ready — download below",
        "agent_download_btn": "⬇ Download Excel Report",
        # Date filter
        "date_filter": "Date Range Filter", "date_from": "From", "date_to": "To", "days": "days",
        "date_filter_info": "Filters users by last order date. Applies to Overview and Segments pages only. Products, Vendors, Health, and Patterns always show all-time data.",
        "date_filter_note": "📅 Date filter active — showing filtered user subset.",
        "date_filter_no_effect": "ℹ️ Date filter is set ({f} → {t}) but does **not** apply to this page. Switch to **Overview** or **User Segments** to see filtered results.",
        "date_filter_count": "Showing **{n:,}** of {total:,} users ({pct:.0f}%)",
        # KPI labels
        "active_buyers": "Active Buyers", "platform_gmv": "Total Lifetime Revenue",
        "avg_ltv": "Avg User LTV", "repeat_rate": "Multi-Category Rate", "at_risk": "At-Risk Users",
        "repeat_rate_sublabel": "(bought from 2+ categories)",
        "platform_gmv_note": "Sum of all users' lifetime spend since platform launch.",
        # Overview page
        "overview_title": "Platform Overview",
        "user_segments_chart": "User Segments", "revenue_by_segment": "Revenue by Segment",
        "ltv_tier_dist": "LTV Tier Distribution",
        # Segments page
        "segments_title": "User Segments & LTV", "filter_segment": "Filter by Segment",
        "all": "All", "users_in_view": "Users in View", "avg_ltv_label": "Avg LTV",
        "avg_recency": "Avg Recency", "total_revenue": "Total Revenue",
        "rfm_map": "RFM Map — Recency vs Monetary Value",
        "segment_table": "Segment Breakdown Table", "churn_risk": "Churn Risk Distribution",
        # Products page
        "products_title": "Product Portfolio — BCG Matrix",
        "bcg_chart_title": "BCG Matrix — Market Share vs Vitality",
        "category_perf": "Category Performance",
        "tab_promote": "✅ Promote", "tab_drop": "⚠️ Drop Candidates", "tab_never": "👻 Never Sold",
        # Vendors page
        "vendors_title": "Vendor / Shop Performance", "shops_analysed": "Shops Analysed",
        "total_rev_kpi": "Total Revenue", "est_commissions": "Est. Commissions",
        "own_brands": "Own Brands", "revenue_by_shop": "Revenue by Shop",
        "health_vs_cancel": "Health Score vs Cancellation Rate",
        "full_vendor_table": "Full Vendor Table",
        # Patterns page
        "patterns_title": "How Customers Shop",
        "total_buyers": "Total Buyers", "multi_category": "Multi-Category Buyers",
        "repeat_rate_label": "Multi-Category Rate", "peak_days": "Peak Order Days",
        "peak_hours": "Peak Order Hours", "payment_split": "Payment Method Split",
        "top_pairs": "Top Category Pairs (Co-Buyers)",
        # Board report
        "report_title": "Board Intelligence Report", "download_report": "⬇ Download Report (.md)",
        "tab_exec": "Executive Summary", "tab_product": "Product Portfolio",
        "tab_vendor": "Vendor Performance", "tab_comm": "Communication Strategy",
        # Errors
        "no_data": "Run `python main.py --skip-sheets` to generate data first.",
        "no_narrative": "No narrative generated yet. Run `python main.py` first.",
        # Chart axis labels
        "ax_days_since": "Days Since Last Order", "ax_ltv_aed": "Lifetime Value (AED)",
        "ax_market_share": "Market Share % (within category)", "ax_vitality": "Vitality Score",
        "ax_revenue": "Revenue (AED)", "ax_category": "Category",
        "ax_cancel_rate": "Cancel Rate %", "ax_health_score": "Health Score (0-100)",
        "ax_hour": "Hour (24h)", "ax_co_buyers": "Co-Buyers", "ax_shop": "Shop",
        "ax_total_revenue": "Total Revenue (AED)", "ax_ltv_tier": "LTV Tier",
        "ax_users": "Users", "ax_orders": "Orders", "ax_day": "Day",
        "ax_products": "products",
        "vendor_no_date_filter": "ℹ️ Vendor data shows all-time performance — not affected by the date filter above.",
        "peak_day_tooltip": "Best day to send campaigns",
        "peak_hour_tooltip": "Peak hour — ideal for push notifications",
        "crosssell_all_title": "Top Category Pairs (All)",
        "crosssell_other_title": "Cross-Sell Pairs (Excluding Personal)",
        "vendor_own_brand": "Own Brand", "vendor_third_party": "Third-Party",
        "vendor_marketplace": "Marketplace", "vendor_charity": "Charity / Donations",
        "vendor_rev_split": "Platform Revenue by Source",
        "vendor_own_revenue": "Own Brand Revenue",
        "vendor_marketplace_commission": "Marketplace Commission Earned",
        "vendor_charity_passthrough": "Charity Pass-through",
        "vendor_charity_note": "Charity orders are donation pass-throughs — this revenue goes to charities, not Zabehaty.",
        "vendor_gmv": "Total Platform GMV (3-Year)",
        "vendor_direct_sales": "Direct Zabehaty Sales",
        "vendor_direct_note": "Orders processed directly by Zabehaty (no marketplace shop). Included in GMV but not in the shop revenue split below.",
        "vendor_to_review": "Vendors to Review",
        "vendor_to_review_note": "These vendors have minimal activity or low health scores. Consider reaching out or removing from the platform.",
        "bcg_growth_caveat": "📊 Growth rate: last 30 days vs same period last year (YoY fallback active — prior 30-day window fell inside the Dec 2025–Feb 2026 data gap).",
        # Segment names (for display)
        "seg_champions": "Champions", "seg_loyal": "Loyal Customers",
        "seg_new": "New Customers", "seg_potential": "Potential Loyalists",
        "seg_promising": "Promising", "seg_at_risk": "At Risk",
        "seg_cant_lose": "Cant Lose Them", "seg_need_attention": "Need Attention",
        "seg_about_sleep": "About to Sleep", "seg_lost": "Lost",
        # BCG / Churn labels
        "bcg_star": "Star", "bcg_cow": "Cash Cow", "bcg_qm": "Question Mark", "bcg_dog": "Dog",
        "churn_critical": "Critical", "churn_high": "High", "churn_medium": "Medium", "churn_low": "Low",
        # LTV tiers
        "tier_platinum": "Platinum", "tier_gold": "Gold", "tier_silver": "Silver", "tier_bronze": "Bronze",
        # Date presets
        "preset_label": "Quick select:", "preset_last_month": "Last Month",
        "preset_last_quarter": "Last Quarter", "preset_6months": "Last 6 Mo.", "preset_last_year": "Last Year",
        # Period banner
        "period_filtered": "📅 Showing results for: {f} → {t}",
        "period_all": "📅 All-time data: {f} → {t}",
        # Segment descriptions (plain language)
        "seg_desc_champions": "Top buyers — ordered recently, often, and spent the most.",
        "seg_desc_at_risk": "Were loyal but haven't ordered in a while. A targeted offer could bring them back.",
        "seg_desc_cant_lose": "High spenders going quiet. Top priority to win back before they're gone.",
        "seg_desc_lost": "Haven't ordered in a very long time. Hard to recover — try a strong incentive.",
        "seg_desc_new": "Placed their first order recently. Nurture them into loyal repeat buyers.",
        "seg_desc_need_attention": "Moderate buyers showing early signs of disengagement.",
        "seg_desc_loyal": "Consistent buyers across multiple categories. Reward their loyalty.",
        "seg_desc_potential": "Buy regularly but haven't reached full loyalty yet — one more nudge needed.",
        "seg_desc_promising": "Newer buyers showing strong early signals. Great candidates for upselling.",
        "seg_desc_about_sleep": "Were active but orders are slowing down — watch this group closely.",
        # BCG quadrant descriptions
        "bcg_desc_star": "Growing fast with strong sales. Invest more here.",
        "bcg_desc_cow": "Steady and profitable. Protect and maintain.",
        "bcg_desc_qm": "Potential but unproven. Decide: invest to grow or cut losses.",
        "bcg_desc_dog": "Low growth, low sales. Consider removing from the catalogue.",
        # Churn level descriptions
        "churn_desc_critical": "Very likely to never buy again. Act immediately.",
        "churn_desc_high": "At risk — haven't ordered in months. Send a win-back offer now.",
        "churn_desc_medium": "Early warning signs. Monitor and send a gentle reminder.",
        "churn_desc_low": "Healthy and active. Keep them happy.",
        # Page intro captions
        "intro_segments": "Your customers are not all equal — this shows who your most valuable buyers are and who is at risk of leaving.",
        "intro_rfm": "Each dot is one customer. Dots to the LEFT ordered recently. Dots at the TOP spent the most. Colour shows the loyalty group.",
        "intro_churn": "These are customers at risk of disappearing. The smaller the Critical bar, the healthier your retention.",
        "intro_bcg": "A quick health check for every product — growing, stable, or dragging the business down?",
        "intro_category": "Which product categories are bringing in the most revenue.",
        "intro_vendors": "How each of your suppliers performs — revenue, reliability, and health.",
        "intro_patterns": "When your customers shop and what they prefer to buy together. Use this to time campaigns.",
        # Traffic-light callout messages
        "tl_champions": "✅ Your Champions are strong. Reward them with exclusive early access or special deals.",
        "tl_at_risk": "⚠️ These customers were loyal before — a targeted offer this week could bring them back.",
        "tl_cant_lose": "🔴 High-value customers going quiet. This needs urgent personal outreach.",
        "tl_lost_high": "⚠️ A large Lost segment means past retention efforts weren't enough. Review your follow-up strategy.",
        "tl_repeat_strong": "✅ Strong repeat buying — customers are coming back on their own.",
        "tl_repeat_weak": "⚠️ Most customers buy only once. A loyalty programme or reminder campaign could double this.",
        "tl_churn_high": "🔴 More than 1 in 5 customers are at risk of leaving — retention campaigns are urgent.",
        # Board report Arabic translation
        "translate_report_btn": "🔄 Translate Report to Arabic",
        "translating": "Translating report into Arabic — takes about 30 seconds...",
        "translate_done": "✅ Report translated. Switch to العربية to read it.",
        # Renamed user-friendly labels
        "rfm_map": "Customer Loyalty Map",
        "churn_risk": "Customers At Risk of Leaving",
        "ltv_tier_dist": "Customer Value Tiers",
        "page_patterns": "How Customers Shop",
        "vendors_title": "Supplier & Shop Performance",
        "bcg_chart_title": "Product Health Map — BCG Matrix",
        # LTV tier guide
        "tier_guide_title": "📖 How are customer tiers defined?",
        "tier_guide_caption": "Every customer is assigned a tier based on their **total lifetime spend** on Zabehaty (all orders ever, AED):",
        "tier_platinum_def": "🏆 **Platinum** — spent **AED 5,000 or more** in total. Top ~3% of customers. Highest-value segment — treat with VIP care.",
        "tier_gold_def":     "🥇 **Gold** — spent **AED 2,000 – 4,999** in total. Strong loyal buyers. ~21% of customers.",
        "tier_silver_def":   "🥈 **Silver** — spent **AED 500 – 1,999** in total. Moderate spenders with growth potential. ~52% of customers.",
        "tier_bronze_def":   "🥉 **Bronze** — spent **less than AED 500** in total. New or light buyers. ~24% of customers.",
        "tier_guide_note":   "Thresholds are based on cumulative lifetime spend — a customer moves up a tier as soon as their total crosses the threshold, regardless of when orders were placed.",
        # Segment guide
        "seg_guide_title": "📖 Segment Definitions",
        "seg_guide_caption": "How each loyalty segment is defined and what action to take:",
        "seg_guide_champions":    "**Champions** — Ordered recently (low recency), ordered often (high frequency), high spend. RFM scores: R=4-5, F=4-5, M=4-5. These are your best customers.",
        "seg_guide_loyal":        "**Loyal Customers** — High frequency and monetary, moderate recency. Buy consistently across multiple categories.",
        "seg_guide_potential":    "**Potential Loyalists** — Recent buyers with moderate frequency. One more nudge could convert them to loyal.",
        "seg_guide_new":          "**New Customers** — First order was recent (last 30-60 days). Recency is high but no history yet.",
        "seg_guide_promising":    "**Promising** — Recent and low frequency. Good early signals — encourage second purchase.",
        "seg_guide_need_attn":    "**Need Attention** — Above-average RFM but starting to slip. Haven't ordered for a while.",
        "seg_guide_at_risk":      "**At Risk** — Were loyal, now drifting. Recency is rising fast. A targeted win-back offer is needed now.",
        "seg_guide_cant_lose":    "**Can't Lose Them** — High-value customers (top monetary), but recency is very high. Urgent personal outreach needed.",
        "seg_guide_about_sleep":  "**About to Sleep** — Moderate scores declining. Watch closely and send a reminder.",
        "seg_guide_lost":         "**Lost** — Lowest recency scores. Haven't ordered in a very long time. Re-activation is difficult.",
        # BCG explanation
        "bcg_explain_title": "📖 What is the BCG Matrix?",
        "bcg_explain_body": (
            "The **BCG Matrix** (Boston Consulting Group) classifies every product on two axes:\n\n"
            "- **X-axis — Market Share %:** This product's share of revenue within its own category. "
            "A product at 80% means it dominates its category.\n"
            "- **Y-axis — Vitality Score:** A composite of recent sales volume and margin. "
            "High vitality = actively selling and profitable right now.\n\n"
            "**The four quadrants:**\n"
            "- ⭐ **Star** (high share + high vitality): Your strongest performers. Invest in marketing and stock.\n"
            "- 🐄 **Cash Cow** (high share + low vitality): Reliable revenue but growth is slowing. Maintain carefully.\n"
            "- ❓ **Question Mark** (low share + high vitality): Growing fast but not dominant yet. Decide: push harder or cut.\n"
            "- 🐕 **Dog** (low share + low vitality): Low growth, low sales. Consider removing from the catalogue.\n\n"
            "**Bubble size** = total revenue. The dashed lines are medians — products above and to the right are above average."
        ),
        # Vendor column labels
        "vnd_col_shop": "Shop", "vnd_col_type": "Type", "vnd_col_shop_type": "Shop Category",
        "vnd_col_orders": "Orders", "vnd_col_revenue": "Revenue (AED)",
        "vnd_col_aov": "Avg Order (AED)", "vnd_col_share": "Revenue Share %",
        "vnd_col_cancel": "Cancel Rate %", "vnd_col_health": "Health Score",
        "vnd_col_comm_pct": "Commission %", "vnd_col_comm_est": "Est. Commission (AED)",
        "vnd_reason_one": "Only 1 order", "vnd_reason_few": "< 5 orders",
        "vnd_reason_low_health": "Low health score", "vnd_reason_test": "Test shop — delete from DB",
        "vnd_reason_low": "Low activity", "vnd_reason_col": "Reason",
        # GMV note
        "gmv_discrepancy_note": (
            "**Why do GMV figures differ across pages?**\n\n"
            "- **Overview page (AED 288M)** — sum of `user_total_orders.total` per user. "
            "This table has **no order-status filter** — it includes all order types "
            "(pending, cancelled, delivered). Overstates actual received revenue.\n\n"
            "- **Vendor / Snapshot page (AED 217M)** — only `orders` where `status=3` "
            "(delivered) AND `payment_status='completed'`. "
            "This is the accurate **cash-in revenue** figure.\n\n"
            "**The AED 217M figure is the correct GMV to report to the board.** "
            "The AED 288M difference (~AED 71M) represents cancelled, pending, or "
            "unconfirmed orders that were never actually collected."
        ),
        # Health / Business Snapshot page
        "health_title": "📊 Business Snapshot",
        "health_subtitle": "A plain-English view of how the business is doing right now. No jargon — just the key numbers that matter.",
        "health_no_comparison": "No comparison data",
        "health_growing": "Growing", "health_stable": "Stable", "health_declining": "Declining",
        "health_vs_lyst": "revenue {pct}% vs same month last year",
        "health_revenue_month": "Revenue This Month", "health_orders_month": "Orders This Month",
        "health_customers_month": "Customers This Month", "health_signups_month": "New Buyers (month)",
        "health_vs_last_year": "vs same month last year",
        "health_gmv_total": "**3-Year Platform Total (GMV):** AED {gmv}  —  all orders ever placed on Zabehaty since launch.",
        "health_revenue_chart": "📈 Revenue by Month",
        "health_revenue_chart_note": "How much money came in each month. The higher the bar, the better.",
        "health_gap_filled": "ℹ️ **Data gap filled with seasonal estimates:** {months} orders were not recorded in the database. Amber bars are estimated using a **{factor} YoY growth factor** applied to the same months in the prior year. Actual data is unavailable.",
        "health_partial_gap": "⚠️ **Partial data gap:** {months} could not be estimated (no prior-year baseline available).",
        "health_synth_banner": "📊 Dec 2025 – Feb 2026 data is model-estimated (YoY forecast). Marked [est.] in charts. Actual DB data resumes March 2026.",
        "health_best_shops": "🏆 Best Performing Shops", "health_best_shops_note": "Top shops by revenue in the last 90 days.",
        "health_best_products": "⭐ Best Selling Products", "health_best_products_note": "Top products by revenue in the last 90 days.",
        "health_loyal_customers": "💎 Most Loyal Customers",
        "health_loyal_note": "Customers who have spent the most on Zabehaty over their lifetime.",
        "health_insights": "💡 Business Insights", "health_alerts": "🚨 Alerts & Watch Points",
        "health_no_alerts": "✅ No critical alerts right now. Business looks healthy.",
        "health_col_shop": "Shop", "health_col_orders": "Orders", "health_col_revenue": "Revenue (AED)",
        "health_col_product": "Product", "health_col_category": "Category", "health_col_customer": "Customer",
        "health_col_total_orders": "Total Orders", "health_col_lifetime": "Lifetime Spend", "health_col_last_order": "Last Order",
        "health_days_ago_ok": "✅ {n} days ago", "health_days_ago_warn": "⚠️ {n} days ago", "health_days_ago_bad": "🔴 {n} days ago",
        "health_estimated": "Estimated", "health_actual": "Actual", "health_ax_month": "Month",
        "health_insight_repeat": "**Repeat customers:** {pct}% of buyers have ordered more than once. {note}",
        "health_insight_repeat_ok": "That's healthy for a food marketplace.",
        "health_insight_repeat_low": "This is low — focus on getting first-time buyers to come back.",
        "health_insight_concentration": "**Revenue concentration:** Your top 10 customers account for {pct}% of all-time revenue. {note}",
        "health_insight_conc_ok": "Well spread.", "health_insight_conc_high": "High dependency — consider diversifying your customer base.",
        "health_insight_base": "**Customer base:** {total:,} registered buyers — {champ:,} are highly active Champions, {lost:,} have gone quiet.",
        "health_insight_peak": "**Peak ordering days:** Thursday and Friday are your busiest. Schedule promotions to land on Wednesday to drive Thursday spikes.",
        "health_insight_direct": "**Direct vs marketplace:** {pct}% of all revenue comes from Zabehaty's own products (not third-party shops).",
        "health_alert_onetime": "**{n:,} customers** ordered once and never came back (90+ days ago). This is your biggest retention opportunity. A simple follow-up message could recover many of them.",
        "health_alert_loyals": "**{n:,} loyal customers** (5+ orders each) have gone quiet — no order in 90+ days. These are your most valuable people to re-engage immediately.",
        "health_alert_critical": "**{n:,} customers** are at critical churn risk right now. They used to buy regularly but have stopped. Act within 2 weeks.",
        "health_alert_revenue": "**Revenue is down {pct}%** vs same month last year. Investigate whether this is seasonal or a sign of a real issue.",
    },
    "ar": {
        # Sidebar
        "app_title": "تحليلات ذبحتي", "subtitle": "لوحة ذكاء مجلس الإدارة",
        "navigation": "التنقل", "refresh": "تحديث البيانات",
        "data_source": "البيانات: replica_uae | العملة: درهم", "language": "اللغة",
        # Pages
        "page_overview": "نظرة عامة", "page_segments": "شرائح المستخدمين",
        "page_products": "المنتجات ومصفوفة BCG", "page_vendors": "الموردون",
        "page_patterns": "أنماط الشراء", "page_report": "تقرير مجلس الإدارة",
        "page_agent": "🤖 المحلل الذكي",
        # AI Agent page
        "agent_title": "محلل الأعمال الذكي",
        "agent_subtitle": "اسأل أي شيء عن الأعمال — بالعربية أو الإنجليزية. سأوضح من أين أتى كل رقم.",
        "agent_clear": "مسح المحادثة",
        "agent_placeholder": "اكتب سؤالك… مثال: كم عدد الطلبات هذا الشهر؟",
        "agent_voice_label": "أو تحدث بسؤالك:",
        "agent_thinking": "جارٍ التفكير…",
        "agent_download_ready": "📥 تقريرك جاهز — حمّله أدناه",
        "agent_download_btn": "⬇ تحميل تقرير Excel",
        # Date filter
        "date_filter": "فلتر نطاق التاريخ", "date_from": "من", "date_to": "إلى", "days": "يوم",
        "date_filter_info": "يفلتر المستخدمين حسب تاريخ آخر طلب. يُطبَّق على صفحتَي النظرة العامة وشرائح المستخدمين فقط. المنتجات والموردون والصحة والأنماط تعرض دائمًا البيانات الكاملة.",
        "date_filter_note": "📅 فلتر التاريخ نشط — عرض مجموعة مستخدمين مصفاة.",
        "date_filter_no_effect": "ℹ️ فلتر التاريخ مضبوط ({f} → {t}) لكنه **لا يؤثر** على هذه الصفحة. انتقل إلى **النظرة العامة** أو **شرائح المستخدمين** لرؤية النتائج المصفاة.",
        "date_filter_count": "عرض **{n:,}** من {total:,} مستخدم ({pct:.0f}%)",
        # KPI labels
        "active_buyers": "المشترون النشطون", "platform_gmv": "إجمالي الإيرادات التراكمية",
        "avg_ltv": "متوسط القيمة الدائمة", "repeat_rate": "معدل تعدد الفئات",
        "at_risk": "المستخدمون المعرضون للخطر",
        "repeat_rate_sublabel": "(اشتروا من فئتين أو أكثر)",
        "platform_gmv_note": "مجموع إنفاق جميع المستخدمين مدى الحياة منذ إطلاق المنصة.",
        # Overview page
        "overview_title": "نظرة عامة على المنصة",
        "user_segments_chart": "شرائح المستخدمين", "revenue_by_segment": "الإيرادات حسب الشريحة",
        "ltv_tier_dist": "توزيع مستويات القيمة الدائمة",
        # Segments page
        "segments_title": "شرائح المستخدمين والقيمة الدائمة", "filter_segment": "فلترة حسب الشريحة",
        "all": "الكل", "users_in_view": "المستخدمون في العرض", "avg_ltv_label": "متوسط القيمة الدائمة",
        "avg_recency": "متوسط الحداثة", "total_revenue": "إجمالي الإيرادات",
        "rfm_map": "خريطة RFM — الحداثة مقابل القيمة النقدية",
        "segment_table": "جدول تفصيل الشرائح", "churn_risk": "توزيع مخاطر فقدان العملاء",
        # Products page
        "products_title": "محفظة المنتجات — مصفوفة BCG",
        "bcg_chart_title": "مصفوفة BCG — الحصة السوقية مقابل الحيوية",
        "category_perf": "أداء الفئات",
        "tab_promote": "✅ للترويج", "tab_drop": "⚠️ مرشحو الإزالة", "tab_never": "👻 لم يُباع قط",
        # Vendors page
        "vendors_title": "أداء المورد / المتجر", "shops_analysed": "المتاجر المحللة",
        "total_rev_kpi": "إجمالي الإيرادات", "est_commissions": "العمولات المقدرة",
        "own_brands": "العلامات الذاتية", "revenue_by_shop": "الإيرادات حسب المتجر",
        "health_vs_cancel": "درجة الصحة مقابل معدل الإلغاء",
        "full_vendor_table": "جدول الموردين الكامل",
        # Patterns page
        "patterns_title": "كيف يتسوق العملاء",
        "total_buyers": "إجمالي المشترين", "multi_category": "مشترو الفئات المتعددة",
        "repeat_rate_label": "معدل تعدد الفئات", "peak_days": "أيام الذروة للطلبات",
        "peak_hours": "ساعات الذروة للطلبات", "payment_split": "توزيع طرق الدفع",
        "top_pairs": "أبرز أزواج الفئات (مشترون مشتركون)",
        # Board report
        "report_title": "تقرير ذكاء مجلس الإدارة", "download_report": "⬇ تنزيل التقرير (.md)",
        "tab_exec": "الملخص التنفيذي", "tab_product": "محفظة المنتجات",
        "tab_vendor": "أداء الموردين", "tab_comm": "استراتيجية التواصل",
        # Errors
        "no_data": "شغّل `python main.py --skip-sheets` لتوليد البيانات أولاً.",
        "no_narrative": "لم يتم توليد أي تقرير بعد. شغّل `python main.py` أولاً.",
        # Chart axis labels
        "ax_days_since": "أيام منذ آخر طلب", "ax_ltv_aed": "القيمة الدائمة (درهم)",
        "ax_market_share": "الحصة السوقية % (داخل الفئة)", "ax_vitality": "درجة الحيوية",
        "ax_revenue": "الإيرادات (درهم)", "ax_category": "الفئة",
        "ax_cancel_rate": "معدل الإلغاء %", "ax_health_score": "درجة الصحة (0-100)",
        "ax_hour": "الساعة (24)", "ax_co_buyers": "المشترون المشتركون", "ax_shop": "المتجر",
        "ax_total_revenue": "إجمالي الإيرادات (درهم)", "ax_ltv_tier": "مستوى القيمة الدائمة",
        "ax_users": "المستخدمون", "ax_orders": "الطلبات", "ax_day": "اليوم",
        "ax_products": "منتج",
        "vendor_no_date_filter": "ℹ️ بيانات الموردين تعرض الأداء الإجمالي — لا تتأثر بفلتر التاريخ أعلاه.",
        "peak_day_tooltip": "أفضل يوم لإرسال الحملات",
        "peak_hour_tooltip": "ساعة الذروة — مثالية للإشعارات الفورية",
        "crosssell_all_title": "أبرز أزواج الفئات (الكل)",
        "crosssell_other_title": "فرص البيع التكميلي (باستثناء الشخصي)",
        "vendor_own_brand": "علامة ذاتية", "vendor_third_party": "طرف ثالث",
        "vendor_marketplace": "سوق", "vendor_charity": "جمعيات خيرية",
        "vendor_rev_split": "إيرادات المنصة حسب المصدر",
        "vendor_own_revenue": "إيرادات العلامة الذاتية",
        "vendor_marketplace_commission": "عمولة السوق المكتسبة",
        "vendor_charity_passthrough": "مبالغ الجمعيات الخيرية",
        "vendor_charity_note": "طلبات التبرع تُحوَّل مباشرةً للجمعيات — هذه الإيرادات لا تعود لذبحتي.",
        "page_health": "لمحة الأعمال",
        "vendor_gmv": "إجمالي حجم المعاملات (3 سنوات)",
        "vendor_direct_sales": "مبيعات ذبحتي المباشرة",
        "vendor_direct_note": "طلبات معالجة مباشرةً بواسطة ذبحتي (بدون متجر). مدرجة في GMV وليست ضمن تحليل المتاجر أدناه.",
        "vendor_to_review": "موردون يستحقون المراجعة",
        "vendor_to_review_note": "هؤلاء الموردون لديهم نشاط محدود أو درجة صحة منخفضة. يُنصح بالتواصل معهم أو إزالتهم من المنصة.",
        "bcg_growth_caveat": "📊 معدل النمو: آخر 30 يوماً مقارنةً بنفس الفترة من العام الماضي (وضع مقارنة سنوي نشط — نافذة الـ 30 يوماً السابقة تقع داخل فجوة البيانات ديسمبر 2025–فبراير 2026).",
        # Segment names
        "seg_champions": "الأبطال", "seg_loyal": "العملاء المخلصون",
        "seg_new": "العملاء الجدد", "seg_potential": "المخلصون المحتملون",
        "seg_promising": "الواعدون", "seg_at_risk": "في خطر",
        "seg_cant_lose": "لا يمكن خسارتهم", "seg_need_attention": "يحتاجون انتباهاً",
        "seg_about_sleep": "على وشك الانقطاع", "seg_lost": "مفقودون",
        # BCG / Churn labels
        "bcg_star": "نجم", "bcg_cow": "بقرة الحليب", "bcg_qm": "علامة استفهام", "bcg_dog": "كلب",
        "churn_critical": "حرج", "churn_high": "مرتفع", "churn_medium": "متوسط", "churn_low": "منخفض",
        # LTV tiers
        "tier_platinum": "بلاتيني", "tier_gold": "ذهبي", "tier_silver": "فضي", "tier_bronze": "برونزي",
        # Date presets
        "preset_label": "اختيار سريع:", "preset_last_month": "الشهر الماضي",
        "preset_last_quarter": "الربع الماضي", "preset_6months": "آخر 6 أشهر", "preset_last_year": "السنة الماضية",
        # Period banner
        "period_filtered": "📅 نتائج الفترة: {f} → {t}",
        "period_all": "📅 جميع البيانات: {f} → {t}",
        # Segment descriptions
        "seg_desc_champions": "أفضل المشترين — اشتروا مؤخراً وبكثرة وأنفقوا الأكثر.",
        "seg_desc_at_risk": "كانوا مخلصين لكنهم لم يطلبوا منذ فترة. عرض مستهدف قد يعيدهم.",
        "seg_desc_cant_lose": "مشترون بقيمة عالية يصمتون تدريجياً. أولوية قصوى لاستعادتهم.",
        "seg_desc_lost": "لم يطلبوا منذ وقت طويل جداً. يصعب استعادتهم — جرب حافزاً قوياً.",
        "seg_desc_new": "أجروا أول طلب مؤخراً. اعتنِ بهم لتحويلهم إلى مشترين متكررين.",
        "seg_desc_need_attention": "مشترون معتدلون يُظهرون علامات فتور مبكرة.",
        "seg_desc_loyal": "مشترون ثابتون عبر فئات متعددة. كافئ ولاءهم.",
        "seg_desc_potential": "يشترون بانتظام لكنهم لم يصلوا للولاء الكامل — دفعة إضافية واحدة تكفي.",
        "seg_desc_promising": "مشترون جدد بإشارات واعدة. مرشحون ممتازون للبيع الإضافي.",
        "seg_desc_about_sleep": "كانوا نشطين لكن طلباتهم تتباطأ — راقب هذه المجموعة.",
        # BCG descriptions
        "bcg_desc_star": "نمو سريع ومبيعات قوية. استثمر أكثر هنا.",
        "bcg_desc_cow": "مستقر ومربح. حافظ عليه.",
        "bcg_desc_qm": "إمكانات غير مثبتة. قرّر: انمِّه أو اسحبه.",
        "bcg_desc_dog": "نمو منخفض ومبيعات ضعيفة. فكّر في حذفه من الكتالوج.",
        # Churn descriptions
        "churn_desc_critical": "من المرجح جداً ألا يشتروا مجدداً. تصرّف الآن.",
        "churn_desc_high": "في خطر — لم يطلبوا منذ أشهر. أرسل عرض استعادة فوراً.",
        "churn_desc_medium": "علامات تحذير مبكرة. راقبهم وأرسل تذكيراً لطيفاً.",
        "churn_desc_low": "بصحة جيدة ونشطون. حافظ على رضاهم.",
        # Page intros
        "intro_segments": "عملاؤك ليسوا متساوين — هذا يوضح من هم أكثر العملاء قيمة ومن هو معرّض لمغادرة المنصة.",
        "intro_rfm": "كل نقطة تمثل عميلاً واحداً. النقاط لليسار = اشتروا مؤخراً. النقاط في الأعلى = أنفقوا الأكثر. اللون يُظهر مجموعة الولاء.",
        "intro_churn": "هؤلاء عملاء معرّضون للاختفاء. كلما صغر الشريط الأحمر، كان الاحتفاظ بالعملاء أفضل.",
        "intro_bcg": "فحص صحة سريع لكل منتج — هل ينمو؟ مستقر؟ أم يُثقل كاهل العمل؟",
        "intro_category": "الفئات التي تجلب أكثر الإيرادات.",
        "intro_vendors": "أداء كل مورد — الإيرادات والموثوقية ودرجة الصحة.",
        "intro_patterns": "متى يتسوق عملاؤك وما الذي يفضلون شراءه معاً. استخدم هذا لتوقيت حملاتك.",
        # Traffic-light messages
        "tl_champions": "✅ أبطالك في وضع ممتاز. كافئهم بعروض حصرية للحفاظ على ولائهم.",
        "tl_at_risk": "⚠️ هؤلاء كانوا مخلصين — عرض مستهدف هذا الأسبوع قد يعيدهم.",
        "tl_cant_lose": "🔴 عملاء عالي القيمة يصمتون. يحتاج هذا تواصلاً شخصياً عاجلاً.",
        "tl_lost_high": "⚠️ نسبة كبيرة من العملاء المفقودين تعني أن جهود الاحتفاظ السابقة غير كافية.",
        "tl_repeat_strong": "✅ نسبة تكرار شراء قوية — العملاء يعودون من تلقاء أنفسهم.",
        "tl_repeat_weak": "⚠️ معظم العملاء يشترون مرة واحدة فقط. برنامج ولاء أو تذكير يمكن أن يضاعف هذا.",
        "tl_churn_high": "🔴 أكثر من 1 من كل 5 عملاء معرّض للمغادرة — حملات الاحتفاظ عاجلة.",
        # Board report
        "translate_report_btn": "🔄 ترجمة التقرير إلى العربية",
        "translating": "جارٍ الترجمة... يستغرق حوالي 30 ثانية.",
        "translate_done": "✅ تمت الترجمة. التقرير جاهز بالعربية.",
        # Renamed labels
        "rfm_map": "خريطة ولاء العملاء",
        "churn_risk": "العملاء المعرّضون لمغادرة المنصة",
        "ltv_tier_dist": "مستويات قيمة العملاء",
        "page_patterns": "كيف يتسوق العملاء",
        "vendors_title": "أداء الموردين والمتاجر",
        "bcg_chart_title": "خريطة صحة المنتجات — مصفوفة BCG",
        # LTV tier guide
        "tier_guide_title": "📖 كيف تُحدَّد مستويات العملاء؟",
        "tier_guide_caption": "يُصنَّف كل عميل في مستوى بناءً على **إجمالي إنفاقه التراكمي** على ذبحتي (جميع طلباته منذ التسجيل، بالدرهم):",
        "tier_platinum_def": "🏆 **بلاتيني** — أنفق **5,000 درهم أو أكثر** إجمالاً. أعلى ~3% من العملاء. أعلى قيمة — يستحق اهتماماً استثنائياً.",
        "tier_gold_def":     "🥇 **ذهبي** — أنفق **2,000 – 4,999 درهم** إجمالاً. مشترون مخلصون بقيمة عالية. ~21% من العملاء.",
        "tier_silver_def":   "🥈 **فضي** — أنفق **500 – 1,999 درهم** إجمالاً. منفقون معتدلون بإمكانية للنمو. ~52% من العملاء.",
        "tier_bronze_def":   "🥉 **برونزي** — أنفق **أقل من 500 درهم** إجمالاً. مشترون جدد أو خفيفون. ~24% من العملاء.",
        "tier_guide_note":   "الحدود مبنية على الإنفاق التراكمي — ينتقل العميل لمستوى أعلى فور تجاوز إجمالي إنفاقه العتبة المحددة، بصرف النظر عن تواريخ الطلبات.",
        # Segment guide
        "seg_guide_title": "📖 تعريفات الشرائح",
        "seg_guide_caption": "كيف تُعرَّف كل شريحة ولاء وما الإجراء المقترح:",
        "seg_guide_champions":    "**الأبطال** — طلبوا مؤخراً (حداثة منخفضة)، بتكرار عالٍ، وإنفاق مرتفع. درجات RFM: R=4-5, F=4-5, M=4-5. هؤلاء أفضل عملائك.",
        "seg_guide_loyal":        "**العملاء المخلصون** — تكرار وقيمة نقدية مرتفعان مع حداثة معتدلة. يشترون بانتظام من فئات متعددة.",
        "seg_guide_potential":    "**المخلصون المحتملون** — مشترون جدد بتكرار معتدل. دفعة إضافية واحدة قد تحوّلهم إلى مخلصين.",
        "seg_guide_new":          "**العملاء الجدد** — أول طلب كان مؤخراً (آخر 30-60 يوماً). الحداثة جيدة لكن لا تاريخ بعد.",
        "seg_guide_promising":    "**الواعدون** — طلبوا مؤخراً بتكرار منخفض. إشارات جيدة — شجّعهم على الطلب الثاني.",
        "seg_guide_need_attn":    "**يحتاجون انتباهاً** — درجات RFM فوق المتوسط لكن بدأت تتراجع. لم يطلبوا منذ فترة.",
        "seg_guide_at_risk":      "**في خطر** — كانوا مخلصين وبدأوا بالابتعاد. الحداثة ترتفع بسرعة. عرض استعادة مستهدف مطلوب الآن.",
        "seg_guide_cant_lose":    "**لا يمكن خسارتهم** — عملاء عالي القيمة (أعلى إنفاق) لكن الحداثة مرتفعة جداً. تواصل شخصي عاجل مطلوب.",
        "seg_guide_about_sleep":  "**على وشك الانقطاع** — درجات معتدلة تتراجع. راقبهم وأرسل تذكيراً.",
        "seg_guide_lost":         "**مفقودون** — أدنى درجات الحداثة. لم يطلبوا منذ وقت طويل جداً. إعادة التنشيط صعبة.",
        # BCG explanation
        "bcg_explain_title": "📖 ما هي مصفوفة BCG؟",
        "bcg_explain_body": (
            "**مصفوفة BCG** (مجموعة بوسطن الاستشارية) تُصنّف كل منتج على محورين:\n\n"
            "- **المحور X — الحصة السوقية %:** نسبة إيرادات هذا المنتج من إجمالي إيرادات فئته. "
            "منتج بنسبة 80% يعني أنه يهيمن على فئته.\n"
            "- **المحور Y — درجة الحيوية:** مؤشر مركّب من حجم المبيعات الأخيرة والهامش. "
            "حيوية مرتفعة = يُباع بنشاط ومربح الآن.\n\n"
            "**المربعات الأربعة:**\n"
            "- ⭐ **نجم** (حصة مرتفعة + حيوية مرتفعة): أقوى منتجاتك. استثمر في التسويق والمخزون.\n"
            "- 🐄 **بقرة الحليب** (حصة مرتفعة + حيوية منخفضة): إيرادات موثوقة لكن النمو يتباطأ. حافظ عليها.\n"
            "- ❓ **علامة الاستفهام** (حصة منخفضة + حيوية مرتفعة): ينمو بسرعة لكن لم يتصدر بعد. قرّر: ادفع أكثر أو اقطع.\n"
            "- 🐕 **كلب** (حصة منخفضة + حيوية منخفضة): نمو ضعيف ومبيعات منخفضة. فكّر في حذفه.\n\n"
            "**حجم الفقاعة** = إجمالي الإيرادات. الخطوط المتقطعة هي الوسيط — المنتجات فوق وإلى اليمين أعلى من المتوسط."
        ),
        # Vendor column labels
        "vnd_col_shop": "المتجر", "vnd_col_type": "النوع", "vnd_col_shop_type": "فئة المتجر",
        "vnd_col_orders": "الطلبات", "vnd_col_revenue": "الإيرادات (درهم)",
        "vnd_col_aov": "متوسط الطلب (درهم)", "vnd_col_share": "نسبة الإيرادات %",
        "vnd_col_cancel": "معدل الإلغاء %", "vnd_col_health": "درجة الصحة",
        "vnd_col_comm_pct": "العمولة %", "vnd_col_comm_est": "العمولة المقدرة (درهم)",
        "vnd_reason_one": "طلب واحد فقط", "vnd_reason_few": "أقل من 5 طلبات",
        "vnd_reason_low_health": "درجة صحة منخفضة", "vnd_reason_test": "متجر تجريبي — يُحذف من قاعدة البيانات",
        "vnd_reason_low": "نشاط منخفض", "vnd_reason_col": "السبب",
        # GMV note
        "gmv_discrepancy_note": (
            "**لماذا تختلف أرقام GMV بين الصفحات؟**\n\n"
            "- **صفحة النظرة العامة (288 مليون درهم)** — مجموع `user_total_orders.total` لكل مستخدم. "
            "هذا الجدول **لا يفلتر بحالة الطلب** — يشمل جميع أنواع الطلبات "
            "(قيد الانتظار، ملغاة، مسلّمة). يُبالغ في تقدير الإيرادات الفعلية.\n\n"
            "- **صفحة الموردين / لمحة الأعمال (217 مليون درهم)** — فقط الطلبات بحالة `status=3` "
            "(مسلّم) و`payment_status='completed'`. "
            "هذا هو رقم **الإيرادات النقدية الفعلية** الصحيح.\n\n"
            "**الرقم الصحيح للإبلاغ لمجلس الإدارة هو 217 مليون درهم.** "
            "الفرق (~71 مليون درهم) يمثل طلبات ملغاة أو معلّقة أو غير مؤكدة لم يتم تحصيلها."
        ),
        # Health / Business Snapshot page
        "health_title": "📊 لمحة الأعمال",
        "health_subtitle": "نظرة واضحة على أداء الأعمال الآن. أرقام جوهرية بلا تعقيد.",
        "health_no_comparison": "لا توجد بيانات مقارنة",
        "health_growing": "نمو", "health_stable": "استقرار", "health_declining": "تراجع",
        "health_vs_lyst": "الإيرادات {pct}% مقارنةً بنفس الشهر العام الماضي",
        "health_revenue_month": "إيرادات هذا الشهر", "health_orders_month": "طلبات هذا الشهر",
        "health_customers_month": "عملاء هذا الشهر", "health_signups_month": "مشترون جدد (الشهر)",
        "health_vs_last_year": "مقارنةً بنفس الشهر العام الماضي",
        "health_gmv_total": "**إجمالي المنصة التراكمي (GMV):** درهم {gmv}  —  جميع الطلبات التي أُجريت على ذبحتي منذ الإطلاق.",
        "health_revenue_chart": "📈 الإيرادات الشهرية",
        "health_revenue_chart_note": "إجمالي الإيرادات لكل شهر. كلما ارتفع العمود، كان الأداء أفضل.",
        "health_gap_filled": "ℹ️ **فجوة البيانات ممتلئة بتقديرات موسمية:** طلبات {months} غير مسجّلة في قاعدة البيانات. الأعمدة العنبرية مقدَّرة باستخدام **نسبة نمو سنوي {factor}** مقارنةً بنفس الأشهر في العام السابق. البيانات الفعلية غير متاحة.",
        "health_partial_gap": "⚠️ **فجوة بيانات جزئية:** {months} لم يتمكن من التقدير (لا توجد بيانات أساسية للعام السابق).",
        "health_synth_banner": "📊 بيانات ديسمبر 2025 – فبراير 2026 مقدَّرة بالنموذج (توقع سنوي). مميَّزة بـ [تقدير] في الرسوم البيانية. بيانات قاعدة البيانات الفعلية تستأنف من مارس 2026.",
        "health_best_shops": "🏆 أفضل المتاجر أداءً", "health_best_shops_note": "أعلى المتاجر إيراداً في آخر 90 يوماً.",
        "health_best_products": "⭐ أكثر المنتجات مبيعاً", "health_best_products_note": "أعلى المنتجات إيراداً في آخر 90 يوماً.",
        "health_loyal_customers": "💎 أكثر العملاء وفاءً",
        "health_loyal_note": "العملاء الذين أنفقوا أكثر على ذبحتي على مدار عمرهم.",
        "health_insights": "💡 رؤى الأعمال", "health_alerts": "🚨 تنبيهات ونقاط مراقبة",
        "health_no_alerts": "✅ لا توجد تنبيهات حرجة الآن. الأعمال تبدو بصحة جيدة.",
        "health_col_shop": "المتجر", "health_col_orders": "الطلبات", "health_col_revenue": "الإيرادات (درهم)",
        "health_col_product": "المنتج", "health_col_category": "الفئة", "health_col_customer": "العميل",
        "health_col_total_orders": "إجمالي الطلبات", "health_col_lifetime": "الإنفاق الكلي", "health_col_last_order": "آخر طلب",
        "health_days_ago_ok": "✅ منذ {n} يوماً", "health_days_ago_warn": "⚠️ منذ {n} يوماً", "health_days_ago_bad": "🔴 منذ {n} يوماً",
        "health_estimated": "تقدير", "health_actual": "فعلي", "health_ax_month": "الشهر",
        "health_insight_repeat": "**تكرار الشراء:** {pct}% من المشترين طلبوا أكثر من مرة. {note}",
        "health_insight_repeat_ok": "هذا جيد لسوق طعام.",
        "health_insight_repeat_low": "هذا منخفض — ركّز على جعل المشترين لأول مرة يعودون.",
        "health_insight_concentration": "**تركّز الإيرادات:** أفضل 10 عملاء يمثلون {pct}% من إيرادات كل الوقت. {note}",
        "health_insight_conc_ok": "توزيع جيد.", "health_insight_conc_high": "اعتماد مرتفع — فكّر في تنويع قاعدة عملائك.",
        "health_insight_base": "**قاعدة العملاء:** {total:,} مشتر مسجّل — {champ:,} أبطال نشطون، {lost:,} أصبحوا صامتين.",
        "health_insight_peak": "**أيام الذروة:** الخميس والجمعة هما أكثر الأيام ازدحاماً. جدوِل حملاتك يوم الأربعاء لتعزيز الطلبات يوم الخميس.",
        "health_insight_direct": "**ذبحتي مقابل السوق:** {pct}% من الإيرادات تأتي من منتجات ذبحتي المباشرة (ليست متاجر خارجية).",
        "health_alert_onetime": "**{n:,} عميل** طلبوا مرة واحدة ولم يعودوا (منذ 90+ يوماً). هذه أكبر فرصة للاحتفاظ. رسالة متابعة بسيطة قد تستعيد الكثير منهم.",
        "health_alert_loyals": "**{n:,} عميل مخلص** (5+ طلبات لكل منهم) أصبحوا صامتين — لا طلبات منذ 90+ يوماً. هؤلاء أهم الأشخاص لإعادة إشراكهم فوراً.",
        "health_alert_critical": "**{n:,} عميل** في خطر فقدان حرج الآن. كانوا يشترون بانتظام ثم توقفوا. تصرّف خلال أسبوعين.",
        "health_alert_revenue": "**الإيرادات انخفضت {pct}%** مقارنةً بنفس الشهر العام الماضي. حقّق ما إذا كان هذا موسمياً أم علامة على مشكلة حقيقية.",
    }
}

# Tooltip content — EN and AR
HELP_TEXTS = {
    "en": {
        "ltv":          "Lifetime Value (LTV): Total revenue a customer has generated on the platform since their first order.",
        "rfm":          "RFM scores each buyer on 3 axes — Recency (days since last order), Frequency (categories purchased), Monetary (total spend in AED). Segments are derived from these scores.",
        "bcg":          "BCG Matrix: Stars = high growth + high share (invest). Cash Cows = stable + high share (maintain). Question Marks = high growth + low share (decide). Dogs = low on both (consider dropping).",
        "churn":        "Churn Risk: Likelihood a customer stops buying. Critical = very likely gone, High = at risk, Medium = warning signals, Low = healthy.",
        "recency":      "Recency: Days since the customer's last order. Lower is better — high recency means they're drifting away.",
        "frequency":    "Frequency: Number of distinct product categories purchased. More categories = deeper platform engagement.",
        "monetary":     "Monetary: Total amount (AED) spent across all orders — the 'M' in RFM.",
        "health_score": "Health Score (0–100): Composite vendor score based on order volume, cancellation rate, average order value, and shop rating.",
        "gmv":          "Total Lifetime Revenue: Cumulative spend by all users across all historical orders since the platform launched. This is not a single-period GMV figure.",
        "commission":   "Estimated Commission: Zabehaty's projected revenue from vendor sales, based on each shop's agreed commission percentage.",
        "repeat_rate":  "Multi-Category Rate: % of buyers who purchased from more than one product category. Note: this counts category breadth, not the number of separate orders.",
        "at_risk":      "At-Risk Users: Customers classified as Critical or High churn risk. Haven't ordered recently and need targeted re-engagement.",
        "revenue_share":"Revenue Share %: This segment's or vendor's contribution to total platform revenue.",
        "cancel_rate":  "Cancellation Rate %: % of this vendor's orders that were cancelled. High rates signal fulfilment or quality issues.",
        "ltv_tier":     "LTV Tiers: Customers grouped by total lifetime spend. Platinum ≥ AED 5,000 | Gold AED 2,000–4,999 | Silver AED 500–1,999 | Bronze < AED 500. A customer moves up automatically when their cumulative spend crosses the threshold.",
        "market_share": "Market Share %: This product's share of revenue within its own category.",
        "vitality":     "Vitality Score: Combines recent order count and margin. High vitality = actively selling and profitable right now.",
        "peak_timing":  "Peak timing shows which days and hours have the most orders. Use this to schedule push notifications and campaigns.",
        "payment":      "Payment method distribution. Card dominance signals a digital-first base. BNPL (Tamara/Tabby) share is worth monitoring.",
        "cross_sell":   "Category pairs bought by the same customer. High co-buyer counts = strong cross-sell opportunity.",
        "health_rev_month":   "Total revenue from completed orders placed this calendar month.",
        "health_ord_month":   "Count of completed orders placed this calendar month.",
        "health_cust_month":  "Count of unique customers who placed at least one order this calendar month.",
        "health_signups":     "Users who placed their very first completed order this calendar month. This is first-time buyers, not account registrations.",
        "health_gmv_info":    "Platform GMV = revenue from shops with a shop_id + Zabehaty direct orders (no shop). Source: shop_rankings.json generated by shop_analysis.py.",
    },
    "ar": {
        "ltv":          "القيمة الدائمة للعميل: إجمالي الإيرادات التي ولّدها العميل على المنصة منذ أول طلب له.",
        "rfm":          "يقيس كل مشترٍ على ثلاثة محاور — الحداثة (أيام منذ آخر طلب)، التكرار (الفئات المشتراة)، القيمة النقدية (إجمالي الإنفاق). تُشتق شرائح العملاء من هذه الدرجات.",
        "bcg":          "مصفوفة BCG: النجوم = نمو مرتفع + حصة مرتفعة (استثمر). بقرة الحليب = مستقر + حصة مرتفعة (حافظ). علامة الاستفهام = نمو مرتفع + حصة منخفضة (قرر). الكلب = منخفض في الاثنين (فكر في الإزالة).",
        "churn":        "مخاطر الانقطاع: احتمال توقف العميل عن الشراء. حرج = غادر على الأرجح، مرتفع = في خطر، متوسط = إشارات تحذير، منخفض = بصحة جيدة.",
        "recency":      "الحداثة: أيام منذ آخر طلب للعميل. كلما قل الرقم كان أفضل — الأرقام العالية تعني أن العميل يبتعد.",
        "frequency":    "التكرار: عدد الفئات المختلفة التي اشترى منها العميل. المزيد من الفئات = تفاعل أعمق مع المنصة.",
        "monetary":     "القيمة النقدية: إجمالي المبالغ المنفقة (بالدرهم) عبر جميع الطلبات — حرف 'M' في RFM.",
        "health_score": "درجة الصحة (0-100): درجة مركبة للمورد بناءً على حجم الطلبات ومعدل الإلغاء ومتوسط قيمة الطلب والتقييم.",
        "gmv":          "إجمالي الإيرادات التراكمية: مجموع إنفاق جميع المستخدمين عبر كل الطلبات التاريخية منذ إطلاق المنصة. ليس رقم GMV لفترة واحدة.",
        "commission":   "العمولة المقدرة: الإيرادات المتوقعة لذبحتي من مبيعات الموردين بناءً على نسبة العمولة المتفق عليها لكل متجر.",
        "repeat_rate":  "معدل تعدد الفئات: نسبة المشترين الذين اشتروا من أكثر من فئة منتجات. ملاحظة: يقيس تنوع الفئات وليس عدد الطلبات المستقلة.",
        "at_risk":      "المستخدمون المعرضون للخطر: العملاء المصنفون ضمن خطر الانقطاع الحرج أو المرتفع. لم يطلبوا مؤخراً ويحتاجون حملة إعادة استهداف.",
        "revenue_share":"نسبة الإيرادات: مساهمة هذه الشريحة أو المورد في إجمالي إيرادات المنصة.",
        "cancel_rate":  "معدل الإلغاء: نسبة طلبات هذا المورد التي تم إلغاؤها. المعدلات المرتفعة تشير إلى مشاكل في التوصيل أو جودة المنتج.",
        "ltv_tier":     "مستويات القيمة الدائمة: تصنيف العملاء حسب إجمالي الإنفاق التراكمي. بلاتيني ≥ 5,000 درهم | ذهبي 2,000–4,999 | فضي 500–1,999 | برونزي < 500 درهم. ينتقل العميل تلقائياً لمستوى أعلى عند تجاوز إنفاقه التراكمي العتبة المحددة.",
        "market_share": "الحصة السوقية: نسبة إيرادات هذا المنتج من إجمالي إيرادات فئته.",
        "vitality":     "درجة الحيوية: تجمع بين حجم الطلبات الأخيرة والهامش. حيوية مرتفعة = يُباع بنشاط ومربح الآن.",
        "peak_timing":  "أوقات الذروة توضح الأيام والساعات التي تشهد أعلى حجم طلبات. استخدم هذا لجدولة الإشعارات والحملات التسويقية.",
        "payment":      "توزيع طرق الدفع. هيمنة البطاقة تشير إلى قاعدة عملاء رقمية. مراقبة حصة BNPL (تمارة/تابي) مهمة لإدارة مخاطر الائتمان.",
        "cross_sell":   "أزواج الفئات التي يشتريها نفس العميل. ارتفاع عدد المشترين المشتركين = فرصة بيع تكميلي قوية.",
        "health_rev_month":   "إجمالي الإيرادات من الطلبات المكتملة المُسجَّلة في هذا الشهر التقويمي.",
        "health_ord_month":   "عدد الطلبات المكتملة المُسجَّلة في هذا الشهر التقويمي.",
        "health_cust_month":  "عدد العملاء الفريدين الذين أجروا طلباً واحداً على الأقل في هذا الشهر التقويمي.",
        "health_signups":     "المستخدمون الذين أجروا أول طلب مكتمل لهم في هذا الشهر التقويمي. هذا يقيس المشترين الجدد الفعليين، لا مجرد تسجيلات الحساب.",
        "health_gmv_info":    "إجمالي حجم المعاملات = إيرادات المتاجر المرتبطة + الطلبات المباشرة لذبحتي. المصدر: shop_rankings.json المُولَّد من shop_analysis.py.",
    },
}

def t(key):
    lang = st.session_state.get("lang", "en")
    return STRINGS[lang].get(key, STRINGS["en"].get(key, key))

def h(key):
    lang = st.session_state.get("lang", "en")
    return HELP_TEXTS[lang].get(key, HELP_TEXTS["en"].get(key, ""))

# ── Segment/BCG/Churn label maps ──────────────────────────────────────────────
SEG_KEY_MAP = {
    "Champions": "seg_champions", "Loyal Customers": "seg_loyal",
    "New Customers": "seg_new", "Potential Loyalists": "seg_potential",
    "Promising": "seg_promising", "At Risk": "seg_at_risk",
    "Cant Lose Them": "seg_cant_lose", "Need Attention": "seg_need_attention",
    "About to Sleep": "seg_about_sleep", "Lost": "seg_lost",
}
BCG_KEY_MAP   = {"Star": "bcg_star", "Cash Cow": "bcg_cow", "Question Mark": "bcg_qm", "Dog": "bcg_dog"}
CHURN_KEY_MAP = {"Critical": "churn_critical", "High": "churn_high", "Medium": "churn_medium", "Low": "churn_low"}
TIER_KEY_MAP  = {"Platinum": "tier_platinum", "Gold": "tier_gold", "Silver": "tier_silver", "Bronze": "tier_bronze"}

SEG_DESC_KEY_MAP = {
    "Champions": "seg_desc_champions", "Loyal Customers": "seg_desc_loyal",
    "New Customers": "seg_desc_new", "Potential Loyalists": "seg_desc_potential",
    "Promising": "seg_desc_promising", "At Risk": "seg_desc_at_risk",
    "Cant Lose Them": "seg_desc_cant_lose", "Need Attention": "seg_desc_need_attention",
    "About to Sleep": "seg_desc_about_sleep", "Lost": "seg_desc_lost",
}
BCG_DESC_KEY_MAP  = {"Star": "bcg_desc_star", "Cash Cow": "bcg_desc_cow",
                     "Question Mark": "bcg_desc_qm", "Dog": "bcg_desc_dog"}
CHURN_DESC_KEY_MAP = {"Critical": "churn_desc_critical", "High": "churn_desc_high",
                      "Medium": "churn_desc_medium", "Low": "churn_desc_low"}

def label_map(key_map):
    """Return {en_label: translated_label} dict for the current language."""
    return {k: t(v) for k, v in key_map.items()}

def translate_col(df, col, key_map, new_col=None):
    """Add a display column with translated labels. Returns (df, new_col_name)."""
    out_col = new_col or f"{col}_display"
    mapping = label_map(key_map)
    df = df.copy()
    df[out_col] = df[col].map(mapping).fillna(df[col])
    return df, out_col

def translate_color_map(key_map, color_map):
    """Remap color_map keys to translated labels."""
    lm = label_map(key_map)
    return {lm.get(k, k): v for k, v in color_map.items()}

SEGMENT_COLORS = {
    "Champions": "#22c55e", "Loyal Customers": "#86efac", "New Customers": "#60a5fa",
    "Potential Loyalists": "#93c5fd", "Promising": "#bfdbfe", "At Risk": "#f97316",
    "Cant Lose Them": "#ef4444", "Need Attention": "#facc15",
    "About to Sleep": "#d1d5db", "Lost": "#6b7280",
}
BCG_COLORS   = {"Star": "#f59e0b", "Cash Cow": "#22c55e", "Question Mark": "#3b82f6", "Dog": "#ef4444"}
CHURN_COLORS = {"Critical": "#ef4444", "High": "#f97316", "Medium": "#facc15", "Low": "#22c55e"}
TIER_COLORS  = {"Platinum": "#a855f7", "Gold": "#f59e0b", "Silver": "#94a3b8", "Bronze": "#b45309"}

# ── Helpers ───────────────────────────────────────────────────────────────────
def section(title, help_key=None):
    if help_key:
        c1, c2 = st.columns([20, 1])
        c1.subheader(title)
        with c2:
            with st.popover("ℹ️"):
                st.write(h(help_key))
    else:
        st.subheader(title)

def safe_df(df, subset=None, cmap="Greens"):
    if HAS_MATPLOTLIB and subset:
        avail = [c for c in subset if c in df.columns]
        if avail:
            try:
                return df.style.background_gradient(subset=avail, cmap=cmap)
            except Exception:
                pass
    return df

def show_period_banner(from_date, to_date, mn, mx):
    """Show which date range is currently active, at the top of every page."""
    if from_date is None or mn is None: return
    fmt = "%d %b %Y"
    f_str = from_date.strftime(fmt)
    t_str = to_date.strftime(fmt)
    if from_date != mn or to_date != mx:
        st.info(t("period_filtered").format(f=f_str, t=t_str))
    else:
        st.caption(t("period_all").format(f=f_str, t=t_str))

def show_filter_not_applied(from_date, to_date, mn, mx):
    """Show a note on pages where date filter is set but has no effect."""
    if from_date is None or mn is None: return
    if from_date != mn or to_date != mx:
        fmt = "%d %b %Y"
        st.caption(t("date_filter_no_effect").format(
            f=from_date.strftime(fmt), t=to_date.strftime(fmt)))

def seg_desc(segment_en):
    """Return plain-language description for a segment name (English key)."""
    key = SEG_DESC_KEY_MAP.get(segment_en, "")
    return t(key) if key else ""

def bcg_desc(quadrant_en):
    key = BCG_DESC_KEY_MAP.get(quadrant_en, "")
    return t(key) if key else ""

def churn_desc(level_en):
    key = CHURN_DESC_KEY_MAP.get(level_en, "")
    return t(key) if key else ""

# ── CSS: font + layout + sidebar spacing ─────────────────────────────────────
def inject_css():
    lang = st.session_state.get("lang", "en")
    if lang == "ar":
        st.markdown("""
        <link href="https://fonts.googleapis.com/css2?family=Tajawal:wght@300;400;500;700&display=swap" rel="stylesheet">
        <link href="https://fonts.googleapis.com/icon?family=Material+Icons" rel="stylesheet">
        <style>
        /* Apply Tajawal font everywhere except icon fonts */
        body, .stApp, .stMarkdown, p, h1, h2, h3, h4, label, div,
        [data-testid="stSidebar"] * { font-family: 'Tajawal', sans-serif !important; }
        /* Exclude span from global font override — icons live inside spans */
        span { font-family: inherit; }
        /* Restore Tajawal for text-bearing spans */
        p span, .stMarkdown span, h1 span, h2 span, h3 span, label span,
        [data-testid="stMetricValue"] span,
        [data-testid="stMetricLabel"] span { font-family: 'Tajawal', sans-serif !important; }
        /* Material Icons — must override everything else */
        .material-icons, [class*="material-icons"],
        [data-testid="stIconMaterial"] {
            font-family: 'Material Icons' !important;
            font-size: 18px !important;
            font-style: normal !important;
            font-weight: normal !important;
            letter-spacing: normal !important;
            text-transform: none !important;
            word-wrap: normal !important;
            direction: ltr !important;
            display: inline-block !important;
            -webkit-font-feature-settings: 'liga' !important;
            font-feature-settings: 'liga' !important;
        }
        /* RTL for content */
        body, .stApp, .stMarkdown, p, h1, h2, h3, label,
        [data-testid="stSidebar"], [data-testid="stVerticalBlock"] {
            direction: rtl !important; text-align: right !important;
        }
        /* Keep dropdowns/selects LTR so icon doesn't break */
        .stSelectbox svg, .stMultiSelect svg,
        [data-testid="stSelectbox"] svg { direction: ltr !important; }
        /* Fix expand_more icon in selectbox — keep it LTR */
        [data-testid="stSelectbox"] > div > div { direction: ltr !important; }
        [data-testid="stSelectbox"] > div > div > div { direction: rtl !important; text-align: right !important; }
        /* Sidebar nav spacing — compact */
        [data-testid="stSidebar"] .stRadio > div { gap: 2px !important; }
        [data-testid="stSidebar"] .stRadio label { padding: 4px 8px !important; border-radius: 6px; margin: 0 !important; }
        [data-testid="stSidebar"] .stRadio [data-testid="stWidgetLabel"] {
            font-size: 13px !important; font-weight: 700 !important; margin-bottom: 2px !important;
        }
        /* AI Analyst — highlight first nav item */
        [data-testid="stSidebar"] .stRadio label:first-of-type {
            background: linear-gradient(90deg, #c0392b18, #c0392b08) !important;
            border: 1px solid #c0392b55 !important;
            font-weight: 700 !important;
            color: #c0392b !important;
        }
        /* ── Mobile (Arabic) ── */
        @media (max-width: 768px) {
            .block-container { padding: 0.75rem 0.5rem !important; max-width: 100vw !important; overflow-x: hidden !important; }
            .stApp, .main, .main > div { overflow-x: hidden !important; max-width: 100vw !important; }
            /* Sidebar: RTL direction must not break the overlay — force LTR positioning */
            section[data-testid="stSidebar"] {
                position: fixed !important; z-index: 999 !important;
                width: 85vw !important; max-width: 320px !important;
                direction: ltr !important; left: 0 !important; right: auto !important;
                height: 100vh !important; overflow-y: auto !important;
                -webkit-overflow-scrolling: touch !important;
            }
            /* Inner sidebar content stays RTL and scrollable */
            section[data-testid="stSidebar"] > div {
                direction: rtl !important;
                height: 100% !important; overflow-y: auto !important;
            }
            /* Main content area must not be offset by sidebar */
            .main .block-container { margin-left: 0 !important; margin-right: 0 !important; }
            [data-testid="stDataFrame"], [data-testid="stDataFrameResizable"], .stDataFrame, .stTable { overflow-x: auto !important; max-width: 100% !important; font-size: 12px !important; }
            [data-testid="stDataFrame"] table, .stDataFrame table { min-width: unset !important; }
            .js-plotly-plot, .plotly, [data-testid="stPlotlyChart"] { max-width: 100% !important; overflow: hidden !important; }
            [data-testid="stHorizontalBlock"] { flex-wrap: wrap !important; gap: 8px !important; }
            [data-testid="stHorizontalBlock"] > [data-testid="stVerticalBlock"] { min-width: calc(50% - 8px) !important; flex: 1 1 calc(50% - 8px) !important; }
            h1 { font-size: 1.4rem !important; } h2 { font-size: 1.2rem !important; } h3 { font-size: 1rem !important; }
        }
        </style>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <link href="https://fonts.googleapis.com/icon?family=Material+Icons" rel="stylesheet">
        <style>
        [data-testid="stIconMaterial"] {
            font-family: 'Material Icons' !important;
            font-style: normal !important; font-weight: normal !important;
            direction: ltr !important; display: inline-block !important;
            -webkit-font-feature-settings: 'liga' !important;
            font-feature-settings: 'liga' !important;
        }
        [data-testid="stSidebar"] .stRadio > div { gap: 2px !important; }
        [data-testid="stSidebar"] .stRadio label { padding: 4px 8px !important; border-radius: 6px; margin: 0 !important; }
        [data-testid="stSidebar"] .stRadio [data-testid="stWidgetLabel"] {
            font-size: 13px !important; font-weight: 700 !important; margin-bottom: 2px !important;
        }
        /* AI Analyst — highlight first nav item */
        [data-testid="stSidebar"] .stRadio label:first-of-type {
            background: linear-gradient(90deg, #c0392b18, #c0392b08) !important;
            border: 1px solid #c0392b55 !important;
            font-weight: 700 !important;
            color: #c0392b !important;
        }
        /* ── Mobile responsiveness ── */
        @media (max-width: 768px) {
            /* Main content: full width, no side padding waste */
            .block-container {
                padding: 0.75rem 0.5rem !important;
                max-width: 100vw !important;
                width: 100% !important;
                overflow-x: hidden !important;
            }
            /* Prevent any element from causing horizontal scroll */
            .stApp, .main, .main > div {
                overflow-x: hidden !important;
                max-width: 100vw !important;
            }
            /* Sidebar: overlay mode — never bleeds into content, fully scrollable */
            section[data-testid="stSidebar"] {
                position: fixed !important;
                z-index: 999 !important;
                width: 85vw !important;
                max-width: 320px !important;
                height: 100vh !important;
                overflow-y: auto !important;
                -webkit-overflow-scrolling: touch !important;
            }
            section[data-testid="stSidebar"] > div {
                height: 100% !important;
                overflow-y: auto !important;
            }
            /* Tables: horizontal scroll within container, don't expand page */
            [data-testid="stDataFrame"],
            [data-testid="stDataFrameResizable"],
            .stDataFrame, .stTable {
                overflow-x: auto !important;
                max-width: 100% !important;
                font-size: 12px !important;
            }
            [data-testid="stDataFrame"] table,
            .stDataFrame table {
                min-width: unset !important;
            }
            /* Plotly charts */
            .js-plotly-plot, .plotly, [data-testid="stPlotlyChart"] {
                max-width: 100% !important;
                overflow: hidden !important;
            }
            /* Metric cards: wrap into 2 columns on mobile */
            [data-testid="stHorizontalBlock"] {
                flex-wrap: wrap !important;
                gap: 8px !important;
            }
            [data-testid="stHorizontalBlock"] > [data-testid="stVerticalBlock"] {
                min-width: calc(50% - 8px) !important;
                flex: 1 1 calc(50% - 8px) !important;
            }
            /* Chat input: account for keyboard on mobile */
            .stChatInput { position: sticky !important; bottom: 0 !important; }
            /* Prevent iframe overflow */
            iframe { max-width: 100% !important; }
            /* Smaller headings on mobile */
            h1 { font-size: 1.4rem !important; }
            h2 { font-size: 1.2rem !important; }
            h3 { font-size: 1rem !important; }
        }
        </style>
        <script>
        // Mobile: auto-close sidebar after nav item click
        (function() {
            function attachSidebarClose() {
                if (window.innerWidth > 768) return;
                var labels = document.querySelectorAll('[data-testid="stSidebar"] .stRadio label');
                labels.forEach(function(label) {
                    if (label._mobileCB) return; // already attached
                    label._mobileCB = true;
                    label.addEventListener('click', function() {
                        setTimeout(function() {
                            // Try the collapse button (sidebar header X / chevron)
                            var btn = document.querySelector('[data-testid="stSidebar"] button')
                                   || document.querySelector('[data-testid="collapsedControl"]');
                            if (btn) btn.click();
                        }, 250);
                    });
                });
            }
            // Run on load and after every Streamlit rerender
            var observer = new MutationObserver(attachSidebarClose);
            observer.observe(document.body, { childList: true, subtree: true });
            attachSidebarClose();
        })();
        </script>
        """, unsafe_allow_html=True)

# ── Data loading ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_csv(name):
    path = os.path.join(TMP, name)
    return pd.read_csv(path) if os.path.exists(path) else pd.DataFrame()

@st.cache_data(ttl=3600)
def load_json(name):
    path = os.path.join(TMP, name)
    if not os.path.exists(path): return {}
    with open(path, encoding="utf-8") as f: return json.load(f)

@st.cache_data(ttl=1800)
def load_health_data():
    """Live DB queries for the Business Snapshot page."""
    if not _DB_AVAILABLE:
        return None
    try:
        mom = _db_query("""
            SELECT DATE_FORMAT(created_at,'%Y-%m') AS month,
                   COUNT(DISTINCT id) AS orders, SUM(total) AS revenue,
                   COUNT(DISTINCT user_id) AS customers
            FROM orders WHERE status=3 AND payment_status='completed'
              AND created_at >= DATE_SUB(CURDATE(), INTERVAL 13 MONTH)
            GROUP BY DATE_FORMAT(created_at,'%Y-%m') ORDER BY month
        """)
        # Compare current month vs same month last year (avoids data gaps in recent months)
        comparison = _db_query("""
            SELECT
                CASE WHEN DATE_FORMAT(created_at,'%Y-%m')=DATE_FORMAT(CURDATE(),'%Y-%m') THEN 'this_month'
                     WHEN DATE_FORMAT(created_at,'%Y-%m')=DATE_FORMAT(DATE_SUB(CURDATE(),INTERVAL 12 MONTH),'%Y-%m') THEN 'last_year_same_month'
                END AS period,
                COUNT(DISTINCT id) AS orders, SUM(total) AS revenue,
                COUNT(DISTINCT user_id) AS customers
            FROM orders WHERE status=3 AND payment_status='completed'
              AND (DATE_FORMAT(created_at,'%Y-%m')=DATE_FORMAT(CURDATE(),'%Y-%m')
                OR DATE_FORMAT(created_at,'%Y-%m')=DATE_FORMAT(DATE_SUB(CURDATE(),INTERVAL 12 MONTH),'%Y-%m'))
            GROUP BY period HAVING period IS NOT NULL
        """)
        top_shops = _db_query("""
            SELECT s.name_en AS shop, s.name AS shop_ar,
                   COUNT(DISTINCT o.id) AS orders,
                   SUM(o.total) AS revenue,
                   AVG(CASE WHEN o.rating>0 THEN o.rating END) AS avg_rating
            FROM orders o JOIN shops s ON s.id=o.shop_id
            WHERE o.status=3 AND o.payment_status='completed'
              AND o.created_at >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
            GROUP BY s.id, s.name_en, s.name ORDER BY revenue DESC LIMIT 8
        """)
        # Group by product name + category to avoid duplicate rows for same product across shops
        top_products = _db_query("""
            SELECT p.name_en AS product, p.name AS product_ar,
                   c.name_en AS category, c.name AS category_ar,
                   COUNT(DISTINCT o.id) AS orders,
                   SUM(od.price * od.quantity) AS revenue
            FROM orders o
            JOIN order_details od ON od.order_id=o.id
            JOIN products p ON p.id=od.product_id
            JOIN categories c ON c.id=p.category_id
            WHERE o.status=3 AND o.payment_status='completed'
              AND o.created_at >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
              AND p.deleted_at IS NULL AND p.price>0
            GROUP BY p.name_en, p.name, c.name_en, c.name ORDER BY revenue DESC LIMIT 10
        """)
        top_customers = _db_query("""
            SELECT u.id AS user_id,
                   CONCAT('Customer #', u.id) AS customer,
                   COALESCE(NULLIF(TRIM(u.first_name),''), '—') AS first_name,
                   COUNT(DISTINCT o.id) AS total_orders,
                   SUM(o.total) AS lifetime_value,
                   DATEDIFF(NOW(), MAX(o.created_at)) AS days_since_last_order
            FROM orders o JOIN `user` u ON u.id=o.user_id
            WHERE o.status=3 AND o.payment_status='completed'
            GROUP BY u.id, u.first_name ORDER BY lifetime_value DESC LIMIT 10
        """)
        one_time = _db_query("""
            SELECT COUNT(*) AS n FROM (
                SELECT user_id FROM orders WHERE status=3 AND payment_status='completed'
                GROUP BY user_id
                HAVING COUNT(*)=1 AND MAX(created_at)<DATE_SUB(NOW(), INTERVAL 90 DAY)
            ) x
        """)
        stopped = _db_query("""
            SELECT COUNT(*) AS n FROM (
                SELECT user_id FROM orders WHERE status=3 AND payment_status='completed'
                GROUP BY user_id
                HAVING COUNT(*)>=5 AND MAX(created_at)<DATE_SUB(NOW(), INTERVAL 90 DAY)
            ) x
        """)
        new_users = _db_query("""
            SELECT DATE_FORMAT(first_order, '%Y-%m') AS month, COUNT(*) AS new_users
            FROM (
                SELECT user_id, MIN(created_at) AS first_order
                FROM orders
                WHERE status = 3
                  AND payment_status = 'completed'
                  AND user_id IN (SELECT id FROM `user` WHERE is_ban=0 OR is_ban IS NULL)
                GROUP BY user_id
                HAVING first_order >= DATE_SUB(CURDATE(), INTERVAL 2 MONTH)
            ) first_orders
            GROUP BY DATE_FORMAT(first_order, '%Y-%m')
            ORDER BY month
        """)
        # Baseline data for gap-fill estimation (same months 1 year ago)
        gap_baseline = _db_query("""
            SELECT DATE_FORMAT(created_at,'%Y-%m') AS month,
                   COUNT(DISTINCT id) AS orders, SUM(total) AS revenue,
                   COUNT(DISTINCT user_id) AS customers
            FROM orders WHERE status=3 AND payment_status='completed'
              AND DATE_FORMAT(created_at,'%Y-%m') IN (
                  '2024-10','2024-11','2024-12','2025-01','2025-02'
              )
            GROUP BY month ORDER BY month
        """)
        return {
            "mom": mom, "comparison": comparison,
            "top_shops": top_shops, "top_products": top_products,
            "top_customers": top_customers,
            "one_time_buyers": int(one_time.iloc[0]["n"]) if not one_time.empty else 0,
            "stopped_loyals": int(stopped.iloc[0]["n"]) if not stopped.empty else 0,
            "new_users": new_users,
            "gap_baseline": gap_baseline,
        }
    except Exception as e:
        return {"error": str(e)}

@st.cache_data(ttl=3600)
def rfm_date_bounds():
    df = load_csv("rfm_scores.csv")
    if df.empty or "last_order_date" not in df.columns: return None, None
    dates = pd.to_datetime(df["last_order_date"], errors="coerce").dropna()
    return (dates.min().date(), dates.max().date()) if not dates.empty else (None, None)

def date_filter(df, from_d, to_d):
    if from_d is None or "last_order_date" not in df.columns: return df
    dates = pd.to_datetime(df["last_order_date"], errors="coerce")
    return df[(dates.dt.date >= from_d) & (dates.dt.date <= to_d)]

# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
inject_css()

_logo_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Zabehaty Logo 1.svg")
if os.path.exists(_logo_path):
    st.sidebar.image(_logo_path, width=160)
else:
    st.sidebar.markdown("### 🥩")
st.sidebar.title(t("app_title"))
st.sidebar.caption(t("subtitle"))

# Language — must come before page navigation
lang_pick = st.sidebar.radio(t("language"), ["English", "العربية"],
                             horizontal=True, key="lang_radio")
new_lang = "ar" if lang_pick == "العربية" else "en"
if new_lang != st.session_state.lang:
    st.session_state.lang = new_lang
    st.rerun()

inject_css()
st.sidebar.divider()

# Page navigation — index-based so it survives language switches
# Agent is first so it's the default landing page and easiest to reach
PAGE_KEYS = ["agent", "health", "overview", "segments", "products", "vendors", "patterns", "report"]
page_opts = [t(f"page_{k}") for k in PAGE_KEYS]
# Guard: clamp page_idx in case session state is stale from an older page list
if st.session_state.page_idx >= len(PAGE_KEYS):
    st.session_state.page_idx = 0
page_label = st.sidebar.radio(
    t("navigation"), page_opts,
    index=st.session_state.page_idx,
    key="page_nav",
)
new_idx = page_opts.index(page_label)
st.session_state.page_idx = new_idx
page_key = PAGE_KEYS[new_idx]

# Date range filter — two explicit From / To pickers
st.sidebar.divider()
st.sidebar.markdown(f"**{t('date_filter')}**")
st.sidebar.caption(t("date_filter_info"))
mn, mx = rfm_date_bounds()
from_date = to_date = None
is_filtered = False
if mn and mx:
    # Apply staged preset BEFORE widgets are instantiated (Streamlit requires this order)
    if st.session_state.get("_apply_preset"):
        st.session_state["date_from"] = st.session_state.get("_preset_from", mn)
        st.session_state["date_to"]   = st.session_state.get("_preset_to",   mx)
        del st.session_state["_apply_preset"]

    from_date = st.sidebar.date_input(t("date_from"), value=mn, min_value=mn, max_value=mx, key="date_from")
    to_date   = st.sidebar.date_input(t("date_to"),   value=mx, min_value=mn, max_value=mx, key="date_to")
    if from_date > to_date:
        from_date, to_date = to_date, from_date
    is_filtered = (from_date != mn or to_date != mx)
    # Quick-select preset buttons
    st.sidebar.caption(t("preset_label"))
    if relativedelta is not None:
        _presets = [
            ("preset_last_month",   relativedelta(months=1)),
            ("preset_last_quarter", relativedelta(months=3)),
            ("preset_6months",      relativedelta(months=6)),
            ("preset_last_year",    relativedelta(years=1)),
        ]
        _p_cols = st.sidebar.columns(2)
        for _i, (_key, _delta) in enumerate(_presets):
            _preset_from = max(mn, (mx - _delta))
            if _p_cols[_i % 2].button(t(_key), key=f"btn_{_key}", use_container_width=True):
                # Stage the values — applied before widgets on next rerun
                st.session_state["_preset_from"]  = _preset_from
                st.session_state["_preset_to"]    = mx
                st.session_state["_apply_preset"] = True
                st.rerun()
else:
    st.sidebar.caption(t("no_data"))

st.sidebar.divider()
if st.sidebar.button(t("refresh")):
    st.cache_data.clear()
    st.rerun()
st.sidebar.caption(t("data_source"))

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════
# ── Lazy pipeline — only run when a data page is accessed, not on agent ───────
if page_key != "agent" and not st.session_state.pipeline_ready:
    with st.spinner("⏳ Loading analysis data for the first time — about 60 seconds…"):
        try:
            _run_pipeline()
            st.session_state.pipeline_ready = True
        except Exception as _e:
            st.error(f"Pipeline failed: {_e}. Check DB credentials.")
            st.stop()

if page_key == "overview":
    st.title(t("overview_title"))
    show_period_banner(from_date, to_date, mn, mx)

    df_rfm   = load_csv("rfm_scores.csv")
    df_ltv   = load_csv("ltv_analysis.csv")
    patterns = load_json("buying_patterns.json")
    shop_r   = load_json("shop_rankings.json")

    if df_rfm.empty:
        st.warning(t("no_data")); st.stop()

    _total_before = len(df_rfm)
    if is_filtered:
        df_rfm = date_filter(df_rfm, from_date, to_date)
        if not df_ltv.empty and "user_id" in df_ltv.columns:
            df_ltv = df_ltv[df_ltv["user_id"].isin(df_rfm["user_id"])]
        _pct = len(df_rfm) / _total_before * 100 if _total_before else 0
        st.info(t("date_filter_count").format(n=len(df_rfm), total=_total_before, pct=_pct))

    total_users = len(df_rfm)
    ltv_src     = df_ltv if not df_ltv.empty and "monetary" in df_ltv.columns else df_rfm
    total_ltv   = ltv_src["monetary"].sum()
    avg_ltv     = total_ltv / total_users if total_users else 0

    # Repeat rate — computed from filtered df_rfm (frequency > 1 category)
    if "frequency" in df_rfm.columns and total_users > 0:
        repeat_rate = round(len(df_rfm[df_rfm["frequency"] > 1]) / total_users * 100, 1)
    else:
        repeat_rate = patterns.get("repeat_purchase", {}).get("repeat_rate_pct", 0)

    # At-risk users — join churn_risk.csv against the filtered user set
    df_churn_ov = load_csv("churn_risk.csv")
    if not df_churn_ov.empty and "user_id" in df_churn_ov.columns and "churn_risk_label" in df_churn_ov.columns:
        if is_filtered:
            df_churn_ov = df_churn_ov[df_churn_ov["user_id"].isin(df_rfm["user_id"])]
        at_risk_n = len(df_churn_ov[df_churn_ov["churn_risk_label"].isin(["Critical", "High"])])
    else:
        churn_dist = patterns.get("churn_risk_distribution", {})
        at_risk_n  = churn_dist.get("Critical", 0) + churn_dist.get("High", 0)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric(t("active_buyers"), f"{total_users:,}",          help=h("rfm"))
    c2.metric(t("platform_gmv"),  f"AED {total_ltv/1e6:.1f}M",
              help=h("gmv") + "\n\n" + t("gmv_discrepancy_note"))
    c3.metric(t("avg_ltv"),       f"AED {avg_ltv:,.0f}",       help=h("ltv"))
    c4.metric(t("repeat_rate"),   f"{repeat_rate}%",           help=h("repeat_rate"))
    c5.metric(t("at_risk"),       f"{at_risk_n:,}",            help=h("at_risk"),
              delta="⚠ action needed", delta_color="inverse")
    st.caption(t("platform_gmv_note") + "  |  " + t("repeat_rate_sublabel"))

    # Traffic-light callouts
    try:
        _rr = float(str(repeat_rate).replace("%",""))
    except Exception:
        _rr = 0
    if _rr < 20:
        st.warning(t("tl_repeat_weak"))
    elif _rr >= 40:
        st.success(t("tl_repeat_strong"))
    if total_users > 0 and at_risk_n / total_users > 0.20:
        st.error(t("tl_churn_high"))

    st.divider()
    col_a, col_b = st.columns(2)

    with col_a:
        section(t("user_segments_chart"), "rfm")
        st.caption(t("intro_segments"))
        df_s, sc = translate_col(df_rfm, "Segment", SEG_KEY_MAP)
        sc_map   = translate_color_map(SEG_KEY_MAP, SEGMENT_COLORS)
        seg_c    = df_s[sc].value_counts().reset_index()
        seg_c.columns = [sc, t("ax_users")]
        # Build description lookup: translated_seg → plain description
        _disp_to_en = {v: k for k, v in label_map(SEG_KEY_MAP).items()}
        seg_c["_desc"] = seg_c[sc].map(lambda x: seg_desc(_disp_to_en.get(x, x)))
        total_u = seg_c[t("ax_users")].sum()
        seg_c["_pct"] = (seg_c[t("ax_users")] / total_u * 100).round(1)
        fig = px.pie(seg_c, names=sc, values=t("ax_users"),
                     color=sc, color_discrete_map=sc_map, hole=0.4,
                     custom_data=["_desc", "_pct"])
        fig.update_traces(
            textposition="inside", textinfo="percent+label",
            hovertemplate="<b>%{label}</b><br>%{value:,} customers (%{customdata[1]:.1f}%)<br><i>%{customdata[0]}</i><extra></extra>"
        )
        fig.update_layout(showlegend=False, margin=dict(t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        section(t("revenue_by_segment"), "monetary")
        df_s, sc = translate_col(df_rfm, "Segment", SEG_KEY_MAP)
        sc_map   = translate_color_map(SEG_KEY_MAP, SEGMENT_COLORS)
        seg_rev  = df_s.groupby(sc)["monetary"].sum().reset_index()
        seg_rev.columns = [sc, "Revenue"]
        _total_rev = seg_rev["Revenue"].sum()
        seg_rev["_pct"] = (seg_rev["Revenue"] / _total_rev * 100).round(1)
        _disp_to_en2 = {v: k for k, v in label_map(SEG_KEY_MAP).items()}
        seg_rev["_desc"] = seg_rev[sc].map(lambda x: seg_desc(_disp_to_en2.get(x, x)))
        seg_rev  = seg_rev.sort_values("Revenue", ascending=True)
        fig2 = px.bar(seg_rev, x="Revenue", y=sc, orientation="h",
                      color=sc, color_discrete_map=sc_map,
                      text=seg_rev["Revenue"].apply(lambda x: f"AED {x/1e6:.1f}M"),
                      labels={"Revenue": t("ax_total_revenue"), sc: ""},
                      custom_data=["_pct", "_desc"])
        fig2.update_traces(
            hovertemplate="<b>%{y}</b><br>Revenue: AED %{x:,.0f}<br>Share of total: %{customdata[0]:.1f}%<br><i>%{customdata[1]}</i><extra></extra>"
        )
        fig2.update_layout(showlegend=False, margin=dict(t=10, b=10))
        st.plotly_chart(fig2, use_container_width=True)

    section(t("ltv_tier_dist"), "ltv_tier")
    with st.expander(t("tier_guide_title"), expanded=False):
        st.caption(t("tier_guide_caption"))
        for _tk in ["tier_platinum_def","tier_gold_def","tier_silver_def","tier_bronze_def"]:
            st.markdown(t(_tk))
        st.caption(t("tier_guide_note"))
    tier_src = ltv_src if "ltv_tier" in ltv_src.columns else df_rfm
    if "ltv_tier" in tier_src.columns:
        df_t, tc = translate_col(tier_src, "ltv_tier", TIER_KEY_MAP)
        tc_map   = translate_color_map(TIER_KEY_MAP, TIER_COLORS)
        tier_order_t = [t(v) for v in ["tier_platinum","tier_gold","tier_silver","tier_bronze"]]
        tier_data = df_t.groupby(tc).agg(
            Users=("user_id","count"), Revenue=("monetary","sum")
        ).reset_index()
        tier_data[tc] = pd.Categorical(tier_data[tc], categories=tier_order_t, ordered=True)
        tier_data = tier_data.sort_values(tc)
        cc, cd = st.columns(2)
        with cc:
            f3 = px.bar(tier_data, x=tc, y="Users", color=tc,
                        color_discrete_map=tc_map, text="Users",
                        labels={tc: t("ax_ltv_tier"), "Users": t("ax_users")})
            f3.update_layout(showlegend=False)
            st.plotly_chart(f3, use_container_width=True)
        with cd:
            f4 = px.bar(tier_data, x=tc, y="Revenue", color=tc,
                        color_discrete_map=tc_map,
                        text=tier_data["Revenue"].apply(lambda x: f"AED {x/1e6:.0f}M"),
                        labels={tc: t("ax_ltv_tier"), "Revenue": t("ax_revenue")})
            f4.update_layout(showlegend=False)
            st.plotly_chart(f4, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: USER SEGMENTS
# ══════════════════════════════════════════════════════════════════════════════
elif page_key == "segments":
    st.title(t("segments_title"))
    show_period_banner(from_date, to_date, mn, mx)

    df_rfm   = load_csv("rfm_scores.csv")
    df_churn = load_csv("churn_risk.csv")

    if df_rfm.empty:
        st.warning(t("no_data")); st.stop()

    _total_before = len(df_rfm)
    if is_filtered:
        df_rfm = date_filter(df_rfm, from_date, to_date)
        if not df_churn.empty and "user_id" in df_churn.columns:
            df_churn = df_churn[df_churn["user_id"].isin(df_rfm["user_id"])]
        _pct = len(df_rfm) / _total_before * 100 if _total_before else 0
        st.info(t("date_filter_count").format(n=len(df_rfm), total=_total_before, pct=_pct))

    # Translate segment names for filter dropdown
    seg_en_to_disp = label_map(SEG_KEY_MAP)
    seg_disp_to_en = {v: k for k, v in seg_en_to_disp.items()}
    seg_list = [t("all")] + sorted([seg_en_to_disp.get(s, s) for s in df_rfm["Segment"].unique()])
    sel_disp = st.selectbox(t("filter_segment"), seg_list)
    if sel_disp == t("all"):
        df_view = df_rfm
        sel_en  = None
    else:
        sel_en  = seg_disp_to_en.get(sel_disp, sel_disp)
        df_view = df_rfm[df_rfm["Segment"] == sel_en]
        _desc = seg_desc(sel_en)
        if _desc:
            st.info(f"**{sel_disp}** — {_desc}")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric(t("users_in_view"), f"{len(df_view):,}",                          help=h("rfm"))
    c2.metric(t("avg_ltv_label"), f"AED {df_view['monetary'].mean():,.0f}",     help=h("ltv"))
    c3.metric(t("avg_recency"),   f"{df_view['recency_days'].mean():.0f} {t('days')}", help=h("recency"))
    c4.metric(t("total_revenue"), f"AED {df_view['monetary'].sum()/1e6:.1f}M",  help=h("monetary"))

    with st.expander(t("seg_guide_title"), expanded=False):
        st.caption(t("seg_guide_caption"))
        for _sg_key in ["champions","loyal","potential","new","promising",
                        "need_attn","at_risk","cant_lose","about_sleep","lost"]:
            _sg_text = t(f"seg_guide_{_sg_key}")
            if _sg_text:
                st.markdown(f"- {_sg_text}")

    st.divider()
    section(t("rfm_map"), "rfm")
    st.caption(t("intro_rfm"))
    sample   = df_view.sample(min(2000, len(df_view)), random_state=42)
    df_s, sc = translate_col(sample, "Segment", SEG_KEY_MAP)
    sc_map   = translate_color_map(SEG_KEY_MAP, SEGMENT_COLORS)
    _seg_desc_col = sample["Segment"].map(lambda x: seg_desc(x))
    df_s["_desc"] = _seg_desc_col.values
    fig = px.scatter(
        df_s, x="recency_days", y="monetary", color=sc, size="frequency",
        color_discrete_map=sc_map,
        custom_data=[sc, "ltv_tier", "_desc"],
        labels={"recency_days": t("ax_days_since"), "monetary": t("ax_ltv_aed"), sc: ""},
        opacity=0.7,
    )
    fig.update_traces(
        hovertemplate=(
            "<b>%{customdata[0]}</b><br>"
            "Total spent: AED %{y:,.0f}<br>"
            "Last order: %{x} days ago<br>"
            "Customer tier: %{customdata[1]}<br>"
            "<i>%{customdata[2]}</i><extra></extra>"
        )
    )
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)

    section(t("segment_table"), "rfm")
    agg_dict = {
        "Users":             ("user_id",     "count"),
        "Avg_LTV_AED":       ("monetary",    "mean"),
        "Total_Revenue_AED": ("monetary",    "sum"),
        "Avg_Recency_Days":  ("recency_days","mean"),
    }
    if "predicted_ltv_12m" in df_rfm.columns:
        agg_dict["Predicted_LTV_12m"] = ("predicted_ltv_12m", "mean")
    seg_summary = df_rfm.groupby("Segment").agg(**agg_dict).round(0).reset_index()
    seg_summary["Revenue_Share_%"] = (
        seg_summary["Total_Revenue_AED"] / seg_summary["Total_Revenue_AED"].sum() * 100
    ).round(1)
    seg_summary = seg_summary.sort_values("Total_Revenue_AED", ascending=False)
    # Translate segment column for display
    seg_summary["Segment"] = seg_summary["Segment"].map(seg_en_to_disp).fillna(seg_summary["Segment"])
    st.dataframe(
        safe_df(seg_summary, subset=["Total_Revenue_AED"], cmap="Greens"),
        use_container_width=True, hide_index=True,
    )

    st.divider()
    section(t("churn_risk"), "churn")
    st.caption(t("intro_churn"))
    # Segment action callouts
    _seg_counts = df_rfm["Segment"].value_counts()
    _total_seg  = len(df_rfm)
    if "Cant Lose Them" in _seg_counts and _seg_counts["Cant Lose Them"] > 0:
        st.error(t("tl_cant_lose"))
    if "At Risk" in _seg_counts and _seg_counts.get("At Risk", 0) / _total_seg > 0.10:
        st.warning(t("tl_at_risk"))
    if _seg_counts.get("Lost", 0) / _total_seg > 0.20:
        st.warning(t("tl_lost_high"))
    if _seg_counts.get("Champions", 0) / _total_seg > 0.10:
        st.success(t("tl_champions"))
    if not df_churn.empty and "churn_risk_label" in df_churn.columns:
        df_ch, cc_col = translate_col(df_churn, "churn_risk_label", CHURN_KEY_MAP)
        cc_map = translate_color_map(CHURN_KEY_MAP, CHURN_COLORS)
        rc = df_ch[cc_col].value_counts().reset_index()
        rc.columns = [cc_col, t("ax_users")]
        _churn_en_disp = {v: k for k, v in label_map(CHURN_KEY_MAP).items()}
        rc["_desc"] = rc[cc_col].map(lambda x: churn_desc(_churn_en_disp.get(x, x)))
        f5 = px.bar(rc, x=cc_col, y=t("ax_users"), color=cc_col,
                    color_discrete_map=cc_map, text=t("ax_users"),
                    custom_data=["_desc"],
                    labels={cc_col: "", t("ax_users"): t("ax_users")})
        f5.update_traces(
            hovertemplate="<b>%{x}</b><br>%{y:,} customers<br><i>%{customdata[0]}</i><extra></extra>"
        )
        f5.update_layout(showlegend=False)
        st.plotly_chart(f5, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: PRODUCTS & BCG
# ══════════════════════════════════════════════════════════════════════════════
elif page_key == "products":
    st.title(t("products_title"))
    show_filter_not_applied(from_date, to_date, mn, mx)

    df_bcg = load_csv("bcg_matrix.csv")
    df_cat = load_csv("category_performance.csv")
    prod_r = load_json("product_recommendations.json")

    if df_bcg.empty:
        st.warning(t("no_data")); st.stop()

    section(t("bcg_chart_title"), "bcg")
    st.caption(t("intro_bcg"))
    st.caption(t("bcg_growth_caveat"))
    with st.expander(t("bcg_explain_title"), expanded=False):
        st.markdown(t("bcg_explain_body"))
    df_b, qc = translate_col(df_bcg, "bcg_quadrant", BCG_KEY_MAP)
    qc_map   = translate_color_map(BCG_KEY_MAP, BCG_COLORS)
    _bcg_desc_col = df_bcg["bcg_quadrant"].map(lambda x: bcg_desc(x))
    df_b["_desc"] = _bcg_desc_col.values
    fig = px.scatter(
        df_b.dropna(subset=["market_share_pct", "vitality"]),
        x="market_share_pct", y="vitality",
        color=qc, size="total_revenue",
        color_discrete_map=qc_map,
        custom_data=["product_name", "shop_name", "total_revenue", "_desc"] if "product_name" in df_b.columns else ["_desc"],
        labels={"market_share_pct": t("ax_market_share"), "vitality": t("ax_vitality"), qc: ""},
        size_max=40,
    )
    if "product_name" in df_b.columns:
        fig.update_traces(
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "Shop: %{customdata[1]}<br>"
                "Revenue: AED %{customdata[2]:,.0f}<br>"
                "Market share: %{x:.1f}%  |  Vitality: %{y:.1f}<br>"
                "<i>%{customdata[3]}</i><extra></extra>"
            )
        )
    ms_med = df_bcg["market_share_pct"].median()
    vt_med = df_bcg["vitality"].median()
    fig.add_vline(x=ms_med, line_dash="dash", line_color="gray", opacity=0.5)
    fig.add_hline(y=vt_med, line_dash="dash", line_color="gray", opacity=0.5)
    fig.update_layout(height=550)
    st.plotly_chart(fig, use_container_width=True)

    ca, cb, cc, cd = st.columns(4)
    for col_w, quad_en, quad_key, emoji in [
        (ca, "Star", "bcg_star", "⭐"), (cb, "Cash Cow", "bcg_cow", "🐄"),
        (cc, "Question Mark", "bcg_qm", "❓"), (cd, "Dog", "bcg_dog", "🐕"),
    ]:
        g = df_bcg[df_bcg["bcg_quadrant"] == quad_en]
        col_w.metric(f"{emoji} {t(quad_key)}", f"{len(g)} {t('ax_products')}",
                     f"AED {g['total_revenue'].sum():,.0f}", help=h("bcg"))

    st.divider()
    section(t("category_perf"), "market_share")
    st.caption(t("intro_category"))
    if not df_cat.empty:
        f2 = px.bar(df_cat.head(10), x="category_name", y="total_revenue",
                    color="revenue_share_pct", color_continuous_scale="Greens",
                    text=df_cat.head(10)["total_revenue"].apply(lambda x: f"AED {x:,.0f}"),
                    labels={"total_revenue": t("ax_revenue"), "category_name": t("ax_category")})
        f2.update_layout(coloraxis_showscale=False)
        st.plotly_chart(f2, use_container_width=True)

    st.divider()
    tab1, tab2, tab3 = st.tabs([t("tab_promote"), t("tab_drop"), t("tab_never")])
    with tab1:
        if prod_r.get("promote"):
            st.dataframe(pd.DataFrame(prod_r["promote"]), use_container_width=True, hide_index=True)
    with tab2:
        if prod_r.get("drop_candidates"):
            st.dataframe(pd.DataFrame(prod_r["drop_candidates"]), use_container_width=True, hide_index=True)
    with tab3:
        _never_all = prod_r.get("never_sold_active", [])
        _never_clean = [p for p in _never_all if (p.get("price") or 0) > 0]
        _hidden = len(_never_all) - len(_never_clean)
        if _hidden > 0:
            st.caption(f"ℹ️ {_hidden} legacy product(s) with price=0 are hidden — these are data artifacts that should be deleted from the DB.")
        if _never_clean:
            st.dataframe(pd.DataFrame(_never_clean), use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: VENDORS
# ══════════════════════════════════════════════════════════════════════════════
elif page_key == "vendors":
    st.title(t("vendors_title"))
    show_filter_not_applied(from_date, to_date, mn, mx)
    st.caption(t("intro_vendors"))

    df_shops = load_csv("shop_performance.csv")
    shop_r   = load_json("shop_rankings.json")

    if df_shops.empty:
        st.warning(t("no_data")); st.stop()

    # ── Section A: Platform Revenue Split ────────────────────────────────────
    section(t("vendor_rev_split"), "revenue_share")

    _has_type = "commission_type" in df_shops.columns
    if _has_type:
        _rev_split = df_shops.groupby("commission_type")["gross_revenue"].sum().reset_index()
        _ct_label_map = {
            "Own Brand":  t("vendor_own_brand"),
            "Marketplace": t("vendor_marketplace"),
            "Charity":    t("vendor_charity"),
        }
        _rev_split["commission_type_display"] = _rev_split["commission_type"].map(_ct_label_map).fillna(_rev_split["commission_type"])
        _ct_color = {
            t("vendor_own_brand"):   "#22c55e",
            t("vendor_marketplace"): "#3b82f6",
            t("vendor_charity"):     "#f59e0b",
        }
        _fig_split = px.pie(
            _rev_split, values="gross_revenue", names="commission_type_display",
            color="commission_type_display", color_discrete_map=_ct_color,
            hole=0.45,
        )
        _fig_split.update_traces(
            hovertemplate="<b>%{label}</b><br>AED %{value:,.0f}<br>%{percent}<extra></extra>"
        )
        _fig_split.update_layout(height=340, margin=dict(t=20, b=20))

        col_pie, col_kpi = st.columns([1, 1])
        with col_pie:
            st.plotly_chart(_fig_split, use_container_width=True)
        with col_kpi:
            st.metric(
                t("vendor_gmv"),
                f"AED {shop_r.get('total_gmv_aed', 0):,.0f}",
                help="Total platform GMV = direct Zabehaty sales + all marketplace/shop sales.",
            )
            st.metric(
                t("vendor_direct_sales"),
                f"AED {shop_r.get('direct_zabehaty_revenue_aed', 0):,.0f}",
                help=t("vendor_direct_note"),
            )
            st.caption(t("vendor_charity_note"))
            st.metric(
                t("vendor_own_revenue"),
                f"AED {shop_r.get('own_brand_revenue_aed', 0):,.0f}",
                help="Direct Zabehaty brand sales via shop listings — full revenue retained.",
            )
            st.metric(
                t("vendor_marketplace_commission"),
                f"AED {shop_r.get('marketplace_commission_earned_aed', 0):,.0f}",
                help="Actual commission earned from third-party marketplace shops.",
            )
            st.metric(
                t("vendor_charity_passthrough"),
                f"AED {shop_r.get('charity_passthrough_aed', 0):,.0f}",
                help="100% of this revenue is donated to charities — not Zabehaty income.",
            )
    else:
        # Fallback: old KPI row if commission_type column absent (re-run shop_analysis.py)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric(t("shops_analysed"),  shop_r.get("total_shops_analysed", 0))
        c2.metric(t("total_rev_kpi"),   f"AED {shop_r.get('total_platform_revenue_aed', 0):,.0f}", help=h("gmv"))
        c3.metric(t("est_commissions"), f"AED {shop_r.get('total_estimated_commission_aed', 0):,.0f}", help=h("commission"))
        c4.metric(t("own_brands"),      len(shop_r.get("own_brand_shops", [])))

    st.divider()

    # ── Section B: Best Performing Vendors ───────────────────────────────────
    section(t("revenue_by_shop"), "revenue_share")
    _is_ar_v = st.session_state.get("lang", "en") == "ar"
    top_shops = df_shops[df_shops["total_orders"] >= 3].nlargest(15, "gross_revenue").copy()
    if _is_ar_v and "shop_name_ar" in top_shops.columns:
        top_shops["shop_display"] = top_shops["shop_name_ar"].fillna(top_shops["shop_name"])
    else:
        top_shops["shop_display"] = top_shops["shop_name"]
    _own_label   = t("vendor_own_brand")
    _third_label = t("vendor_third_party")
    fig = px.bar(
        top_shops, x="shop_display", y="gross_revenue",
        color="health_score", color_continuous_scale="RdYlGn",
        text=top_shops["gross_revenue"].apply(lambda x: f"AED {x:,.0f}"),
        labels={"gross_revenue": t("ax_revenue"), "shop_display": t("ax_shop")},
        custom_data=["shop_display", "total_orders", "health_score", "cancel_rate_pct"],
    )
    fig.update_traces(
        hovertemplate=(
            "<b>%{customdata[0]}</b><br>"
            f"{t('vnd_col_revenue')}: AED %{{y:,.0f}}<br>"
            f"{t('vnd_col_orders')}: %{{customdata[1]:,}}<br>"
            f"{t('vnd_col_health')}: %{{customdata[2]:.0f}}/100<br>"
            f"{t('vnd_col_cancel')}: %{{customdata[3]:.1f}}%<extra></extra>"
        )
    )
    fig.update_layout(xaxis_tickangle=-30)
    st.plotly_chart(fig, use_container_width=True)

    section(t("health_vs_cancel"), "health_score")
    _vdf = df_shops[df_shops["total_orders"] >= 3].copy()
    if _is_ar_v and "shop_name_ar" in _vdf.columns:
        _vdf["shop_display"] = _vdf["shop_name_ar"].fillna(_vdf["shop_name"])
    else:
        _vdf["shop_display"] = _vdf["shop_name"]
    _vdf["shop_type_display"] = _vdf["is_own_brand"].map({1: _own_label, 0: _third_label})
    fig2 = px.scatter(
        _vdf,
        x="cancel_rate_pct", y="health_score",
        size="gross_revenue", color="shop_type_display",
        custom_data=["shop_display", "total_orders", "avg_order_value", "gross_revenue"],
        labels={"cancel_rate_pct": t("ax_cancel_rate"), "health_score": t("ax_health_score"), "shop_type_display": ""},
        color_discrete_map={_own_label: "#22c55e", _third_label: "#3b82f6"},
    )
    fig2.update_traces(
        hovertemplate=(
            "<b>%{customdata[0]}</b><br>"
            f"{t('vnd_col_health')}: %{{y:.0f}}/100<br>"
            f"{t('vnd_col_cancel')}: %{{x:.1f}}%<br>"
            f"{t('vnd_col_orders')}: %{{customdata[1]:,}}<br>"
            f"{t('vnd_col_revenue')}: AED %{{customdata[3]:,.0f}}<extra></extra>"
        )
    )
    st.plotly_chart(fig2, use_container_width=True)

    st.divider()

    # ── Section C: Vendors to Review ─────────────────────────────────────────
    section(t("vendor_to_review"), "cancel_rate")
    _review = df_shops[
        (df_shops["total_orders"] < 5) | (df_shops["health_score"] < 40)
    ].copy()
    if not _review.empty:
        def _review_reason(row):
            reasons = []
            if row["total_orders"] == 1:
                reasons.append(t("vnd_reason_one"))
            elif row["total_orders"] < 5:
                reasons.append(t("vnd_reason_few"))
            if row["health_score"] < 40:
                reasons.append(t("vnd_reason_low_health"))
            if str(row.get("shop_name", "")).lower() == "test111":
                reasons.append(t("vnd_reason_test"))
            return "; ".join(reasons) if reasons else t("vnd_reason_low")
        if _is_ar_v and "shop_name_ar" in _review.columns:
            _review["shop_display"] = _review["shop_name_ar"].fillna(_review["shop_name"])
        else:
            _review["shop_display"] = _review["shop_name"]
        _review[t("vnd_reason_col")] = _review.apply(_review_reason, axis=1)
        st.warning(f"⚠️ {t('vendor_to_review_note')}")
        _review_cols_map = {
            "shop_display": t("vnd_col_shop"),
            "total_orders": t("vnd_col_orders"),
            "gross_revenue": t("vnd_col_revenue"),
            "health_score": t("vnd_col_health"),
            "cancel_rate_pct": t("vnd_col_cancel"),
            t("vnd_reason_col"): t("vnd_reason_col"),
        }
        _avail_review = [c for c in _review_cols_map if c in _review.columns]
        st.dataframe(
            _review[_avail_review].rename(columns=_review_cols_map).sort_values(t("vnd_col_orders")),
            use_container_width=True, hide_index=True,
        )

    st.divider()

    # ── Section D: Full Vendor Table ──────────────────────────────────────────
    section(t("full_vendor_table"), "cancel_rate")
    _full = df_shops.copy()
    if _is_ar_v and "shop_name_ar" in _full.columns:
        _full["shop_name"] = _full["shop_name_ar"].fillna(_full["shop_name"])
    # Translate commission_type values
    _ct_val_map = {
        "Own Brand": t("vendor_own_brand"), "Marketplace": t("vendor_marketplace"),
        "Charity": t("vendor_charity"), "Direct": t("vendor_direct_sales"),
    }
    if "commission_type" in _full.columns:
        _full["commission_type"] = _full["commission_type"].map(_ct_val_map).fillna(_full["commission_type"])
    _col_rename = {
        "shop_name": t("vnd_col_shop"), "commission_type": t("vnd_col_type"),
        "shop_type": t("vnd_col_shop_type"), "total_orders": t("vnd_col_orders"),
        "gross_revenue": t("vnd_col_revenue"), "avg_order_value": t("vnd_col_aov"),
        "revenue_share_pct": t("vnd_col_share"), "cancel_rate_pct": t("vnd_col_cancel"),
        "health_score": t("vnd_col_health"), "commission_pct": t("vnd_col_comm_pct"),
        "estimated_commission": t("vnd_col_comm_est"),
    }
    display_cols = [c for c in _col_rename if c in _full.columns]
    _full_disp = _full[display_cols].rename(columns=_col_rename).sort_values(t("vnd_col_revenue"), ascending=False)
    st.dataframe(
        safe_df(_full_disp, subset=[t("vnd_col_health")], cmap="RdYlGn"),
        use_container_width=True, hide_index=True,
    )

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: BUYING PATTERNS
# ══════════════════════════════════════════════════════════════════════════════
elif page_key == "patterns":
    st.title(t("patterns_title"))
    show_filter_not_applied(from_date, to_date, mn, mx)
    st.caption(t("intro_patterns"))

    patterns = load_json("buying_patterns.json")
    df_cross = load_csv("cross_category.csv")

    if not patterns:
        st.warning(t("no_data")); st.stop()

    rp = patterns.get("repeat_purchase", {})
    c1, c2, c3 = st.columns(3)
    c1.metric(t("total_buyers"),      f"{rp.get('total_buyers', 0):,}",          help=h("rfm"))
    c2.metric(t("multi_category"),    f"{rp.get('multi_category_buyers', 0):,}", help=h("frequency"))
    c3.metric(t("repeat_rate_label"), f"{rp.get('repeat_rate_pct', 0)}%",        help=h("repeat_rate"))
    st.caption(t("repeat_rate_sublabel"))

    st.divider()
    col_a, col_b = st.columns(2)

    with col_a:
        section(t("peak_days"), "peak_timing")
        timing  = patterns.get("order_timing", {})
        days_df = pd.DataFrame(timing.get("peak_days", []))
        if not days_df.empty:
            f = px.bar(days_df, x="day_name", y="orders", color="orders",
                       color_continuous_scale="Blues", text="orders",
                       labels={"day_name": t("ax_day"), "orders": t("ax_orders")})
            f.update_traces(
                hovertemplate=f"<b>%{{x}}</b><br>%{{y:,}} orders<br><i>{t('peak_day_tooltip')}</i><extra></extra>"
            )
            f.update_layout(showlegend=False, coloraxis_showscale=False)
            st.plotly_chart(f, use_container_width=True)

        section(t("peak_hours"), "peak_timing")
        hours_df = pd.DataFrame(timing.get("peak_hours", []))
        if not hours_df.empty:
            f2 = px.bar(hours_df, x="hour_of_day", y="orders", color="orders",
                        color_continuous_scale="Oranges", text="orders",
                        labels={"hour_of_day": t("ax_hour"), "orders": t("ax_orders")})
            f2.update_traces(
                hovertemplate=f"<b>%{{x}}:00</b><br>%{{y:,}} orders<br><i>{t('peak_hour_tooltip')}</i><extra></extra>"
            )
            f2.update_layout(showlegend=False, coloraxis_showscale=False)
            st.plotly_chart(f2, use_container_width=True)

    with col_b:
        section(t("payment_split"), "payment")
        pay_df = pd.DataFrame(patterns.get("payment_methods", []))
        if not pay_df.empty:
            _total_pay = pay_df["orders"].sum()
            pay_df["_pct"] = (pay_df["orders"] / _total_pay * 100).round(1)
            f3 = px.pie(pay_df, names="method_name", values="orders",
                        hole=0.4, color_discrete_sequence=px.colors.qualitative.Set3,
                        custom_data=["_pct"])
            f3.update_traces(
                textposition="inside", textinfo="percent+label",
                hovertemplate="<b>%{label}</b><br>%{value:,} orders (%{customdata[0]:.1f}%)<extra></extra>"
            )
            f3.update_layout(showlegend=True)
            st.plotly_chart(f3, use_container_width=True)

        section(t("crosssell_other_title"), "cross_sell")
        if not df_cross.empty:
            df_cross["pair"] = df_cross["category_a"] + " + " + df_cross["category_b"]
            # Show non-Personal pairs first — more actionable cross-sell signals
            df_cross_other = df_cross[
                (df_cross["category_a"] != "Personal") & (df_cross["category_b"] != "Personal")
            ]
            _cross_data = df_cross_other.head(10) if not df_cross_other.empty else df_cross.head(10)
            f4 = px.bar(_cross_data, x="co_buyers", y="pair", orientation="h",
                        text="co_buyers", color="co_buyers", color_continuous_scale="Teal",
                        labels={"co_buyers": t("ax_co_buyers"), "pair": ""})
            f4.update_traces(
                hovertemplate="<b>%{y}</b><br>%{x:,} customers buy both<br><i>Strong cross-sell opportunity — consider bundling</i><extra></extra>"
            )
            f4.update_layout(showlegend=False, coloraxis_showscale=False)
            st.plotly_chart(f4, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: BOARD REPORT
# ══════════════════════════════════════════════════════════════════════════════
elif page_key == "report":
    st.title(t("report_title"))
    show_filter_not_applied(from_date, to_date, mn, mx)

    lang = st.session_state.get("lang", "en")
    narrative_en = load_json("narrative_report.json")
    ar_path      = os.path.join(TMP, "narrative_report_ar.json")
    md_path      = os.path.join(TMP, "board_summary.md")

    if not narrative_en:
        st.warning(t("no_narrative")); st.stop()

    # Load the correct language report
    if lang == "ar" and os.path.exists(ar_path):
        with open(ar_path, encoding="utf-8") as _f:
            narrative = json.load(_f)
    elif lang == "ar":
        st.info("التقرير العربي غير متاح بعد. شغّل `python tools/llm_interpreter.py` لتوليده.")
        narrative = narrative_en
    else:
        narrative = narrative_en

    if os.path.exists(md_path):
        with open(md_path, encoding="utf-8") as f:
            md_content = f.read()
        st.download_button(t("download_report"), md_content,
                           file_name="zabehaty_board_report.md", mime="text/markdown")

    st.divider()
    tabs = st.tabs([t("tab_exec"), t("tab_product"), t("tab_vendor"), t("tab_comm")])
    with tabs[0]: st.markdown(narrative.get("executive_summary",  "_Not generated_"))
    with tabs[1]: st.markdown(narrative.get("product_narrative",  "_Not generated_"))
    with tabs[2]: st.markdown(narrative.get("vendor_narrative",   "_Not generated_"))
    with tabs[3]: st.markdown(narrative.get("communication_strategy", "_Not generated_"))

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: BUSINESS SNAPSHOT  (plain-English overview for non-technical board)
# ══════════════════════════════════════════════════════════════════════════════
elif page_key == "health":
    st.title(t("health_title"))
    st.caption(t("health_subtitle"))
    show_filter_not_applied(from_date, to_date, mn, mx)

    hd = load_health_data()
    shop_r = load_json("shop_rankings.json")
    df_rfm = load_csv("rfm_scores.csv")

    if hd is None or "error" in (hd or {}):
        st.error(f"Could not load live data: {(hd or {}).get('error','DB unavailable')}")
        st.stop()

    # ── Inject synthetic gap months (always fresh — outside cache) ────────────
    _synth_path = os.path.join(ROOT, ".tmp", "synthetic_gap_summary.json")
    _has_synthetic = False
    if os.path.exists(_synth_path):
        try:
            with open(_synth_path, encoding="utf-8") as _sf:
                _synth_meta = json.load(_sf)
            _smdf = pd.DataFrame(_synth_meta["monthly_summary"])
            _gap_set = set(_synth_meta["gap_months"])
            _mom_raw = hd.get("mom", pd.DataFrame())
            _live_set = set(_mom_raw["month"].tolist()) if not _mom_raw.empty else set()
            _to_add   = _gap_set - _live_set
            if _to_add:
                _sinject = _smdf[_smdf["month"].isin(_to_add)].copy()
                _sinject["is_synthetic"] = True
                _mom_raw = _mom_raw.copy()
                _mom_raw["is_synthetic"] = False
                _mom_merged = pd.concat(
                    [_mom_raw, _sinject[["month","orders","revenue","customers","is_synthetic"]]],
                    ignore_index=True
                ).sort_values("month").reset_index(drop=True)
                hd = {**hd, "mom": _mom_merged}
                _has_synthetic = True
        except Exception:
            pass
    hd["has_synthetic"] = _has_synthetic

    # ── Period metrics ────────────────────────────────────────────────────────
    cmp = hd.get("comparison", pd.DataFrame())
    this_m = cmp[cmp["period"] == "this_month"].iloc[0] if not cmp.empty and "this_month" in cmp["period"].values else None
    prev_m = cmp[cmp["period"] == "last_year_same_month"].iloc[0] if not cmp.empty and "last_year_same_month" in cmp["period"].values else None

    this_rev   = float(this_m["revenue"])   if this_m is not None else 0
    prev_rev   = float(prev_m["revenue"])   if prev_m is not None else 0
    this_ord   = int(this_m["orders"])      if this_m is not None else 0
    prev_ord   = int(prev_m["orders"])      if prev_m is not None else 0
    this_cust  = int(this_m["customers"])   if this_m is not None else 0
    has_yoy    = prev_rev > 0
    rev_growth = ((this_rev - prev_rev) / prev_rev * 100) if has_yoy else None
    ord_growth = ((this_ord - prev_ord) / prev_ord * 100) if prev_ord > 0 else None

    nu_df = hd.get("new_users", pd.DataFrame())
    new_this = int(nu_df[nu_df["month"] == pd.Timestamp.now().strftime("%Y-%m")]["new_users"].sum()) if not nu_df.empty else 0

    # ── Health colour ─────────────────────────────────────────────────────────
    if rev_growth is None:
        health_icon, health_label, health_color, health_note = "⚪", t("health_no_comparison"), "gray", ""
    elif rev_growth >= 5:
        health_icon, health_label, health_color, health_note = "🟢", t("health_growing"), "green", t("health_vs_lyst").format(pct=f"{rev_growth:+.1f}")
    elif rev_growth >= -5:
        health_icon, health_label, health_color, health_note = "🟡", t("health_stable"), "orange", t("health_vs_lyst").format(pct=f"{rev_growth:+.1f}")
    else:
        health_icon, health_label, health_color, health_note = "🔴", t("health_declining"), "red", t("health_vs_lyst").format(pct=f"{rev_growth:+.1f}")

    st.markdown(f"### {health_icon} Business Health: **:{health_color}[{health_label}]**" + (f" &nbsp;&nbsp; *({health_note})*" if health_note else ""))
    st.divider()

    # ── KPI row ───────────────────────────────────────────────────────────────
    k1, k2, k3, k4 = st.columns(4)
    _yoy_delta = f"{rev_growth:+.1f}% {t('health_vs_last_year')}" if rev_growth is not None else None
    _ord_delta  = f"{ord_growth:+.1f}% {t('health_vs_last_year')}" if ord_growth is not None else None
    k1.metric(t("health_revenue_month"),   f"AED {this_rev:,.0f}", delta=_yoy_delta,
              help=h("health_rev_month"))
    k2.metric(t("health_orders_month"),    f"{this_ord:,}",         delta=_ord_delta,
              help=h("health_ord_month"))
    k3.metric(t("health_customers_month"), f"{this_cust:,}",
              help=h("health_cust_month"))
    k4.metric(t("health_signups_month"),   f"{new_this:,}",
              help=h("health_signups"))

    # 3-year GMV callout
    gmv = shop_r.get("total_gmv_aed", 0)
    st.info(t("health_gmv_total").format(gmv=f"{gmv:,.0f}") + "  ℹ️ " + h("health_gmv_info"))

    st.divider()

    # ── Revenue trend ─────────────────────────────────────────────────────────
    st.subheader(t("health_revenue_chart"))
    st.caption(t("health_revenue_chart_note"))
    if hd.get("has_synthetic"):
        st.info(t("health_synth_banner"))
    mom_df = hd.get("mom", pd.DataFrame())
    if not mom_df.empty:
        mom_df = mom_df.copy()
        mom_df["revenue"] = pd.to_numeric(mom_df["revenue"], errors="coerce")
        mom_df["orders"] = pd.to_numeric(mom_df["orders"], errors="coerce")
        mom_df["customers"] = pd.to_numeric(mom_df["customers"], errors="coerce")
        # Use is_synthetic flag if injected from SQLite; otherwise start as False
        if "is_synthetic" in mom_df.columns:
            mom_df["is_estimated"] = mom_df["is_synthetic"].fillna(False)
        else:
            mom_df["is_estimated"] = False

        # ── Gap-fill: predict missing months using YoY growth factor ──────────
        baseline_df = hd.get("gap_baseline", pd.DataFrame())
        predicted_rows = []
        if not baseline_df.empty:
            baseline_df = baseline_df.copy()
            baseline_df["revenue"] = pd.to_numeric(baseline_df["revenue"], errors="coerce")
            baseline_df["orders"] = pd.to_numeric(baseline_df["orders"], errors="coerce")
            baseline_df["customers"] = pd.to_numeric(baseline_df["customers"], errors="coerce")

            def _get_rev(df, m):
                row = df[df["month"] == m]
                return float(row["revenue"].iloc[0]) if not row.empty and row["revenue"].iloc[0] > 0 else None

            # YoY factor: average ratio of Oct/Nov 2025 vs Oct/Nov 2024
            ratios = [a/b for a, b in [
                (_get_rev(mom_df, "2025-10"), _get_rev(baseline_df, "2024-10")),
                (_get_rev(mom_df, "2025-11"), _get_rev(baseline_df, "2024-11")),
            ] if a and b]
            yoy_factor = sum(ratios) / len(ratios) if ratios else 1.0

            for base_m, pred_m in [("2024-12","2025-12"), ("2025-01","2026-01"), ("2025-02","2026-02")]:
                if pred_m not in mom_df["month"].values:
                    base = baseline_df[baseline_df["month"] == base_m]
                    if not base.empty:
                        predicted_rows.append({
                            "month": pred_m,
                            "orders": round(float(base["orders"].iloc[0]) * yoy_factor),
                            "revenue": float(base["revenue"].iloc[0]) * yoy_factor,
                            "customers": round(float(base["customers"].iloc[0]) * yoy_factor),
                            "is_estimated": True,
                        })
        else:
            yoy_factor = 1.0

        if predicted_rows:
            mom_df = pd.concat([mom_df, pd.DataFrame(predicted_rows)], ignore_index=True)
            mom_df = mom_df.sort_values("month").reset_index(drop=True)

        # Detect any remaining gaps after estimation
        all_months = pd.date_range(
            start=pd.to_datetime(mom_df["month"].min()),
            end=pd.to_datetime(mom_df["month"].max()), freq="MS"
        ).strftime("%Y-%m").tolist()
        estimated_months = [r["month"] for r in predicted_rows]
        missing_all = [m for m in all_months if m not in mom_df["month"].values]
        still_missing = [m for m in missing_all if m not in estimated_months]

        if estimated_months and not hd.get("has_synthetic"):
            st.info(t("health_gap_filled").format(
                months=", ".join(estimated_months), factor=f"{yoy_factor:.0%}"
            ))
        if still_missing:
            st.warning(t("health_partial_gap").format(months=", ".join(still_missing)))

        _actual_lbl = t("health_actual")
        _est_lbl    = t("health_estimated")
        mom_df["type"] = mom_df["is_estimated"].map({True: _est_lbl, False: _actual_lbl})
        mom_df["month_label"] = mom_df.apply(
            lambda r: r["month"] + (" [est.]" if r["is_estimated"] else ""), axis=1
        )
        mom_df["label"] = mom_df.apply(
            lambda r: ("~" if r["is_estimated"] else "") +
                      (f"AED {r['revenue']/1e6:.1f}M" if r["revenue"] >= 1e6 else f"AED {r['revenue']:,.0f}"),
            axis=1
        )
        fig_mom = px.bar(
            mom_df, x="month_label", y="revenue", color="type", text="label",
            color_discrete_map={_actual_lbl: "#86efac", _est_lbl: "#fde68a"},
            labels={"month_label": t("health_ax_month"), "revenue": t("health_col_revenue"), "type": ""},
        )
        fig_mom.update_layout(xaxis_tickangle=-45, height=360, legend=dict(orientation="h", y=1.05))
        fig_mom.update_traces(textposition="outside")
        st.plotly_chart(fig_mom, use_container_width=True)

    st.divider()

    # ── Top shops + top products ──────────────────────────────────────────────
    col_shops, col_prods = st.columns(2)

    with col_shops:
        st.subheader(t("health_best_shops"))
        st.caption(t("health_best_shops_note"))
        ts = hd.get("top_shops", pd.DataFrame())
        if not ts.empty:
            _is_ar = st.session_state.get("lang","en") == "ar"
            ts = ts.copy()
            ts["revenue"] = pd.to_numeric(ts["revenue"], errors="coerce")
            ts["shop_display"] = ts["shop_ar"].fillna(ts["shop"]) if (_is_ar and "shop_ar" in ts.columns) else ts["shop"]
            ts_disp = ts[["shop_display","orders","revenue"]].copy()
            ts_disp["revenue"] = ts_disp["revenue"].apply(lambda x: f"AED {x:,.0f}")
            ts_disp.columns = [t("health_col_shop"), t("health_col_orders"), t("health_col_revenue")]
            ts_disp.index = range(1, len(ts_disp)+1)
            st.dataframe(ts_disp, use_container_width=True)
            fig_shops = px.bar(ts.head(6), x="revenue", y="shop_display", orientation="h",
                               color="revenue", color_continuous_scale="Greens",
                               labels={"revenue": t("health_col_revenue"), "shop_display": t("health_col_shop")})
            fig_shops.update_layout(showlegend=False, coloraxis_showscale=False, height=280, yaxis={"autorange":"reversed"})
            st.plotly_chart(fig_shops, use_container_width=True)

    with col_prods:
        st.subheader(t("health_best_products"))
        st.caption(t("health_best_products_note"))
        tp = hd.get("top_products", pd.DataFrame())
        if not tp.empty:
            _is_ar = st.session_state.get("lang","en") == "ar"
            tp = tp.copy()
            tp["revenue"] = pd.to_numeric(tp["revenue"], errors="coerce")
            tp["product_display"]  = tp["product_ar"].fillna(tp["product"])   if (_is_ar and "product_ar"  in tp.columns) else tp["product"]
            tp["category_display"] = tp["category_ar"].fillna(tp["category"]) if (_is_ar and "category_ar" in tp.columns) else tp["category"]
            tp_disp = tp[["product_display","category_display","orders","revenue"]].copy()
            tp_disp["revenue"] = tp_disp["revenue"].apply(lambda x: f"AED {x:,.0f}")
            tp_disp.columns = [t("health_col_product"), t("health_col_category"), t("health_col_orders"), t("health_col_revenue")]
            tp_disp.index = range(1, len(tp_disp)+1)
            st.dataframe(tp_disp, use_container_width=True)
            fig_prods = px.bar(tp.head(6), x="revenue", y="product_display", orientation="h",
                               color="revenue", color_continuous_scale="Blues",
                               labels={"revenue": t("health_col_revenue"), "product_display": t("health_col_product")})
            fig_prods.update_layout(showlegend=False, coloraxis_showscale=False, height=280, yaxis={"autorange":"reversed"})
            st.plotly_chart(fig_prods, use_container_width=True)

    st.divider()

    # ── Most loyal customers ──────────────────────────────────────────────────
    st.subheader(t("health_loyal_customers"))
    st.caption(t("health_loyal_note"))
    tc = hd.get("top_customers", pd.DataFrame())
    if not tc.empty:
        tc["lifetime_value"] = pd.to_numeric(tc["lifetime_value"], errors="coerce")
        tc["days_since_last_order"] = pd.to_numeric(tc["days_since_last_order"], errors="coerce").astype("Int64")
        # Merge ID + first name into one display column
        if "first_name" in tc.columns:
            tc["customer_display"] = tc.apply(
                lambda r: f"Customer #{r['user_id']} — {r['first_name']}" if r['first_name'] != '—'
                          else f"Customer #{r['user_id']}", axis=1)
        else:
            tc["customer_display"] = tc["customer"]
        tc_disp = tc[["customer_display","total_orders","lifetime_value","days_since_last_order"]].copy()
        tc_disp["lifetime_value"] = tc_disp["lifetime_value"].apply(lambda x: f"AED {x:,.0f}")
        tc_disp["days_since_last_order"] = tc_disp["days_since_last_order"].apply(
            lambda x: t("health_days_ago_ok").format(n=x) if x <= 30
                 else (t("health_days_ago_warn").format(n=x) if x <= 90
                  else  t("health_days_ago_bad").format(n=x))
        )
        tc_disp.columns = [t("health_col_customer"), t("health_col_total_orders"),
                           t("health_col_lifetime"), t("health_col_last_order")]
        tc_disp.index = range(1, len(tc_disp)+1)
        st.dataframe(tc_disp, use_container_width=True)

    st.divider()

    # ── Business health insights ──────────────────────────────────────────────
    st.subheader(t("health_insights"))
    patterns = load_json("buying_patterns.json")
    repeat_rate = patterns.get("repeat_purchase", {}).get("repeat_rate_pct", 0)
    churn_dist  = patterns.get("churn_risk_distribution", {})
    critical_n  = churn_dist.get("Critical", 0)

    total_users = len(df_rfm) if not df_rfm.empty else 0
    champions   = len(df_rfm[df_rfm["Segment"] == "Champions"]) if not df_rfm.empty else 0
    lost        = len(df_rfm[df_rfm["Segment"] == "Lost"]) if not df_rfm.empty else 0

    top_user_rev_share = 0
    if not df_rfm.empty and "monetary" in df_rfm.columns:
        top10 = df_rfm.nlargest(10, "monetary")["monetary"].sum()
        total_mon = df_rfm["monetary"].sum()
        top_user_rev_share = top10 / total_mon * 100 if total_mon > 0 else 0

    direct_pct = shop_r.get('direct_zabehaty_revenue_aed', 0) / max(shop_r.get('total_gmv_aed', 1), 1) * 100
    insights = [
        t("health_insight_repeat").format(
            pct=f"{repeat_rate:.1f}",
            note=t("health_insight_repeat_ok") if repeat_rate >= 25 else t("health_insight_repeat_low")),
        t("health_insight_concentration").format(
            pct=f"{top_user_rev_share:.1f}",
            note=t("health_insight_conc_ok") if top_user_rev_share < 15 else t("health_insight_conc_high")),
        t("health_insight_base").format(total=total_users, champ=champions, lost=lost),
        t("health_insight_peak"),
        t("health_insight_direct").format(pct=f"{direct_pct:.0f}"),
    ]
    for ins in insights:
        st.markdown(f"- {ins}")

    st.divider()

    # ── Alerts ────────────────────────────────────────────────────────────────
    st.subheader(t("health_alerts"))
    one_time_n   = hd.get("one_time_buyers", 0)
    stopped_n    = hd.get("stopped_loyals", 0)
    alerts = []

    if one_time_n > 0:
        alerts.append(("error", t("health_alert_onetime").format(n=one_time_n)))
    if stopped_n > 0:
        alerts.append(("warning", t("health_alert_loyals").format(n=stopped_n)))
    if critical_n > 0:
        alerts.append(("warning", t("health_alert_critical").format(n=critical_n)))
    if rev_growth is not None and rev_growth < -10:
        alerts.append(("error", t("health_alert_revenue").format(pct=f"{abs(rev_growth):.1f}")))

    if not alerts:
        st.success(t("health_no_alerts"))
    else:
        for level, msg in alerts:
            if level == "error":
                st.error(msg)
            else:
                st.warning(msg)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: AI ANALYST (conversational agent)
# ══════════════════════════════════════════════════════════════════════════════
elif page_key == "agent":
    import sys as _sys
    _agent_tools_dir = os.path.join(ROOT, "tools")
    if _agent_tools_dir not in _sys.path:
        _sys.path.insert(0, _agent_tools_dir)
    _dash_dir = os.path.dirname(__file__)
    if _dash_dir not in _sys.path:
        _sys.path.insert(0, _dash_dir)
    from agent_page import render_agent_page
    render_agent_page(t, h, st.session_state.get("lang", "en"))
