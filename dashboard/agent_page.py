"""
agent_page.py
Conversational AI Analytics Agent — Zabehaty Dashboard.

Features:
  - Natural-language Q&A about business data (Arabic / English auto-detect)
  - Data provenance: shows source, filters, and formula for every number
  - Answerable follow-up: "how did you get that?" always works
  - Excel export: "make me a report with X, Y, Z columns"
  - Predictive projections: "how much revenue next 2 weeks?"
"""

import os
import sys
import json
import random
from datetime import date

import streamlit as st
import anthropic

# ── Tool imports ──────────────────────────────────────────────────────────────
_tools_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "tools")
sys.path.insert(0, _tools_dir)

from agent_tools import TOOL_DEFINITIONS, dispatch_tool

# ── Constants ─────────────────────────────────────────────────────────────────
MODEL        = "claude-sonnet-4-6"
MAX_TOKENS   = 4096
MAX_MESSAGES = 40
TMP          = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".tmp")

# ─────────────────────────────────────────────────────────────────────────────
# System prompt
# ─────────────────────────────────────────────────────────────────────────────

def _load_kpi_snapshot() -> str:
    try:
        shop_path = os.path.join(TMP, "shop_rankings.json")
        pat_path  = os.path.join(TMP, "buying_patterns.json")
        seg_path  = os.path.join(TMP, "user_segments.json")

        shop_r = {}
        if os.path.exists(shop_path):
            with open(shop_path, encoding="utf-8") as f:
                shop_r = json.load(f)

        patterns = {}
        if os.path.exists(pat_path):
            with open(pat_path, encoding="utf-8") as f:
                patterns = json.load(f)

        segments = []
        if os.path.exists(seg_path):
            with open(seg_path, encoding="utf-8") as f:
                segments = json.load(f)

        gmv      = shop_r.get("total_gmv_aed", 0)
        rp_stats = patterns.get("repeat_purchase", {})
        churn    = patterns.get("churn_risk_distribution", {})
        top3_seg = sorted(segments, key=lambda x: x.get("total_revenue", 0), reverse=True)[:3] if segments else []

        return f"""
PLATFORM SNAPSHOT (pre-computed, as of latest pipeline run):
- Total platform GMV (3-year): AED {gmv:,.0f}
- Repeat purchase rate: {rp_stats.get('repeat_rate_pct', 'N/A')}%
- Avg categories per user: {rp_stats.get('avg_categories_per_user', 'N/A')}
- Churn risk: Critical={churn.get('Critical',0):,}  High={churn.get('High',0):,}  Medium={churn.get('Medium',0):,}  Low={churn.get('Low',0):,}
- Top segments by revenue: {', '.join(f"{s.get('Segment','?')} (AED {s.get('total_revenue',0):,.0f})" for s in top3_seg)}
""".strip()
    except Exception:
        return "Platform snapshot: data unavailable — run the analysis pipeline first."


def build_system_prompt() -> str:
    kpi = _load_kpi_snapshot()
    today = date.today().strftime("%B %d, %Y")
    return f"""You are the AI Business Analyst for Zabehaty — a UAE-based Islamic meat, slaughter, and food marketplace (currency: AED).
Today's date: {today}

{kpi}

YOUR ROLE:
- Answer stakeholder questions about business performance in plain language
- Detect the language of every user message and reply in the SAME language (Arabic or English)
- Be confident and direct — speak like a senior analyst briefing a board, not hedging

TOOLS:
You have access to live data tools. Use them to answer questions precisely.
Always prefer calling a tool over guessing from memory.

DATE RANGE RULES — CRITICAL:
Today is {today}. When the user says any relative time range, you MUST calculate the exact dates BEFORE calling any tool:
- "last 6 months"  → date_from = 6 months before today, date_to = today
- "last 3 months"  → date_from = 3 months before today, date_to = today
- "last year"      → date_from = 12 months before today, date_to = today
- "this month"     → date_from = first day of current month, date_to = today
- "Q1 2026"        → date_from = 2026-01-01, date_to = 2026-03-31
- "this year"      → date_from = 2026-01-01, date_to = today
NEVER leave date_from/date_to blank when the user specifies a time range.

EXCEL REPORT RULES:
- "all core business metrics" / "full report" / "everything" → pass columns=["all"]
- "BCG matrix" → pass columns=["bcg"]
- "customer behavior" / "deeper dive" → pass columns=["all", "customer_behavior"]
- "lost users" / "win-back" / "churned customers" → FIRST call get_lost_users_winback, THEN export_excel_report with columns=["lost_users"] if they want a file
- Always calculate and pass date_from/date_to — never let it default to current month when the user asked for a longer range

AVERAGE LTV:
When asked "average LTV", "average customer value", "what is our LTV":
→ Call get_ltv_average(). It returns overall average + median + breakdown by RFM segment.
   Do NOT use get_ltv_stats() for this — that only returns tier buckets, not averages.

CROSS-SELLING / OFFER IDEAS:
When asked about cross-selling, upselling, bundling, or "what offer should I make":
→ Call get_cross_sell_opportunities(). It returns category pairs with co-buyer counts and ready offer wording.
   Always include the best send timing (peak day/hour) in your answer.

TOP CUSTOMERS + PROMO CAMPAIGNS:
When asked to create a promotion, campaign, WhatsApp/notification content, or target top customers:
1. Call generate_promo_campaign(segment="Champions", limit=15)
2. Present the campaign summary (customers targeted, estimated revenue lift, optimal send day/time)
3. Show 3–5 sample messages (English and Arabic) to illustrate personalisation
4. Offer to export the full list as Excel with columns=["all"]
Do NOT just describe the idea — generate the actual WhatsApp message content.

LOST USERS / WIN-BACK:
When asked for lost users, churned customers, or win-back campaigns:
1. Call get_lost_users_winback(min_revenue=2000, limit=50)
2. Summarize: total lost revenue, top 5–10 users with their tactic
3. Group tactics by urgency tier (Platinum/VIP, High, Standard)
4. If user wants an Excel file, call export_excel_report with columns=["lost_users"]

PROVENANCE FORMAT — CRITICAL:
After every answer that contains a number from a tool, append a collapsible data-validation block using this EXACT HTML structure (do not use markdown italic notes):

<details>
<summary>📊 Data Source & Query</summary>

**Source:** [value from tool result source field]
**Filters:** [value from tool result filters field]
**Formula:** [value from tool result formula field]
**SQL Query:**
```sql
[value from tool result sql field — paste it verbatim]
```

</details>

Rules:
- Always include this block after every numeric answer, even simple ones
- The SQL must be the exact query from the tool result's sql field — never paraphrase it
- If a single answer uses multiple tools, include one <details> block per tool
- If the tool returned no sql field, write "Pre-computed — see pipeline tool"
- Do NOT add any other provenance notation (no inline italic notes)
- The block is collapsed by default — stakeholders won't see it unless they click

EXCEL REPORTS:
When asked to generate a report or export data, call export_excel_report.
After the tool returns, tell the user their file is ready and what sheets/columns it contains.
The download button will appear automatically below your reply.

FORECASTING:
When asked to project or predict future metrics, call forecast_metric.
Always explain the methodology and confidence band clearly.
Never present a projection without the ±15% caveat.

LANGUAGE:
- If the user writes in Arabic → respond in Arabic
- If the user writes in English → respond in English
- Mixed messages → match the dominant language
- Use formal Gulf Arabic (الفصحى التجارية) not colloquial

NUMBER FORMATTING — ALWAYS APPLY:
Every number in your response MUST be formatted with thousand separators and correct decimals.
No exceptions — applies to inline text, tables, bullet points, and tool output values alike.

| Type | Format | Example |
|---|---|---|
| Revenue / amounts | AED X,XXX,XXX.XX (2 dp) | AED 1,356,874.22 |
| Order / user counts | X,XXX (commas, no dp) | 12,450 |
| Percentages | XX.X% (1 dp) | 23.5% |
| LTV / averages | AED X,XXX.XX (2 dp) | AED 4,231.80 |

NEVER write a raw unformatted number like 1356874 or 45678.234 — always format it.

NEVER:
- Make up numbers without calling a tool
- Say "it seems" or "it appears" — be direct
"""


# ─────────────────────────────────────────────────────────────────────────────
# Claude API client
# ─────────────────────────────────────────────────────────────────────────────

def _get_client() -> anthropic.Anthropic:
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        try:
            api_key = st.secrets.get("ANTHROPIC_API_KEY", "")
        except Exception:
            pass
    if not api_key:
        st.error("ANTHROPIC_API_KEY not found. Add it to .env or Streamlit secrets.")
        st.stop()
    return anthropic.Anthropic(api_key=api_key)


# ─────────────────────────────────────────────────────────────────────────────
# Tool execution + message building
# ─────────────────────────────────────────────────────────────────────────────

def _run_tool(name: str, inputs: dict) -> tuple[str, bytes | None]:
    result      = dispatch_tool(name, inputs)
    excel_bytes = None

    if name == "export_excel_report" and isinstance(result.get("data"), dict):
        excel_bytes = result["data"].pop("excel_bytes", None)

    payload = {
        "data":    result.get("data"),
        "source":  result.get("source", ""),
        "filters": result.get("filters", ""),
        "formula": result.get("formula", ""),
        "sql":     result.get("sql", ""),
    }
    if result.get("error"):
        payload["error"] = result["error"]

    return json.dumps(payload, default=str, ensure_ascii=False), excel_bytes


def _run_agent_turn(client: anthropic.Anthropic, messages: list, system: str) -> tuple[str, bytes | None]:
    excel_bytes = None
    windowed    = messages[-MAX_MESSAGES:] if len(messages) > MAX_MESSAGES else messages

    response = client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=system,
        tools=TOOL_DEFINITIONS,
        messages=windowed,
    )

    iterations = 0
    while response.stop_reason == "tool_use" and iterations < 8:
        iterations += 1
        tool_results = []

        for block in response.content:
            if block.type == "tool_use":
                tool_text, tool_excel = _run_tool(block.name, block.input)
                if tool_excel:
                    excel_bytes = tool_excel
                tool_results.append({
                    "type":        "tool_result",
                    "tool_use_id": block.id,
                    "content":     tool_text,
                })

        windowed = windowed + [
            {"role": "assistant", "content": response.content},
            {"role": "user",      "content": tool_results},
        ]

        response = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            system=system,
            tools=TOOL_DEFINITIONS,
            messages=windowed,
        )

    if iterations >= 8 and response.stop_reason == "tool_use":
        return "I was unable to complete this request after several attempts. Please try rephrasing your question.", excel_bytes

    final_text = ""
    for block in response.content:
        if hasattr(block, "text"):
            final_text += block.text

    return final_text.strip(), excel_bytes


# ─────────────────────────────────────────────────────────────────────────────
# Voice input (STT only — no TTS output)
# ─────────────────────────────────────────────────────────────────────────────

def _transcribe_audio(audio_bytes: bytes) -> str:
    """Transcribe audio using Google STT. Tries Arabic first, falls back to English."""
    try:
        import io
        import speech_recognition as sr
        recognizer = sr.Recognizer()
        with sr.AudioFile(io.BytesIO(audio_bytes)) as source:
            audio_data = recognizer.record(source)
        try:
            return recognizer.recognize_google(audio_data, language="ar-AE")
        except sr.UnknownValueError:
            try:
                return recognizer.recognize_google(audio_data, language="en-US")
            except sr.UnknownValueError:
                return ""
    except Exception as e:
        return f"[transcription error: {e}]"


# ─────────────────────────────────────────────────────────────────────────────
# Spinner messages
# ─────────────────────────────────────────────────────────────────────────────

_SPINNER_EN = ["Thinking…", "Analyzing data…", "Calculating…", "Validating…",
               "Querying database…", "Crunching numbers…", "Preparing answer…"]
_SPINNER_AR = ["جارٍ التفكير…", "جارٍ التحليل…", "جارٍ الحساب…", "جارٍ التحقق…",
               "جارٍ الاستعلام…", "جارٍ معالجة البيانات…", "جارٍ الإعداد…"]

def _spinner_text(lang: str) -> str:
    return random.choice(_SPINNER_AR if lang == "ar" else _SPINNER_EN)


# ─────────────────────────────────────────────────────────────────────────────
# Main render function
# ─────────────────────────────────────────────────────────────────────────────

def render_agent_page(t, h, lang: str):
    # ── Session state ─────────────────────────────────────────────────────────
    if "chat_messages" not in st.session_state:
        st.session_state.chat_messages = []
    if "pending_prompt" not in st.session_state:
        st.session_state.pending_prompt = None

    client = _get_client()
    system = build_system_prompt()

    # ── Sticky header ─────────────────────────────────────────────────────────
    import base64 as _b64
    _logo_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Zabehaty Logo 1.svg")
    try:
        with open(_logo_path, "rb") as _f:
            _logo_b64 = _b64.b64encode(_f.read()).decode()
        _logo_tag = f'<img src="data:image/svg+xml;base64,{_logo_b64}" style="width:36px;height:36px;border-radius:50%;object-fit:cover;">'
    except Exception:
        _logo_tag = '<span style="font-size:24px;">🥩</span>'

    st.markdown(f"""
    <style>
    /* ── Sticky header ─────────────────────────────────────────────────── */
    #zab-header {{
        position: sticky; top: 0; z-index: 100;
        background: rgba(255,255,255,0.93); backdrop-filter: blur(8px);
        border-bottom: 1px solid #e2e8f0;
        padding: 10px 16px; margin: -1rem -1rem 0 -1rem;
        display: flex; align-items: center; gap: 10px;
    }}
    #zab-header-title {{ font-size: 15px; font-weight: 700; color: #1e293b; line-height: 1.2; margin: 0; }}
    #zab-header-sub   {{ font-size: 10px; color: #64748b; font-weight: 600;
                         letter-spacing: 0.05em; text-transform: uppercase; margin: 0; }}
    /* ── Mic bar (in normal page flow — NO position:fixed) ─────────────── */
    #zab-mic {{
        display: block; width: 100%;
        padding: 12px 0; margin: 14px 0 6px 0;
        border: none; border-radius: 10px;
        background: #f4f4f5; color: #444;
        font-size: 14px; font-family: inherit;
        cursor: pointer; text-align: center;
        transition: background 0.15s; -webkit-tap-highlight-color: transparent;
    }}
    #zab-mic:hover  {{ background: #e8e8ea; }}
    #zab-mic.zab-rec {{
        background: #fee2e2; color: #b91c1c;
        animation: zab-pulse 1.2s ease-in-out infinite;
    }}
    @keyframes zab-pulse {{ 0%,100% {{ opacity:1; }} 50% {{ opacity:0.65; }} }}
    /* ── Expander chip groups styling ──────────────────────────────────── */
    [data-testid="stExpander"] details summary {{
        font-size: 14px; font-weight: 700;
    }}
    [data-testid="stExpander"] [data-testid="stButton"] button {{
        text-align: start; justify-content: flex-start;
        font-size: 13px; color: #475569;
        border: none; background: transparent;
        padding: 8px 4px;
    }}
    [data-testid="stExpander"] [data-testid="stButton"] button:hover {{
        color: #c0392b; background: #fdf2f2;
    }}
    /* ── Collapsible data-source blocks ─────────────────────────────────── */
    details {{
        margin-top: 8px; border: 1px solid #e0e0e0; border-radius: 6px;
        padding: 0; background: #f9f9f9; font-size: 0.82rem;
    }}
    details summary {{
        cursor: pointer; padding: 5px 10px; color: #555;
        font-weight: 500; list-style: none; user-select: none;
    }}
    details summary::-webkit-details-marker {{ display: none; }}
    details[open] summary {{ border-bottom: 1px solid #e0e0e0; }}
    details > *:not(summary) {{ padding: 8px 12px; }}
    details pre {{ font-size: 0.78rem; background: #f0f0f0; border-radius: 4px; padding: 8px; overflow-x: auto; }}
    /* ── Dark mode ───────────────────────────────────────────────────────── */
    @media (prefers-color-scheme: dark) {{
        #zab-header {{ background: rgba(15,15,25,0.93); border-color: rgba(255,255,255,0.08); }}
        #zab-header-title {{ color: #f1f5f9; }}
        #zab-mic {{ background: #1e1e2e; color: #ccc; }}
        #zab-mic:hover {{ background: #26263a; }}
        #zab-mic.zab-rec {{ background: #3b0000; color: #fca5a5; }}
    }}
    </style>

    <div id="zab-header">
      {_logo_tag}
      <div>
        <p id="zab-header-title">{t("agent_title")}</p>
        <p id="zab-header-sub">{t("agent_subtitle")}</p>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Clear conversation button ─────────────────────────────────────────────
    if st.session_state.chat_messages:
        if st.button("🗑️ " + t("agent_clear"), key="clear_conv"):
            st.session_state.chat_messages = []
            st.rerun()

    # ── Mic button (HTML, inline JS only — <script> blocks don't run in innerHTML) ──
    _mic_idle = "🎤  اضغط للتحدث  /  Tap to speak"
    _mic_rec  = "🔴  جارٍ التسجيل… اضغط للإيقاف  /  Recording… tap to stop"
    _mic_lang = "ar-AE" if lang == "ar" else "en-US"
    st.markdown(f"""
    <button id="zab-mic" type="button"
      onclick="(function(btn){{
        var SR=window.SpeechRecognition||window.webkitSpeechRecognition;
        if(!SR){{btn.textContent='Not supported';return;}}
        if(window._zabRec){{window._zabRec.stop();window._zabRec=null;return;}}
        var r=new SR();r.lang='{_mic_lang}';
        r.onstart=function(){{btn.textContent={repr(_mic_rec)};btn.style.background='#fee2e2';btn.style.color='#b91c1c';}};
        r.onend=function(){{btn.textContent={repr(_mic_idle)};btn.style.background='';btn.style.color='';window._zabRec=null;}};
        r.onerror=function(){{btn.textContent={repr(_mic_idle)};btn.style.background='';btn.style.color='';window._zabRec=null;}};
        r.onresult=function(e){{
          var ta=document.querySelector('[data-testid=\\'stChatInput\\'] textarea');
          if(!ta)return;
          var s=Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype,'value').set;
          s.call(ta,e.results[0][0].transcript);
          ta.dispatchEvent(new Event('input',{{bubbles:true}}));
          ta.dispatchEvent(new Event('change',{{bubbles:true}}));
          ta.focus();
        }};
        window._zabRec=r;
        try{{r.start();}}catch(x){{}}
      }})(this)">
      {_mic_idle}
    </button>
    """, unsafe_allow_html=True)

    # ── Grouped accordion chips — native Streamlit expander + button ──────────
    # st.button is the ONLY reliable way to handle clicks in Streamlit.
    # HTML/JS chips don't work because innerHTML doesn't execute <script> tags.
    _chip_groups = [
        ("📊", "التقارير", "Reports", [
            ("كم عدد الطلبات هذا الشهر؟",              "How many orders this month?"),
            ("ما إجمالي الإيرادات هذا الشهر؟",          "What is total revenue this month?"),
            ("أعطني تقريراً شاملاً بإكسل عن الستة أشهر الماضية", "Full Excel report for the last 6 months"),
            ("أي المتاجر تحقق أعلى مبيعات؟",            "Which shops have the highest sales?"),
        ]),
        ("👥", "العملاء", "Customers", [
            ("من هم أفضل عملائنا؟",                     "Who are our top customers?"),
            ("ما متوسط القيمة الحياتية للعميل؟",         "What is average customer LTV?"),
            ("كم عدد العملاء في خطر الانقطاع؟",          "How many users are at churn risk?"),
            ("من هم العملاء الذين فقدناهم وكيف نستعيدهم؟", "Who are lost customers and how do we win them back?"),
            ("أنشئ حملة واتساب لأفضل العملاء",           "Create a WhatsApp campaign for top customers"),
        ]),
        ("📈", "التوقعات", "Forecasts", [
            ("ما الإيرادات المتوقعة في الأسبوعين القادمين؟", "What revenue should we expect in the next 2 weeks?"),
            ("توقع المبيعات للشهر القادم",               "Forecast sales for next month"),
        ]),
        ("🔍", "الاتجاهات", "Trends", [
            ("ما أفضل فرص البيع المتقاطع؟",              "What are the top cross-selling opportunities?"),
            ("كيف توزع المستخدمون على الشرائح؟",          "What is the breakdown of user segments?"),
            ("ما أكثر أيام الأسبوع نشاطاً للطلبات؟",     "Which days of the week have the most orders?"),
        ]),
    ]

    for icon, label_ar, label_en, pairs in _chip_groups:
        label = f"{icon} {label_ar}" if lang == "ar" else f"{icon} {label_en}"
        with st.expander(label):
            for text_ar, text_en in pairs:
                chip_text = text_ar if lang == "ar" else text_en
                if st.button(chip_text, key=f"chip_{chip_text}", use_container_width=True):
                    st.session_state.pending_prompt = chip_text
                    st.rerun()

    # ── Chat history ──────────────────────────────────────────────────────────
    for i, msg in enumerate(st.session_state.chat_messages):
        role = msg["role"]
        content = msg.get("display_text") or (
            msg["content"] if isinstance(msg["content"], str) else
            " ".join(b["text"] for b in msg["content"] if isinstance(b, dict) and b.get("type") == "text")
        )
        with st.chat_message(role):
            st.markdown(content, unsafe_allow_html=True)
            if role == "assistant" and msg.get("excel_bytes"):
                st.download_button(
                    label     = t("agent_download_btn"),
                    data      = msg["excel_bytes"],
                    file_name = msg.get("excel_filename", "zabehaty_report.xlsx"),
                    mime      = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key       = f"dl_{i}",
                )

    # ── Text input ────────────────────────────────────────────────────────────
    user_text = st.chat_input(t("agent_placeholder"))

    # ── Process input ─────────────────────────────────────────────────────────
    display_text    = None
    message_content = None

    if st.session_state.pending_prompt:
        display_text    = st.session_state.pending_prompt
        message_content = st.session_state.pending_prompt
        st.session_state.pending_prompt = None
    elif user_text:
        display_text    = user_text
        message_content = user_text

    if message_content is not None:
        with st.chat_message("user"):
            st.markdown(display_text)

        st.session_state.chat_messages.append({
            "role":         "user",
            "content":      message_content,
            "display_text": display_text,
        })

        api_messages = [
            {"role": m["role"], "content": m["content"]}
            for m in st.session_state.chat_messages
        ]

        with st.chat_message("assistant"):
            with st.spinner(_spinner_text(lang)):
                try:
                    reply_text, excel_bytes = _run_agent_turn(client, api_messages, system)
                except Exception as e:
                    reply_text  = f"⚠️ Error: {e}"
                    excel_bytes = None

            st.markdown(reply_text, unsafe_allow_html=True)
            if excel_bytes:
                st.download_button(
                    label     = t("agent_download_btn"),
                    data      = excel_bytes,
                    file_name = "zabehaty_report.xlsx",
                    mime      = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key       = "dl_new",
                )

        st.session_state.chat_messages.append({
            "role":         "assistant",
            "content":      reply_text,
            "display_text": reply_text,
            "excel_bytes":  excel_bytes,
            "excel_filename": "zabehaty_report.xlsx",
        })

