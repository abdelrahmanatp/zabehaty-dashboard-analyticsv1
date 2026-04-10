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

    # ── Header ────────────────────────────────────────────────────────────────
    hcol1, hcol2 = st.columns([8, 1])
    with hcol1:
        st.title(t("agent_title"))
        st.caption(t("agent_subtitle"))
    with hcol2:
        # Only show trash icon when there is an active conversation
        if st.session_state.chat_messages:
            if st.button("🗑️", key="clear_conv", help=t("agent_clear")):
                st.session_state.chat_messages = []
                st.rerun()

    st.divider()

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

    # ── CSS: collapsible provenance blocks ───────────────────────────────────
    st.markdown("""
    <style>
    details {
        margin-top: 8px; border: 1px solid #e0e0e0; border-radius: 6px;
        padding: 0; background: #f9f9f9; font-size: 0.82rem;
    }
    details summary {
        cursor: pointer; padding: 5px 10px; color: #555;
        font-weight: 500; list-style: none; user-select: none;
    }
    details summary::-webkit-details-marker { display: none; }
    details[open] summary { border-bottom: 1px solid #e0e0e0; }
    details > *:not(summary) { padding: 8px 12px; }
    details pre { font-size: 0.78rem; background: #f0f0f0; border-radius: 4px; padding: 8px; overflow-x: auto; }
    </style>
    """, unsafe_allow_html=True)

    # ── Mic button: injected into document.body, kept alive via MutationObserver ─
    # The button is appended to document.body (not inside a Streamlit component)
    # so React cannot remove it during rerenders. A MutationObserver recreates it
    # immediately if it ever disappears. Speech recognition state lives in the JS
    # closure and survives Streamlit rerenders without resetting.
    st.markdown("""
    <style>
    /* Narrow the chat input so text doesn't run under the mic button */
    [data-testid="stBottom"] > div > div {
        padding-right: 58px !important;
    }
    /* Mic button styles — injected into body by JS below */
    #zab-mic-btn {
        position: fixed;
        bottom: 12px;
        right: 14px;
        z-index: 99999;
        width: 44px;
        height: 44px;
        border-radius: 10px;
        border: 1.5px solid rgba(100,100,100,0.25);
        background: #ffffff;
        cursor: pointer;
        font-size: 20px;
        display: flex !important;
        align-items: center;
        justify-content: center;
        box-shadow: 0 2px 8px rgba(0,0,0,0.14);
        user-select: none;
        -webkit-user-select: none;
        transition: background 0.15s, border-color 0.15s, transform 0.1s;
        -webkit-tap-highlight-color: transparent;
    }
    #zab-mic-btn:hover  { border-color: rgba(100,100,100,0.5); transform: scale(1.05); }
    #zab-mic-btn:active { transform: scale(0.96); }
    #zab-mic-btn.zab-rec { background: #fee2e2; border-color: #f87171; }
    @media (prefers-color-scheme: dark) {
        #zab-mic-btn { background: #2a2a3a; border-color: rgba(255,255,255,0.2); }
        #zab-mic-btn.zab-rec { background: #4a0000; border-color: #f87171; }
    }
    @media (max-width: 640px) {
        #zab-mic-btn { width: 40px; height: 40px; font-size: 18px; bottom: 10px; right: 10px; }
        [data-testid="stBottom"] > div > div { padding-right: 54px !important; }
    }
    </style>

    <script>
    (function() {
        // All state lives in this closure — survives Streamlit rerenders
        var listening = false;
        var recognition = null;

        function getLang() {
            return (document.documentElement.dir === 'rtl' || document.body.dir === 'rtl')
                ? 'ar-AE' : 'en-US';
        }

        function fillInput(text) {
            var chatEl = document.querySelector('[data-testid="stChatInput"]');
            if (!chatEl) return;
            var ta = chatEl.querySelector('textarea');
            if (!ta) return;
            var setter = Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype, 'value').set;
            setter.call(ta, text);
            ta.dispatchEvent(new Event('input',  { bubbles: true }));
            ta.dispatchEvent(new Event('change', { bubbles: true }));
            ta.focus();
        }

        function buildRecognition(btn) {
            var SR = window.SpeechRecognition || window.webkitSpeechRecognition;
            if (!SR) { btn.title = 'Voice not supported in this browser'; btn.style.opacity = '0.35'; return null; }
            var r = new SR();
            r.continuous = false;
            r.interimResults = false;
            r.onstart  = function() { listening = true;  btn.innerHTML = '🔴'; btn.classList.add('zab-rec'); };
            r.onend    = function() { listening = false; btn.innerHTML = '🎤'; btn.classList.remove('zab-rec'); };
            r.onerror  = function() { listening = false; btn.innerHTML = '🎤'; btn.classList.remove('zab-rec'); recognition = null; };
            r.onresult = function(ev) { fillInput(ev.results[0][0].transcript); };
            return r;
        }

        function createMic() {
            if (document.getElementById('zab-mic-btn')) return;
            var btn = document.createElement('button');
            btn.id        = 'zab-mic-btn';
            btn.type      = 'button';
            btn.innerHTML = '🎤';
            btn.title     = 'Tap to speak';
            btn.addEventListener('click', function(e) {
                e.preventDefault(); e.stopPropagation();
                if (!recognition) recognition = buildRecognition(btn);
                if (!recognition) return;
                if (listening) { recognition.stop(); }
                else { try { recognition.lang = getLang(); recognition.start(); } catch(_) { recognition = null; } }
            });
            document.body.appendChild(btn);
        }

        // Create on first load
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', createMic);
        } else {
            createMic();
        }

        // Recreate if Streamlit's React ever removes it during a rerender
        new MutationObserver(function() {
            if (!document.getElementById('zab-mic-btn')) createMic();
        }).observe(document.body, { childList: true, subtree: true });
    })();
    </script>
    """, unsafe_allow_html=True)

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

    # ── Starter prompts (shown when conversation is empty) ────────────────────
    if not st.session_state.chat_messages:
        st.markdown("---")
        if lang == "ar":
            st.markdown("**💡 أسئلة مقترحة:**")
            starters = [
                "كم عدد الطلبات هذا الشهر؟",
                "ما متوسط القيمة الحياتية للعميل؟",
                "أعطني تقريراً شاملاً عن الستة أشهر الماضية بإكسل",
                "ما هي أفضل فرص البيع المتقاطع لزيادة مبيعاتنا؟",
                "من هم عملاؤنا الأكثر قيمة وما الأفضل لاستهدافهم؟",
                "أنشئ حملة واتساب مخصصة لأفضل عملائنا",
                "من هم العملاء الذين فقدناهم وكيف نستعيدهم؟",
                "كم عدد العملاء في خطر الانقطاع؟",
                "ما هي الإيرادات المتوقعة في الأسبوعين القادمين؟",
                "كيف توزع المستخدمون على الشرائح؟",
            ]
        else:
            st.markdown("**💡 Suggested questions:**")
            starters = [
                "How many orders did we get this month?",
                "What is our average customer LTV?",
                "Give me a full business performance report for the last 6 months as Excel",
                "What are the top cross-selling opportunities to boost sales?",
                "Generate a personalised WhatsApp promo campaign for our top customers",
                "Who are our lost high-value customers and how do we win them back?",
                "Which shops are performing best this month?",
                "How many users are at churn risk?",
                "What revenue should we expect in the next 2 weeks?",
                "What's the breakdown of user segments?",
            ]

        cols = st.columns(2)
        for i, prompt in enumerate(starters):
            if cols[i % 2].button(prompt, use_container_width=True, key=f"starter_{i}"):
                st.session_state.pending_prompt = prompt
                st.rerun()
