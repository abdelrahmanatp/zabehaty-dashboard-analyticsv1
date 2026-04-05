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
    if "last_audio_key" not in st.session_state:
        st.session_state.last_audio_key = None

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

    # ── Voice input (mic → transcribe → text) ────────────────────────────────
    st.markdown("""
    <style>
    /* ── Collapsible provenance block ── */
    details {
        margin-top: 8px;
        border: 1px solid #e0e0e0;
        border-radius: 6px;
        padding: 0;
        background: #f9f9f9;
        font-size: 0.82rem;
    }
    details summary {
        cursor: pointer;
        padding: 5px 10px;
        color: #555;
        font-weight: 500;
        list-style: none;
        user-select: none;
    }
    details summary::-webkit-details-marker { display: none; }
    details[open] summary { border-bottom: 1px solid #e0e0e0; }
    details > *:not(summary) { padding: 8px 12px; }
    details pre { font-size: 0.78rem; background: #f0f0f0; border-radius: 4px; padding: 8px; overflow-x: auto; }

    /* ── Mic icon: layered inside the chat input row, right of textarea, left of send ── */

    /* 1. Shrink & position the audio widget over the chat input's right interior */
    [data-testid="stAudioInput"] {
        position: fixed !important;
        bottom: 10px !important;
        right: 54px !important;   /* sits just left of the send/arrow button (~50px wide) */
        z-index: 1000 !important;
        width: 36px !important;
        height: 36px !important;
        overflow: hidden !important;
        background: transparent !important;
        padding: 0 !important;
        margin: 0 !important;
        border: none !important;
        box-shadow: none !important;
    }
    /* Hide label */
    [data-testid="stAudioInput"] > label { display: none !important; }
    /* Clip inner wrapper to exactly the button area */
    [data-testid="stAudioInput"] > div {
        width: 36px !important;
        height: 36px !important;
        overflow: hidden !important;
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
        gap: 0 !important;
        padding: 0 !important;
        background: transparent !important;
        border: none !important;
        box-shadow: none !important;
    }
    /* Hide the timer text (00:00) — target all common Streamlit patterns */
    [data-testid="stAudioInput"] time,
    [data-testid="stAudioInput"] [class*="time"],
    [data-testid="stAudioInput"] [class*="duration"],
    [data-testid="stAudioInput"] [class*="timer"],
    [data-testid="stAudioInput"] > div > span,
    [data-testid="stAudioInput"] > div > p { display: none !important; }

    /* 2. Push textarea text left so it doesn't slide under the mic or send button */
    [data-testid="stChatInput"] textarea {
        padding-right: 96px !important;
    }
    </style>
    """, unsafe_allow_html=True)
    audio_input = st.audio_input("🎤", key="agent_audio", label_visibility="collapsed")

    # ── Text input ────────────────────────────────────────────────────────────
    user_text = st.chat_input(t("agent_placeholder"))

    # ── Process input ─────────────────────────────────────────────────────────
    display_text    = None
    message_content = None

    if st.session_state.pending_prompt:
        display_text    = st.session_state.pending_prompt
        message_content = st.session_state.pending_prompt
        st.session_state.pending_prompt = None
    elif audio_input is not None:
        # Only process if this is a newly recorded clip (avoid re-processing on rerun)
        audio_id = id(audio_input)
        if audio_id != st.session_state.last_audio_key:
            st.session_state.last_audio_key = audio_id
            try:
                audio_bytes = audio_input.read()
            except Exception:
                audio_bytes = None
            if audio_bytes:
                with st.spinner("🎙️ Transcribing…"):
                    transcript = _transcribe_audio(audio_bytes)
                if transcript and not transcript.startswith("[transcription error"):
                    display_text    = f"🎤 {transcript}"
                    message_content = transcript
                else:
                    st.warning("Could not transcribe — please try again or type your question.")
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
                "ما هي الإيرادات المتوقعة في الأسبوعين القادمين؟",
                "أعطني تقريراً عن الربع الأول 2026 بإكسل",
                "ما هي أكثر المتاجر مبيعاً؟",
                "كم عدد العملاء في خطر الانقطاع؟",
                "كيف توزع المستخدمون على الشرائح؟",
            ]
        else:
            st.markdown("**💡 Suggested questions:**")
            starters = [
                "How many orders did we get this month?",
                "How much revenue should we expect in the next 2 weeks?",
                "Make me a Q1 2026 Excel report with: new buyers, repeat orders, total revenue, revenue per user",
                "Which shops are performing best this month?",
                "How many users are at churn risk?",
                "What's the breakdown of user segments?",
            ]

        cols = st.columns(2)
        for i, prompt in enumerate(starters):
            if cols[i % 2].button(prompt, use_container_width=True, key=f"starter_{i}"):
                st.session_state.pending_prompt = prompt
                st.rerun()
