"""
agent_page.py
Conversational AI Analytics Agent — Zabehaty Dashboard.

Features:
  - Natural-language Q&A about business data (Arabic / English auto-detect)
  - Data provenance: shows source, filters, and formula for every number
  - Answerable follow-up: "how did you get that?" always works
  - Excel export: "make me a report with X, Y, Z columns"
  - Predictive projections: "how much revenue next 2 weeks?"
  - Voice input (browser mic) + voice output (browser TTS)
"""

import os
import sys
import json
import base64
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
MAX_MESSAGES = 40          # keep last N messages in context (rolling window)
TMP          = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".tmp")

# ─────────────────────────────────────────────────────────────────────────────
# System prompt
# ─────────────────────────────────────────────────────────────────────────────

def _load_kpi_snapshot() -> str:
    """Pull a compact KPI snapshot from cached .tmp files for the system prompt."""
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
- When you give a number, ALWAYS state: where the data came from, what filters were applied, and how it was calculated
- If asked "how did you get that?" or "كيف حسبت ذلك؟" — quote the exact source, filters, and formula from the last tool result you used
- Be confident and direct — speak like a senior analyst briefing a board, not hedging

TOOLS:
You have access to live data tools. Use them to answer questions precisely.
Always prefer calling a tool over guessing from memory.

PROVENANCE FORMAT:
After each number, include a small italic note like:
*(Source: orders table · Filters: status=delivered, March 2026 · Formula: COUNT(DISTINCT id))*

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
- Reveal raw SQL to users unless they explicitly ask "show me the query"
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
    """
    Execute a tool and return (text_result_for_claude, excel_bytes_or_None).
    The text result is a JSON string containing data + provenance.
    Excel bytes are extracted and stored separately so the UI can offer a download.
    """
    result     = dispatch_tool(name, inputs)
    excel_bytes = None

    # Extract Excel bytes before serialising (they can't go in JSON)
    if name == "export_excel_report" and isinstance(result.get("data"), dict):
        excel_bytes = result["data"].pop("excel_bytes", None)

    # Build a compact provenance-rich string for Claude
    payload = {
        "data":    result.get("data"),
        "source":  result.get("source", ""),
        "filters": result.get("filters", ""),
        "formula": result.get("formula", ""),
    }
    if result.get("error"):
        payload["error"] = result["error"]

    return json.dumps(payload, default=str, ensure_ascii=False), excel_bytes


def _run_agent_turn(client: anthropic.Anthropic, messages: list, system: str) -> tuple[str, bytes | None]:
    """
    Run one full agent turn: call Claude, handle tool_use loops, return final text.
    Returns (assistant_text, excel_bytes_or_None).
    """
    excel_bytes = None

    # Strip tool_result messages if the window got too large (keep system context fresh)
    windowed = messages[-MAX_MESSAGES:] if len(messages) > MAX_MESSAGES else messages

    response = client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=system,
        tools=TOOL_DEFINITIONS,
        messages=windowed,
    )

    # Agentic loop — keep going while Claude wants to use tools (max 8 iterations)
    iterations = 0
    while response.stop_reason == "tool_use" and iterations < 8:
        iterations += 1
        tool_results = []

        for block in response.content:
            if block.type == "tool_use":
                tool_text, tool_excel = _run_tool(block.name, block.input)
                if tool_excel:
                    excel_bytes = tool_excel   # last export wins
                tool_results.append({
                    "type":        "tool_result",
                    "tool_use_id": block.id,
                    "content":     tool_text,
                })

        # Append assistant message + tool results to the conversation
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

    # Extract final text
    final_text = ""
    for block in response.content:
        if hasattr(block, "text"):
            final_text += block.text

    return final_text.strip(), excel_bytes


# ─────────────────────────────────────────────────────────────────────────────
# Voice helpers
# ─────────────────────────────────────────────────────────────────────────────

def _strip_markdown(text: str) -> str:
    """Remove markdown symbols so TTS reads clean prose, not formatting."""
    import re
    text = re.sub(r"```.*?```", "", text, flags=re.DOTALL)
    text = re.sub(r"`[^`]*`", "", text)
    text = re.sub(r"\*{1,3}([^*]+)\*{1,3}", r"\1", text)
    text = re.sub(r"_{1,3}([^_]+)_{1,3}", r"\1", text)
    text = re.sub(r"^#{1,6}\s+", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s*[-*+]\s+", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s*\d+\.\s+", "", text, flags=re.MULTILINE)
    text = re.sub(r"\*\(.*?\)\*", "", text)
    text = re.sub(r"\(.*?Source:.*?\)", "", text)
    text = re.sub(r"https?://\S+", "", text)
    text = re.sub(r"\n+", " ", text)
    text = re.sub(r"  +", " ", text)
    return text.strip()


def _get_spoken_summary(text: str) -> str:
    """Extract first 2-3 complete natural sentences for TTS — no tables, no markup."""
    import re
    clean = _strip_markdown(text)
    # Split on sentence boundaries
    sentences = re.split(r'(?<=[.!?؟])\s+', clean)
    # Skip lines that look like table rows or numbers-only lines
    prose = [s for s in sentences if len(s) > 20 and not re.match(r'^[\d,.\s|%-]+$', s)]
    return " ".join(prose[:3])[:500]


def _detect_lang(text: str, default_lang: str) -> str:
    """Return 'ar' if text contains Arabic characters, else default_lang."""
    import re
    return "ar" if re.search(r'[\u0600-\u06FF]', text) else default_lang


def _speak(text: str, lang: str, tts_id: str):
    """Inject a sessionStorage-gated TTS call — safe across Streamlit reruns."""
    bcp_lang = "ar-AE" if lang == "ar" else "en-US"
    spoken   = _get_spoken_summary(text).replace("\\", "").replace('"', '\\"')
    js = f"""
    <script>
    (function() {{
        var id = "{tts_id}";
        if (sessionStorage.getItem("tts_last_id") === id) return;
        sessionStorage.setItem("tts_last_id", id);
        if (!('speechSynthesis' in window)) return;
        window.speechSynthesis.cancel();
        var u = new SpeechSynthesisUtterance("{spoken}");
        u.lang = "{bcp_lang}";
        u.rate = 0.95;
        window.speechSynthesis.speak(u);
    }})();
    </script>
    """
    st.components.v1.html(js, height=0)


def _stop_speaking(stop_id: str):
    """Write a sentinel to sessionStorage and cancel TTS — persists across reruns."""
    js = f"""
    <script>
    (function() {{
        sessionStorage.setItem("tts_last_id", "stop_{stop_id}");
        if ('speechSynthesis' in window) window.speechSynthesis.cancel();
    }})();
    </script>
    """
    st.components.v1.html(js, height=0)


def _transcribe_audio(audio_bytes: bytes) -> str:
    """
    Transcribe audio bytes to text using Google STT (free, no API key needed).
    Tries Arabic first (primary platform language), falls back to English.
    Returns the transcription string, or an empty string if nothing was heard.
    """
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
# Main render function
# ─────────────────────────────────────────────────────────────────────────────

def render_agent_page(t, h, lang: str):
    """
    Render the AI Analyst page.
    t  — translation function from app.py
    h  — help text function from app.py
    lang — current language ("en" or "ar")
    """
    # ── Session state init ────────────────────────────────────────────────────
    if "chat_messages" not in st.session_state:
        st.session_state.chat_messages = []
    if "used_voice" not in st.session_state:
        st.session_state.used_voice = False
    if "pending_prompt" not in st.session_state:
        st.session_state.pending_prompt = None
    if "tts_id" not in st.session_state:
        st.session_state.tts_id = 0
    if "tts_text" not in st.session_state:
        st.session_state.tts_text = None
    if "tts_lang" not in st.session_state:
        st.session_state.tts_lang = lang

    client = _get_client()
    system = build_system_prompt()

    # ── Header row ────────────────────────────────────────────────────────────
    hcol1, hcol2, hcol3 = st.columns([3, 1, 1])
    with hcol1:
        st.title(t("agent_title"))
        st.caption(t("agent_subtitle"))
    with hcol2:
        if st.button("⏹ Stop", use_container_width=True, help="Stop voice playback"):
            st.session_state.tts_id  += 1
            st.session_state.tts_text = None
            _stop_speaking(str(st.session_state.tts_id))
    with hcol3:
        if st.button(t("agent_clear"), use_container_width=True):
            st.session_state.chat_messages = []
            st.session_state.used_voice    = False
            st.session_state.tts_text      = None
            st.session_state.tts_id       += 1
            _stop_speaking(str(st.session_state.tts_id))
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
            st.markdown(content)
            if role == "assistant":
                # Inline download button
                if msg.get("excel_bytes"):
                    st.download_button(
                        label     = t("agent_download_btn"),
                        data      = msg["excel_bytes"],
                        file_name = msg.get("excel_filename", "zabehaty_report.xlsx"),
                        mime      = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key       = f"dl_{i}",
                    )
                # Play audio button — user-initiated TTS only
                if st.button("🔊", key=f"play_{i}", help="Play audio"):
                    st.session_state.tts_id  += 1
                    st.session_state.tts_text = content
                    st.session_state.tts_lang = _detect_lang(content, lang)
                    st.rerun()

    # ── Voice input — CSS-fixed next to the chat input bar ───────────────────
    st.markdown("""
    <style>
    [data-testid="stAudioInput"] {
        position: fixed;
        bottom: 10px;
        right: 72px;
        z-index: 999;
        background: transparent;
    }
    [data-testid="stAudioInput"] > label { display: none; }
    </style>
    """, unsafe_allow_html=True)
    audio_input = st.audio_input("🎤", key="agent_audio", label_visibility="collapsed")

    # ── Text input ────────────────────────────────────────────────────────────
    user_text = st.chat_input(t("agent_placeholder"))

    # ── Process input ─────────────────────────────────────────────────────────
    display_text  = None   # what to show in the user bubble
    message_content = None # what to send to Claude

    if st.session_state.pending_prompt:
        # Starter chip was clicked on the previous run
        display_text    = st.session_state.pending_prompt
        message_content = st.session_state.pending_prompt
        st.session_state.pending_prompt = None
        st.session_state.used_voice = False
    elif audio_input is not None:
        # Voice input: transcribe in Python then send as plain text
        try:
            audio_bytes = audio_input.read()
        except Exception:
            audio_bytes = None
        if audio_bytes:
            with st.spinner("🎙️ Transcribing..."):
                transcript = _transcribe_audio(audio_bytes)
            if transcript and not transcript.startswith("[transcription error"):
                display_text    = f"🎤 {transcript}"
                message_content = transcript
                st.session_state.used_voice = True
                # Detect language from transcript for accurate TTS reply
                st.session_state.tts_lang = _detect_lang(transcript, lang)
            else:
                st.warning("Could not transcribe audio — please try again or type your question.")
        else:
            st.warning("Could not read audio — please try again.")
    elif user_text:
        display_text    = user_text
        message_content = user_text
        st.session_state.used_voice = False

    if message_content is not None:
        # Show user bubble
        with st.chat_message("user"):
            st.markdown(display_text)

        # Append to history (store display_text separately from raw content)
        st.session_state.chat_messages.append({
            "role":         "user",
            "content":      message_content,
            "display_text": display_text,
        })

        # Build messages list for Claude (only role + content, no display_text)
        api_messages = [
            {"role": m["role"], "content": m["content"]}
            for m in st.session_state.chat_messages
        ]

        # Run agent
        with st.chat_message("assistant"):
            with st.spinner(t("agent_thinking")):
                try:
                    reply_text, excel_bytes = _run_agent_turn(client, api_messages, system)
                except Exception as e:
                    reply_text  = f"⚠️ Error: {e}"
                    excel_bytes = None

            st.markdown(reply_text)
            if excel_bytes:
                st.download_button(
                    label     = t("agent_download_btn"),
                    data      = excel_bytes,
                    file_name = "zabehaty_report.xlsx",
                    mime      = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key       = f"dl_new",
                )

        # Save assistant reply (store excel_bytes so the button persists on re-render)
        st.session_state.chat_messages.append({
            "role":         "assistant",
            "content":      reply_text,
            "display_text": reply_text,
            "excel_bytes":  excel_bytes,
            "excel_filename": "zabehaty_report.xlsx",
        })

    # ── Single TTS renderer — fires only when tts_text is set by Play button ──
    if st.session_state.get("tts_text"):
        _speak(
            st.session_state.tts_text,
            st.session_state.tts_lang,
            str(st.session_state.tts_id),
        )

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
