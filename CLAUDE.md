# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Create and activate virtual environment (Windows)
python -m venv .venv
.venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run a specific tool
python tools/<script_name>.py

# Add a new dependency
pip install <package> && pip freeze > requirements.txt

# Test DB connection
python tools/db_connect.py
```

---

## Project Purpose

This is an **e-commerce user analytics agent** that connects to a MySQL database, deeply analyzes user behaviour, product performance, and vendor data, then produces actionable reports and communication strategies for business stakeholders.

Dual deliverable:
1. **Autonomous Python agent** — runs end-to-end analysis, generates reports, delivers to cloud
2. **n8n workflow** — equivalent logic expressed as an importable n8n JSON workflow

---

## WAT Architecture

This project follows the **WAT framework** (Workflows → Agent → Tools):

**Layer 1: `workflows/`** — Markdown SOPs. Each file defines the objective, required inputs, tool sequence, expected outputs, and edge case handling. These are the agent's instructions. Do not overwrite without asking the user.

**Layer 2: Agent (Claude Code)** — Reads workflows, sequences tool calls, handles failures, asks when blocked. Never executes business logic directly — delegates to tools.

**Layer 3: `tools/`** — Deterministic Python scripts. Each script does one thing: query the DB, transform data, call an LLM, write to Google Sheets, etc. All credentials come from `.env`.

**Why:** If each AI step is 90% accurate, five chained steps = 59% overall. Offloading execution to deterministic scripts keeps the agent focused on orchestration.

---

## Tool Design Conventions

- Every tool in `tools/` is a standalone executable script with a `if __name__ == "__main__"` block for direct testing
- Tools read config from `.env` via `python-dotenv`
- Tools write outputs to `.tmp/` for intermediate state; final deliverables go to cloud (Google Sheets, Slides, etc.)
- Tools should print a clear success/failure summary when run directly
- If a tool makes paid API calls (Claude, OpenAI, Google), check with the user before running it for the first time or re-running after failure

---

## Expected Directory Structure

```
.tmp/               # Intermediate files (regenerable, disposable)
tools/              # Python execution scripts
workflows/          # Markdown SOPs
n8n/                # n8n workflow JSON exports
.env                # All secrets and credentials (never commit)
requirements.txt
CLAUDE.md
```

---

## Core Analysis Domains

The agent covers these analytical areas — each should map to a workflow + set of tools:

1. **User Behaviour & Segmentation** — RFM scoring, cohort analysis, churn risk, session patterns
2. **Buying Patterns** — basket analysis, repeat purchase rates, time-to-reorder, cross-sell affinities
3. **LTV Analysis** — historical LTV per user/segment, predictive LTV modelling
4. **Product Performance** — BCG matrix classification (Stars/Cash Cows/Question Marks/Dogs), sell-through rates, return rates, margin analysis
5. **Vendor Analysis** — vendor contribution to GMV, product quality signals, fulfilment reliability
6. **Communication Strategy** — segment-to-channel mapping, message tone/timing recommendations, campaign brief generation
7. **Board Report** — executive summary synthesising all of the above into a decision-ready format

---

## n8n Workflow Notes

- The n8n workflow is a JSON file importable via n8n's UI (`workflows/` or `n8n/` directory)
- Each n8n node maps to one tool's responsibility
- MySQL nodes handle DB queries; Code nodes handle transformation; HTTP Request nodes call the Claude API; Google Sheets/Slides nodes handle output
- The workflow should be self-contained and triggerable on a schedule or manually

---

## Self-Improvement Loop

When a tool fails:
1. Read the full error and trace
2. Fix the script and retest
3. Update the relevant workflow with any discovered constraints (rate limits, schema quirks, etc.)
4. Move on with a stronger system

---

## Credentials (.env keys expected)

```
MYSQL_HOST=
MYSQL_PORT=
MYSQL_DB=
MYSQL_USER=
MYSQL_PASSWORD=
ANTHROPIC_API_KEY=
GOOGLE_SHEETS_SPREADSHEET_ID=
```

Add others as integrations are added (n8n webhook URL, SendGrid, etc.).
