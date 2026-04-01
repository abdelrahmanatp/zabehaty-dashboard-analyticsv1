"""
google_sheets.py
Pushes analysis outputs to Google Sheets.
Creates/updates one spreadsheet with multiple tabs.

Tabs created:
  - RFM Segments (summary)
  - LTV Analysis (top 500 users)
  - Product BCG Matrix
  - Category Performance
  - Shop Performance
  - Churn Risk (top at-risk users)

Requires: credentials.json or client_secret_*.json + Google Sheets/Drive APIs enabled.
On first run: browser auth flow → creates token.json for future silent runs.

Output: prints the spreadsheet URL.
"""

import os, sys, json
import pandas as pd
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(__file__))
load_dotenv()
os.makedirs(".tmp", exist_ok=True)

import gspread
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]

TOKEN_FILE = "token.json"


def get_credentials():
    """OAuth2 flow — browser on first run, silent after token.json exists."""
    creds = None

    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # Find credentials file
            creds_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")
            if not os.path.exists(creds_file):
                # Search for client_secret_*.json
                import glob
                matches = glob.glob("client_secret_*.json")
                if matches:
                    creds_file = matches[0]
                else:
                    raise FileNotFoundError(
                        "No Google credentials file found. "
                        "Place credentials.json or client_secret_*.json in project root."
                    )
            flow = InstalledAppFlow.from_client_secrets_file(creds_file, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())

    return creds


def push_to_sheets(spreadsheet_title="Zabehaty Analytics Dashboard"):
    """Push all analysis CSVs into a single Google Spreadsheet."""
    print("Authenticating with Google...")
    creds = get_credentials()
    gc    = gspread.authorize(creds)

    # Create or open spreadsheet
    sheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip()
    if sheet_id:
        try:
            sh = gc.open_by_key(sheet_id)
            print(f"Opened existing spreadsheet: {sh.url}")
        except Exception:
            sh = gc.create(spreadsheet_title)
            print(f"Created new spreadsheet: {sh.url}")
    else:
        sh = gc.create(spreadsheet_title)
        print(f"Created new spreadsheet: {sh.url}")
        print(f"  → Add this to .env: GOOGLE_SHEETS_SPREADSHEET_ID={sh.id}")

    def upsert_tab(tab_name, df, max_rows=1000):
        """Create or clear+refill a worksheet tab."""
        df = df.head(max_rows).fillna("")
        # Convert all values to strings to avoid type issues
        df = df.astype(str)
        try:
            ws = sh.worksheet(tab_name)
            ws.clear()
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title=tab_name, rows=max_rows + 5, cols=len(df.columns) + 2)

        header = df.columns.tolist()
        values = [header] + df.values.tolist()
        ws.update(values, "A1")
        print(f"  ✓ Tab '{tab_name}': {len(df)} rows")
        return ws

    # ── Load and push each dataset ────────────────────────────────────────────

    datasets = {
        "RFM Segments":        (".tmp/rfm_scores.csv",         500),
        "LTV Analysis":        (".tmp/ltv_analysis.csv",       500),
        "BCG Matrix":          (".tmp/bcg_matrix.csv",         500),
        "Top Products":        (".tmp/top_products.csv",       100),
        "Category Performance":(".tmp/category_performance.csv", 200),
        "Shop Performance":    (".tmp/shop_performance.csv",   200),
        "Churn Risk":          (".tmp/churn_risk.csv",         500),
        "Cross Category":      (".tmp/cross_category.csv",     100),
    }

    for tab_name, (path, max_rows) in datasets.items():
        if os.path.exists(path):
            df = pd.read_csv(path)
            upsert_tab(tab_name, df, max_rows)
        else:
            print(f"  ⚠ Skipping '{tab_name}' — {path} not found")

    # ── Narrative tab ─────────────────────────────────────────────────────────
    narrative_path = ".tmp/narrative_report.json"
    if os.path.exists(narrative_path):
        with open(narrative_path, encoding="utf-8") as f:
            narrative = json.load(f)

        rows = []
        for section, content in narrative.items():
            rows.append({"Section": section.replace("_", " ").title(),
                         "Content": content})
        df_narrative = pd.DataFrame(rows)
        upsert_tab("Board Report", df_narrative, 20)

    print(f"\nSpreadsheet URL: {sh.url}")
    return sh.url


if __name__ == "__main__":
    url = push_to_sheets()
    print(f"\nDone. Open: {url}")
