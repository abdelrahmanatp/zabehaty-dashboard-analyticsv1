"""
db_connect.py
Shared MySQL connection helper used by all tools.
Works in two environments:
  - Local dev  : reads credentials from .env via load_dotenv()
  - Streamlit Cloud : reads credentials from st.secrets and injects into os.environ
"""

import os
import mysql.connector
from dotenv import load_dotenv

# ── Streamlit Cloud: inject secrets into env before dotenv runs ──────────────
try:
    import streamlit as st
    _s = st.secrets
    for _key in ("ZABEHATY_DB_HOST", "ZABEHATY_DB_PORT", "ZABEHATY_DB_USER",
                 "ZABEHATY_DB_PASSWORD", "ZABEHATY_DB_NAME", "ANTHROPIC_API_KEY"):
        if _key in _s:
            os.environ.setdefault(_key, str(_s[_key]))
except Exception:
    pass  # Not running under Streamlit — fall through to dotenv

load_dotenv()


def get_connection():
    """Return a live MySQL connection using .env credentials."""
    return mysql.connector.connect(
        host=os.getenv("ZABEHATY_DB_HOST"),
        port=int(os.getenv("ZABEHATY_DB_PORT", 3306)),
        user=os.getenv("ZABEHATY_DB_USER"),
        password=os.getenv("ZABEHATY_DB_PASSWORD"),
        database=os.getenv("ZABEHATY_DB_NAME"),
        connection_timeout=30,
        charset="utf8mb4",
    )


def query(sql, params=None, dictionary=True):
    """Run a SELECT and return all rows."""
    conn = get_connection()
    cursor = conn.cursor(dictionary=dictionary)
    cursor.execute(sql, params or ())
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows


def query_df(sql, params=None):
    """Run a SELECT and return a pandas DataFrame."""
    import pandas as pd
    conn = get_connection()
    df = pd.read_sql(sql, conn, params=params)
    conn.close()
    return df


def load_synthetic_orders_df():
    """Load synthetic gap orders (Dec 2025–Feb 2026) from local SQLite.
    Returns an empty DataFrame if the synthetic DB has not been generated yet.
    Run tools/generate_synthetic_gap.py to create it.
    """
    import sqlite3
    import pandas as pd
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".tmp", "synthetic_gap.db")
    if not os.path.exists(db_path):
        return pd.DataFrame()
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query("SELECT * FROM orders", conn)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


if __name__ == "__main__":
    rows = query("SELECT COUNT(*) as user_count FROM `user`")
    print(f"Connection OK — {rows[0]['user_count']:,} users in DB")
