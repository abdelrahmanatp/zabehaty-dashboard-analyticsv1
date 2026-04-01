"""
db_introspect.py
Connects to MySQL, maps all tables with row counts and columns.
Output written to .tmp/schema_map.txt
"""

import os, sys
from dotenv import load_dotenv
import mysql.connector

load_dotenv()

def get_connection():
    return mysql.connector.connect(
        host=os.getenv("ZABEHATY_DB_HOST"),
        port=int(os.getenv("ZABEHATY_DB_PORT", 3306)),
        user=os.getenv("ZABEHATY_DB_USER"),
        password=os.getenv("ZABEHATY_DB_PASSWORD"),
        database=os.getenv("ZABEHATY_DB_NAME"),
        connection_timeout=15
    )

def introspect():
    conn = get_connection()
    cursor = conn.cursor()

    # All tables
    cursor.execute("SHOW TABLES")
    tables = [r[0] for r in cursor.fetchall()]
    print(f"\n{'='*60}")
    print(f"DATABASE: {os.getenv('ZABEHATY_DB_NAME')}  |  {len(tables)} tables")
    print(f"{'='*60}\n")

    os.makedirs(".tmp", exist_ok=True)
    lines = [f"DATABASE: {os.getenv('ZABEHATY_DB_NAME')}\nTABLE COUNT: {len(tables)}\n\n"]

    for table in sorted(tables):
        # Row count
        try:
            cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
            row_count = cursor.fetchone()[0]
        except Exception:
            row_count = "ERR"

        # Columns
        cursor.execute(f"DESCRIBE `{table}`")
        cols = cursor.fetchall()

        header = f"TABLE: {table}  ({row_count:,} rows)" if isinstance(row_count, int) else f"TABLE: {table}  (rows: {row_count})"
        print(header)
        lines.append(header + "\n")

        for col in cols:
            field, typ, null, key, default, extra = col
            flags = []
            if key == "PRI": flags.append("PK")
            if key == "MUL": flags.append("FK/IDX")
            if key == "UNI": flags.append("UNIQUE")
            if extra: flags.append(extra)
            flag_str = f"  [{', '.join(flags)}]" if flags else ""
            line = f"  {field:<35} {typ:<25} {flag_str}"
            print(line)
            lines.append(line + "\n")

        print()
        lines.append("\n")

    cursor.close()
    conn.close()

    with open(".tmp/schema_map.txt", "w") as f:
        f.writelines(lines)

    print(f"\nSchema saved to .tmp/schema_map.txt")

if __name__ == "__main__":
    introspect()
