"""
snowflake_conn.py

Reusable connection helper for Snowflake with built-in self-test.

Priority for credentials (first found wins):
1. Env var  SNOWFLAKE_CREDENTIALS_JSON → points to a JSON file
2. Local file ./snowflake_credentials.json
3. Streamlit secrets [.streamlit/secrets.toml] section [snowflake]

Supports:
- Password authentication  (add "password")
- SSO / External browser / OAuth  (add "authenticator": "externalbrowser")
"""

import json
import os
from typing import Any, Dict, Tuple
import pandas as pd
import snowflake.connector as sf

# Optional: Streamlit import for dashboard integration
try:
    import streamlit as st
except ImportError:
    st = None


# =============================================================
# LOAD CREDENTIALS
# =============================================================
def _load_creds() -> Dict[str, Any]:
    """Load Snowflake credentials from JSON or Streamlit secrets."""
    # 1) Environment variable path
    path = os.environ.get("SNOWFLAKE_CREDENTIALS_JSON")
    if path and os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    # 2) Local default file
    if os.path.exists("snowflake_credentials.json"):
        with open("snowflake_credentials.json", "r", encoding="utf-8") as f:
            return json.load(f)

    # 3) Streamlit secrets
    if st is not None:
        creds = st.secrets.get("snowflake", None)
        if creds:
            return dict(creds)

    raise RuntimeError(
        "❌ No Snowflake credentials found.\n"
        "Provide one of:\n"
        "- Env var SNOWFLAKE_CREDENTIALS_JSON pointing to a JSON file\n"
        "- snowflake_credentials.json in this folder\n"
        "- or .streamlit/secrets.toml [snowflake]"
    )


# =============================================================
# CONNECTION CREATOR
# =============================================================
def get_snowflake_connection():
    """Return a live Snowflake connection."""
    creds = _load_creds()

    base_kwargs = dict(
        account=creds["account"],
        user=creds["user"],
        role=creds.get("role"),
        warehouse=creds.get("warehouse"),
        database=creds.get("database"),
        schema=creds.get("schema"),
        client_session_keep_alive=True,
    )

    # Authentication
    if "authenticator" in creds and creds["authenticator"]:
        base_kwargs["authenticator"] = creds["authenticator"]
    else:
        if not creds.get("password"):
            raise RuntimeError("No password found and no authenticator set.")
        base_kwargs["password"] = creds["password"]

    return sf.connect(**base_kwargs)


# =============================================================
# QUERY RUNNER
# =============================================================
def run_query(sql: str, params: Tuple[Any, ...] | None = None) -> pd.DataFrame:
    """Run SQL and return pandas DataFrame."""
    if params is None:
        params = ()
    with get_snowflake_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            cols = [c[0] for c in cur.description]
    return pd.DataFrame(rows, columns=cols)


# =============================================================
# SELF-TEST (Run this file directly)
# =============================================================
if __name__ == "__main__":
    print("🔍 Testing Snowflake connection ...")

    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()

        # Show current context
        cur.execute(
            "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()"
        )
        print("✅ Connection successful:")
        for row in cur.fetchall():
            print("  USER:", row[0], "ROLE:", row[1], "WAREHOUSE:", row[2], "DB:", row[3], "SCHEMA:", row[4])

        # 🔹 Change this line to your table name
        table_name = "P01_HOSCDA.HOSCDA.HLTH_OS_VLDTN_CNTRL_AUDT"

        # Run a simple test query
        sql = f"SELECT COUNT(*) AS ROW_COUNT FROM {table_name}"
        cur.execute(sql)
        count = cur.fetchone()[0]
        print(f"📊 Row count in {table_name}: {count}")

        cur.close()
        conn.close()
        print("✅ Test completed successfully.")

    except Exception as e:
        print(f"❌ Connection or query failed: {e}")
