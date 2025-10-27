# snowflake_conn.py
"""
Connection helper for Snowflake.

Priority order for credentials (first found wins):
1) JSON file path from env var SNOWFLAKE_CREDENTIALS_JSON
2) Local file ./snowflake_credentials.json
3) Streamlit secrets at [snowflake] in .streamlit/secrets.toml

Supports:
- Password auth (provide "password")
- SSO/OAuth flows by setting "authenticator" (e.g., "externalbrowser")
"""
from __future__ import annotations
import json
import os
from typing import Any, Dict, Tuple

import pandas as pd
import snowflake.connector as sf

# Streamlit is optional so this module can be reused in non-Streamlit Python.
try:
    import streamlit as st  # type: ignore
except Exception:  # pragma: no cover
    st = None  # fallback when not running under Streamlit


def _load_creds() -> Dict[str, Any]:
    # 1) Env var path
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
        creds = st.secrets.get("snowflake", None)  # type: ignore[attr-defined]
        if creds:
            # Convert to normal dict (Streamlit's Secrets object behaves like a dict)
            return dict(creds)

    raise RuntimeError(
        "No Snowflake credentials found. Provide SNOWFLAKE_CREDENTIALS_JSON, "
        "a local snowflake_credentials.json file, or .streamlit/secrets.toml [snowflake]."
    )


def get_snowflake_connection():
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

    # Choose auth method
    auth = creds.get("authenticator")
    if auth:
        base_kwargs["authenticator"] = auth
    else:
        pwd = creds.get("password")
        if not pwd:
            raise RuntimeError("No password provided and no authenticator set. Add one of them.")
        base_kwargs["password"] = pwd

    return sf.connect(**base_kwargs)


def run_query(sql: str, params: Tuple[Any, ...] | None = None) -> pd.DataFrame:
    if params is None:
        params = ()
    with get_snowflake_connection() as con:
        with con.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            cols = [c[0] for c in cur.description]
    return pd.DataFrame(rows, columns=cols)
