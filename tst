import json
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Tuple

import pandas as pd
import plotly.express as px
import streamlit as st

# ✅ Import from your Snowflake helper
from snowflake_conn import get_snowflake_connection, run_query


# =============================================================
# CONFIG LOADING
# =============================================================
def load_config(path: str = "dashboard_config.json") -> Dict[str, Any]:
    """Load JSON dashboard configuration."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# =============================================================
# THEME / STYLING
# =============================================================
def apply_theme(theme: Dict[str, Any]):
    css = f"""
    <style>
      :root {{
        --primary: {theme.get('primary', '#1565C0')};
        --secondary: {theme.get('secondary', '#26A69A')};
        --bg: {theme.get('background', '#FFFFFF')};
        --text: {theme.get('text', '#1F2937')};
      }}
      .stApp {{ background: var(--bg); color: var(--text); }}
      .metric-good {{ border-left: 6px solid {theme.get('kpi_good', '#2E7D32')}; padding-left: 8px; }}
      .metric-warn {{ border-left: 6px solid {theme.get('kpi_warn', '#F9A825')}; padding-left: 8px; }}
      .metric-bad  {{ border-left: 6px solid {theme.get('kpi_bad', '#C62828')}; padding-left: 8px; }}
      .kpi-card {{ background: white; border-radius: 12px; padding: 12px 16px; box-shadow: 0 1px 3px rgba(0,0,0,.08); }}
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)


# =============================================================
# FILTER HELPERS
# =============================================================
def default_date_range(days_back: int = 7):
    end = datetime.utcnow().date()
    start = end - timedelta(days=days_back)
    return start, end


def render_filters(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Render filters dynamically from JSON config."""
    st.sidebar.header("Filters")
    filters = {}
    for f in cfg.get("filters", []):
        f_id = f["id"]
        f_type = f.get("type", "text")
        label = f.get("label", f_id)

        if f_type == "date_range":
            start, end = default_date_range(f.get("default", {}).get("days_back", 7))
            sel = st.sidebar.date_input(label, (start, end))
            filters[f_id] = {"start": sel[0], "end": sel[1]}
        elif f_type == "multiselect":
            df_vals = run_query(
                f"SELECT DISTINCT {f['bound_column']} AS v FROM {cfg['datasets'][0]['snowflake_view']} WHERE {f['bound_column']} IS NOT NULL ORDER BY 1"
            )
            opts = df_vals["v"].tolist() if not df_vals.empty else []
            filters[f_id] = st.sidebar.multiselect(label, opts)
        elif f_type == "search":
            filters[f_id] = st.sidebar.text_input(label, "")
        else:
            filters[f_id] = st.sidebar.text_input(label, "")
    return filters


# =============================================================
# DATA FETCH
# =============================================================
def fetch_data(cfg: Dict[str, Any], filters: Dict[str, Any]) -> pd.DataFrame:
    dataset = cfg["datasets"][0]  # Single dataset for now
    view = dataset["snowflake_view"]

    where_clauses = []
    params = []

    for fid, val in filters.items():
        if isinstance(val, dict) and "start" in val:
            where_clauses.append(f"{dataset['time_column']} BETWEEN %s AND %s")
            params += [val["start"], val["end"]]
        elif isinstance(val, list) and val:
            placeholders = ",".join(["%s"] * len(val))
            where_clauses.append(f"{fid.upper()} IN ({placeholders})")
            params += val
        elif isinstance(val, str) and val.strip():
            where_clauses.append(f"{fid.upper()} ILIKE %s")
            params.append(f"%{val}%")

    where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    sql = f"SELECT * FROM {view}{where_sql} LIMIT 20000"

    df = run_query(sql, tuple(params))
    return df


# =============================================================
# KPI LOGIC
# =============================================================
def show_kpis(df: pd.DataFrame):
    st.subheader("Key Metrics")
    if df.empty:
        st.warning("No data found for filters.")
        return

    freshness = (datetime.utcnow() - pd.to_datetime(df["EDL_INCRMNTL_LOAD_DTM"]).max()).seconds / 60
    total_trgt = df["TRGT_RCRD_CNT"].sum()
    total_stg = df["STG_RCRD_CNT"].sum()
    success_rate = total_trgt / total_stg if total_stg else 0

    cols = st.columns(3)
    cols[0].metric("Freshness (minutes)", f"{freshness:.0f}")
    cols[1].metric("Load Success Rate", f"{success_rate*100:.2f}%")
    cols[2].metric("Total Target Records", f"{total_trgt:,}")


# =============================================================
# MAIN APP
# =============================================================
def main():
    cfg = load_config()
    st.set_page_config(page_title=cfg["app"]["title"], layout="wide")
    apply_theme(cfg["theme"])

    st.title(cfg["app"]["title"])

    # ---- Connection Test Button ----
    st.sidebar.markdown("**Connection**")
    if st.sidebar.button("Test Snowflake connection"):
        try:
            df_test = run_query(
                "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()"
            )
            st.success("✅ Connection successful!")
            st.dataframe(df_test)
        except Exception as e:
            st.error(f"❌ Connection failed: {e}")

    # ---- Filters ----
    filters = render_filters(cfg)

    # ---- Data ----
    with st.spinner("Loading data..."):
        df = fetch_data(cfg, filters)

    # ---- KPIs ----
    show_kpis(df)

    # ---- Table ----
    st.subheader("Detailed Records")
    st.dataframe(df, use_container_width=True)


if __name__ == "__main__":
    main()
