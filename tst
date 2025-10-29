"""
Reusable Snowflake connection + query helpers.

Credential priority (first found wins):
1) Env var SNOWFLAKE_CREDENTIALS_JSON -> path to a JSON file
2) Local ./snowflake_credentials.json
3) Streamlit secrets: .streamlit/secrets.toml [snowflake]

Auth:
- SSO (e.g., "authenticator": "externalbrowser" or OAuth)
- or password ("password": "...")

Prevents repeated SSO prompts by caching one live connection.
"""

from __future__ import annotations
import json
import os
import hashlib
from typing import Any, Dict, Tuple

import pandas as pd
import snowflake.connector as sf

# Optional Streamlit (for caching). Module still works w/out Streamlit.
try:
    import streamlit as st  # type: ignore
except Exception:
    st = None  # pragma: no cover


# ---------------------- Credentials loading ----------------------
def _load_creds() -> Dict[str, Any]:
    # 1) Env var -> JSON file path
    path = os.environ.get("SNOWFLAKE_CREDENTIALS_JSON")
    if path and os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    # 2) Local default JSON
    if os.path.exists("snowflake_credentials.json"):
        with open("snowflake_credentials.json", "r", encoding="utf-8") as f:
            return json.load(f)

    # 3) Streamlit secrets
    if st is not None:
        creds = st.secrets.get("snowflake", None)  # type: ignore[attr-defined]
        if creds:
            return dict(creds)

    raise RuntimeError(
        "No Snowflake credentials found. Provide SNOWFLAKE_CREDENTIALS_JSON, "
        "a local snowflake_credentials.json, or .streamlit/secrets.toml [snowflake]."
    )


def _creds_fingerprint(creds: Dict[str, Any]) -> str:
    keys = [
        "account", "user", "role", "warehouse", "database", "schema",
        "authenticator", "password"
    ]
    material = "|".join([str(creds.get(k, "")) for k in keys])
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


# --------------------------- Connection --------------------------
if st is not None:
    @st.cache_resource(show_spinner=False)
    def _connect_cached(fp: str, kwargs: Dict[str, Any]):
        # One persistent connection per credential fingerprint
        return sf.connect(**kwargs)
else:
    _singleton_conn = None
    def _connect_cached(fp: str, kwargs: Dict[str, Any]):  # type: ignore
        global _singleton_conn
        if _singleton_conn is None:
            _singleton_conn = sf.connect(**kwargs)
        return _singleton_conn


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
        session_parameters={"CLIENT_SESSION_KEEP_ALIVE": True},
    )

    auth = creds.get("authenticator")
    if auth:
        base_kwargs["authenticator"] = auth
    else:
        pwd = creds.get("password")
        if not pwd:
            raise RuntimeError("No password provided and no authenticator set.")
        base_kwargs["password"] = pwd

    fp = _creds_fingerprint(creds)
    conn = _connect_cached(fp, base_kwargs)

    # Ping; if stale, rebuild cached connection
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
    except Exception:
        if st is not None:
            _connect_cached.clear()  # type: ignore[attr-defined]
        conn = _connect_cached(fp, base_kwargs)

    return conn


# ----------------------------- Query -----------------------------
def run_query(sql: str, params: Tuple[Any, ...] | None = None) -> pd.DataFrame:
    """Run SQL using the cached connection and return a DataFrame."""
    if params is None:
        params = ()
    conn = get_snowflake_connection()
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description]
    return pd.DataFrame(rows, columns=cols)


# ---------------------------- Self-test --------------------------
if __name__ == "__main__":
    print("🔍 Testing Snowflake connection ...")
    try:
        conn = get_snowflake_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), "
                "CURRENT_DATABASE(), CURRENT_SCHEMA()"
            )
            u, r, w, d, s = cur.fetchone()
            print(f"✅ Connected as USER={u} ROLE={r} WH={w} DB={d} SCHEMA={s}")

            table_name = "P01_HOSCDA.HOSCDA.HLTH_OS_VLDTN_CNTRL_AUDT"  # adjust if needed
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cur.fetchone()[0]
            print(f"📊 Row count in {table_name}: {count}")
        print("✅ Test completed successfully.")
    except Exception as e:
        print(f"❌ Connection or query failed: {e}")











import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

import pandas as pd
import plotly.express as px
import streamlit as st

from snowflake_conn import run_query


# ================================================================
# Config & Theme
# ================================================================
def load_config(path: str = "dashboard_config.json") -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception as e:
        st.error(f"❌ Failed to load config '{path}': {e}")
        st.stop()

    cfg.setdefault("app", {"title": "Dashboard", "logo_url": ""})
    cfg.setdefault("theme", {})
    cfg.setdefault("datasets", [])
    cfg.setdefault("filters", [])
    cfg.setdefault("widgets", [])
    cfg.setdefault("pages", [])
    return cfg


def apply_theme(theme: Dict[str, Any]):
    st.set_page_config(page_title=theme.get("title", "Dashboard"), layout="wide")
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
      .metric-bad  {{ border-left: 6px solid {theme.get('kpi_bad',  '#C62828')}; padding-left: 8px; }}
      .kpi-card {{ background: white; border-radius: 12px; padding: 12px 16px;
                   box-shadow: 0 1px 3px rgba(0,0,0,.08); min-height: 90px; }}
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)


# ================================================================
# Time parsing helper (EDL_LOAD_DTM like "2025-10-20 00:31:00.000")
# ================================================================
SNOWFLAKE_TIME_FMT = "YYYY-MM-DD HH24:MI:SS.FF3"
def cast_ts(col: str) -> str:
    """Snowflake SQL to parse a VARCHAR time column to TIMESTAMP_NTZ."""
    return f"TO_TIMESTAMP_NTZ({col}, '{SNOWFLAKE_TIME_FMT}')"


# ================================================================
# Filters
# ================================================================
def default_date_range(days_back: int = 7):
    end = datetime.utcnow().date()
    start = end - timedelta(days=days_back)
    return start, end


@st.cache_data(ttl=60, show_spinner=False)
def get_distinct_values(view: str, column: str) -> List[str]:
    sql = f"SELECT DISTINCT {column} AS v FROM {view} WHERE {column} IS NOT NULL ORDER BY 1 LIMIT 2000"
    df = run_query(sql)
    if df is None or df.empty:
        return []
    first_col = df.columns[0]
    return df[first_col].dropna().astype(str).tolist()


def render_filters(cfg: Dict[str, Any]) -> Dict[str, Any]:
    st.sidebar.header("Filters")
    values: Dict[str, Any] = {}

    datasets = {d.get("id", f"ds_{i}"): d for i, d in enumerate(cfg.get("datasets", []))}
    filters_cfg = cfg.get("filters", [])

    for f in filters_cfg:
        f_id = f.get("id")
        if not f_id:
            continue
        label = f.get("label", f_id)
        f_type = f.get("type", "text")
        ds_id = f.get("dataset_id") or (list(datasets.keys())[0] if datasets else None)
        bound_col = f.get("bound_column")
        view = datasets.get(ds_id, {}).get("snowflake_view")

        if f_type == "date_range":
            dv = f.get("default", {}).get("days_back", 7)
            start, end = default_date_range(dv)
            sel = st.sidebar.date_input(label, value=(start, end))
            if isinstance(sel, tuple) and len(sel) == 2:
                values[f_id] = {"start": sel[0], "end": sel[1], "bound_column": bound_col}

        elif f_type in ("select", "multiselect"):
            options: List[str] = []
            if view and bound_col:
                try:
                    options = get_distinct_values(view, bound_col)
                except Exception as e:
                    st.warning(f"Could not load values for {label}: {e}")
            if f_type == "select":
                values[f_id] = {"value": st.sidebar.selectbox(label, options), "bound_column": bound_col}
            else:
                values[f_id] = {"values": st.sidebar.multiselect(label, options), "bound_column": bound_col}

        elif f_type == "search":
            values[f_id] = {"text": st.sidebar.text_input(label, ""), "bound_column": bound_col}

        elif f_type == "number_range":
            mn = st.sidebar.number_input(f"{label} (min)", value=0.0)
            mx = st.sidebar.number_input(f"{label} (max)", value=100.0)
            values[f_id] = {"min": mn, "max": mx, "bound_column": bound_col}

        else:
            values[f_id] = {"text": st.sidebar.text_input(label, ""), "bound_column": bound_col}

    return values


def build_where_clause(dataset_cfg: Dict[str, Any], filters_cfg: List[Dict[str, Any]], filter_values: Dict[str, Any]) -> Tuple[str, Tuple[Any, ...]]:
    conditions: List[str] = []
    params: List[Any] = []

    time_col = dataset_cfg.get("time_column")
    filt_defs = {f.get("id"): f for f in filters_cfg}

    for fid, val in (filter_values or {}).items():
        fdef = filt_defs.get(fid, {})
        col = val.get("bound_column") or fdef.get("bound_column") or time_col
        ftype = fdef.get("type", "text")

        if ftype == "date_range" and isinstance(val, dict):
            start, end = val.get("start"), val.get("end")
            if start and end and col:
                # If targeting configured time_column (usually VARCHAR), filter on parsed timestamp
                if time_col and col.upper() == time_col.upper():
                    ts_expr = cast_ts(col)
                    conditions.append(f"{ts_expr} BETWEEN %s AND %s")
                else:
                    conditions.append(f"{col} BETWEEN %s AND %s")
                params.extend([
                    datetime.combine(start, datetime.min.time()),
                    datetime.combine(end, datetime.max.time())
                ])

        elif ftype == "multiselect":
            vs = val.get("values", [])
            if vs and col:
                placeholders = ",".join(["%s"] * len(vs))
                conditions.append(f"{col} IN ({placeholders})")
                params.extend(vs)

        elif ftype == "select":
            v = val.get("value")
            if v not in (None, "") and col:
                conditions.append(f"{col} = %s")
                params.append(v)

        elif ftype == "search":
            t = val.get("text", "").strip()
            if t and col:
                conditions.append(f"{col} ILIKE %s")
                params.append(f"%{t}%")

        elif ftype == "number_range":
            mn = val.get("min", None)
            mx = val.get("max", None)
            if mn is not None and mx is not None and col:
                conditions.append(f"{col} BETWEEN %s AND %s")
                params.extend([mn, mx])

        else:
            t = val.get("text", "").strip()
            if t and col:
                conditions.append(f"{col} = %s")
                params.append(t)

    where_sql = " WHERE " + " AND ".join(conditions) if conditions else ""
    return where_sql, tuple(params)


# ================================================================
# Data fetch
# ================================================================
def fetch_dataset_df(dataset_cfg: Dict[str, Any], filters_cfg: List[Dict[str, Any]], filter_values: Dict[str, Any]) -> pd.DataFrame:
    view = dataset_cfg.get("snowflake_view")
    if not view:
        st.error("❌ Missing 'snowflake_view' in dataset config.")
        return pd.DataFrame()

    time_col = dataset_cfg.get("time_column")  # e.g., EDL_LOAD_DTM (VARCHAR)
    dims = dataset_cfg.get("dimension_columns", [])
    measures = dataset_cfg.get("measure_columns", [])

    select_cols: List[str] = []
    if time_col:
        select_cols.append(time_col)                          # raw
        select_cols.append(f"{cast_ts(time_col)} AS LOAD_TS") # parsed
    for c in dims + measures:
        if c and c not in select_cols:
            select_cols.append(c)

    select_clause = ", ".join(select_cols) if select_cols else "*"
    where_sql, params = build_where_clause(dataset_cfg, filters_cfg, filter_values)
    order_clause = " ORDER BY LOAD_TS DESC NULLS LAST" if time_col else ""
    sql = f"SELECT {select_clause} FROM {view}{where_sql}{order_clause} LIMIT 20000"

    with st.expander("Debug (SQL)"):
        st.code(sql)
        st.write(params)

    try:
        df = run_query(sql, params)
    except Exception as e:
        st.error(f"❌ Query failed: {e}")
        return pd.DataFrame()

    df.columns = [c.upper() for c in df.columns]
    return df


# ================================================================
# Metrics
# ================================================================
def compute_row_metrics(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    def has(cols: List[str]) -> bool:
        return all(col in out.columns for col in cols)

    # Freshness from parsed timestamp if available
    tcol = None
    if "LOAD_TS" in out.columns:
        tcol = pd.to_datetime(out["LOAD_TS"], errors="coerce")
    elif "EDL_LOAD_DTM" in out.columns:
        tcol = pd.to_datetime(out["EDL_LOAD_DTM"], errors="coerce")
    elif "EDL_INCRMNTL_LOAD_DTM" in out.columns:
        tcol = pd.to_datetime(out["EDL_INCRMNTL_LOAD_DTM"], errors="coerce")
    if tcol is not None:
        now = pd.Timestamp.utcnow()
        out["FRESHNESS_MINUTES"] = (now - tcol).dt.total_seconds() / 60.0

    # Success Rate
    if has(["TRGT_RCRD_CNT", "STG_RCRD_CNT"]):
        stg = out["STG_RCRD_CNT"].replace({0: pd.NA})
        out["LOAD_SUCCESS_RATE"] = out["TRGT_RCRD_CNT"] / stg

    # Raw→Stg ratio
    if has(["LTST_VRSN_STG_RCRD_CNT", "LTST_VRSN_RAWZ_RCRD_CNT"]):
        rawz = out["LTST_VRSN_RAWZ_RCRD_CNT"].replace({0: pd.NA})
        out["RAW_TO_STG_RATIO"] = out["LTST_VRSN_STG_RCRD_CNT"] / rawz

    # Diff counts passthroughs
    if "LTST_VRSN_STG_TRGT_RCRD_DFRNC_CNT" in out.columns:
        out["STG_VS_TRGT_DIFF"] = out["LTST_VRSN_STG_TRGT_RCRD_DFRNC_CNT"]
    if "LTST_VRSN_RAWZ_TRGT_RCRD_DFRNC_CNT" in out.columns:
        out["RAW_VS_TRGT_DIFF"] = out["LTST_VRSN_RAWZ_TRGT_RCRD_DFRNC_CNT"]

    # Diff %
    if has(["LTST_VRSN_STG_TRGT_RCRD_DFRNC_CNT", "LTST_VRSN_STG_RCRD_CNT"]):
        denom = out["LTST_VRSN_STG_RCRD_CNT"].replace({0: pd.NA})
        out["STG_TRGT_DIFF_PCT"] = out["LTST_VRSN_STG_TRGT_RCRD_DFRNC_CNT"] / denom

    if has(["LTST_VRSN_RAWZ_TRGT_RCRD_DFRNC_CNT", "LTST_VRSN_RAWZ_RCRD_CNT"]):
        denom = out["LTST_VRSN_RAWZ_RCRD_CNT"].replace({0: pd.NA})
        out["RAW_TRGT_DIFF_PCT"] = out["LTST_VRSN_RAWZ_TRGT_RCRD_DFRNC_CNT"] / denom

    return out


def kpi_value(metric: str, dfm: pd.DataFrame) -> Tuple[float, str]:
    if dfm.empty or metric not in dfm.columns:
        return float("nan"), "warn"

    sev = "good"
    val = float("nan")

    if metric == "FRESHNESS_MINUTES":
        val = float(pd.to_numeric(dfm[metric], errors="coerce").max())
        if val > 180: sev = "bad"
        elif val >= 60: sev = "warn"

    elif metric == "LOAD_SUCCESS_RATE":
        if all(col in dfm.columns for col in ["TRGT_RCRD_CNT", "STG_RCRD_CNT"]):
            t = dfm["TRGT_RCRD_CNT"].sum()
            s = dfm["STG_RCRD_CNT"].sum()
            val = float(t / s) if s else float("nan")
            if val < 0.95: sev = "bad"
            elif val < 0.98: sev = "warn"
        else:
            val = float(pd.to_numeric(dfm[metric], errors="coerce").mean())

    elif metric in ("STG_VS_TRGT_DIFF", "RAW_VS_TRGT_DIFF"):
        val = float(pd.to_numeric(dfm[metric], errors="coerce").sum())
        if val > 100: sev = "bad"
        elif val > 0: sev = "warn"

    elif metric in ("STG_TRGT_DIFF_PCT", "RAW_TRGT_DIFF_PCT", "RAW_TO_STG_RATIO"):
        val = float(pd.to_numeric(dfm[metric], errors="coerce").mean())
        if metric == "RAW_TO_STG_RATIO":
            if val < 0.95 or val > 1.05: sev = "bad"
            elif val < 0.98 or val > 1.02: sev = "warn"
        else:
            if val > 0.005: sev = "bad"
            elif val > 0: sev = "warn"

    else:
        val = float(pd.to_numeric(dfm[metric], errors="coerce").mean())

    return val, sev


def fmt_metric(metric: str, val: float) -> str:
    if pd.isna(val):
        return "—"
    if metric == "LOAD_SUCCESS_RATE":
        return f"{val*100:.2f}%"
    if metric in ("STG_TRGT_DIFF_PCT", "RAW_TRGT_DIFF_PCT"):
        return f"{val*100:.3f}%"
    if metric == "RAW_TO_STG_RATIO":
        return f"{val:.3f}"
    if metric.endswith("MINUTES"):
        return f"{val:.0f} min"
    return f"{val:,.0f}"


def render_kpi_row(df: pd.DataFrame, theme: Dict[str, Any]):
    dfm = compute_row_metrics(df)
    metrics = [
        ("FRESHNESS_MINUTES", "Freshness (worst min)"),
        ("LOAD_SUCCESS_RATE", "Load Success Rate"),
        ("STG_TRGT_DIFF_PCT", "Stage→Target Diff (%)"),
    ]
    cols = st.columns(len(metrics))
    for i, (mid, title) in enumerate(metrics):
        val, sev = kpi_value(mid, dfm)
        css = {"good": "metric-good", "warn": "metric-warn", "bad": "metric-bad"}.get(sev, "")
        with cols[i]:
            st.markdown(
                f"<div class='kpi-card {css}'><div><strong>{title}</strong></div>"
                f"<div style='font-size:28px'>{fmt_metric(mid, val)}</div></div>",
                unsafe_allow_html=True,
            )


# ================================================================
# Charts
# ================================================================
def render_line_trend(df: pd.DataFrame, time_col: str, measure: str, title: str):
    if df.empty or time_col not in df.columns or measure not in df.columns:
        st.warning(f"No data for {title}")
        return
    g = df.copy()
    g[time_col] = pd.to_datetime(g[time_col], errors="coerce")
    g = g.dropna(subset=[time_col])
    grp = g.groupby(g[time_col].dt.date, as_index=False)[measure].sum()
    st.subheader(title)
    fig = px.line(grp, x=time_col, y=measure)
    st.plotly_chart(fig, use_container_width=True)


def render_bar_by_table(df: pd.DataFrame, table_col: str, measure_col: str, title: str, pct: bool = False):
    if df.empty or table_col not in df.columns or measure_col not in df.columns:
        st.warning(f"No data for {title}")
        return
    g = df.groupby(table_col, as_index=False)[measure_col].mean()  # % metrics -> mean per table
    g = g.sort_values(measure_col, ascending=False)
    st.subheader(title)
    fig = px.bar(g, x=table_col, y=measure_col, text=measure_col)
    if pct:
        fig.update_traces(texttemplate="%{y:.2%}", textposition="outside")
    st.plotly_chart(fig, use_container_width=True)


# ================================================================
# Main
# ================================================================
def main():
    cfg = load_config("dashboard_config.json")

    # Title & theme
    apply_theme(cfg["theme"])
    cols = st.columns([1, 6])
    logo = cfg["app"].get("logo_url")
    if logo:
        with cols[0]:
            st.image(logo, use_column_width=True)
    with cols[1]:
        st.title(cfg["app"]["title"])

    # Connection test
    st.sidebar.markdown("**Connection**")
    if st.sidebar.button("Test Snowflake connection"):
        try:
            diag = run_query(
                "SELECT CURRENT_USER() AS USER, CURRENT_ROLE() AS ROLE, "
                "CURRENT_WAREHOUSE() AS WH, CURRENT_DATABASE() AS DB, CURRENT_SCHEMA() AS SCHEMA"
            )
            st.success("✅ Connected to Snowflake")
            st.dataframe(diag, use_container_width=True)
        except Exception as e:
            st.error(f"❌ Connection failed: {e}")

    # Filters -> Data
    filter_values = render_filters(cfg)
    datasets = cfg.get("datasets", [])
    if not datasets:
        st.warning("No datasets configured.")
        return

    ds = datasets[0]
    with st.spinner("Loading data..."):
        df = fetch_dataset_df(ds, cfg.get("filters", []), filter_values)

    if df.empty:
        st.warning("No records found for selected filters.")
        return

    # KPIs
    render_kpi_row(df, cfg.get("theme", {}))

    # Charts
    tcol = "LOAD_TS" if "LOAD_TS" in df.columns else ds.get("time_column", "EDL_LOAD_DTM").upper()
    if "TRGT_RCRD_CNT" in df.columns:
        render_line_trend(df, tcol, "TRGT_RCRD_CNT", "Target Volume Trend (by day)")

    tbl_col = "TBL_NM" if "TBL_NM" in df.columns else None
    dfm = compute_row_metrics(df)
    if tbl_col and "STG_TRGT_DIFF_PCT" in dfm.columns:
        render_bar_by_table(dfm, tbl_col, "STG_TRGT_DIFF_PCT", "Stage→Target Diff % by Table", pct=True)
    if tbl_col and "LOAD_SUCCESS_RATE" in dfm.columns:
        render_bar_by_table(dfm, tbl_col, "LOAD_SUCCESS_RATE", "Load Success Rate % by Table", pct=True)

    # Details
    st.subheader("Detailed Records")
    st.dataframe(dfm, use_container_width=True)


if __name__ == "__main__":
    main()







{
  "app": {
    "title": "HOSCDA Data Quality Dashboard",
    "logo_url": ""
  },
  "theme": {
    "primary": "#FBC02D",
    "secondary": "#C62828",
    "background": "#FFFDF5",
    "text": "#1F2937",
    "font_family": "Inter, Arial, sans-serif",
    "kpi_good": "#2E7D32",
    "kpi_warn": "#F9A825",
    "kpi_bad": "#C62828"
  },
  "datasets": [
    {
      "id": "dq_audit",
      "snowflake_view": "P01_HOSCDA.HOSCDA.HLTH_OS_VLDTN_CNTRL_AUDT",
      "time_column": "EDL_LOAD_DTM",
      "dimension_columns": ["TBL_NM", "EDL_LOB_CD", "EDL_RUN_ID"],
      "measure_columns": [
        "STG_RCRD_CNT",
        "TRGT_RCRD_CNT",
        "LTST_VRSN_STG_RCRD_CNT",
        "LTST_VRSN_RAWZ_RCRD_CNT",
        "LTST_VRSN_STG_TRGT_RCRD_DFRNC_CNT",
        "LTST_VRSN_RAWZ_TRGT_RCRD_DFRNC_CNT"
      ]
    }
  ],
  "filters": [
    {
      "id": "tbl_filter",
      "label": "Table Name",
      "type": "multiselect",
      "dataset_id": "dq_audit",
      "bound_column": "TBL_NM"
    },
    {
      "id": "date_range",
      "label": "Load Window",
      "type": "date_range",
      "dataset_id": "dq_audit",
      "bound_column": "EDL_LOAD_DTM",
      "default": { "days_back": 7 }
    },
    {
      "id": "lob_filter",
      "label": "LOB",
      "type": "multiselect",
      "dataset_id": "dq_audit",
      "bound_column": "EDL_LOB_CD"
    },
    {
      "id": "run_search",
      "label": "Run ID contains",
      "type": "search",
      "dataset_id": "dq_audit",
      "bound_column": "EDL_RUN_ID"
    }
  ],
  "widgets": [
    {
      "id": "kpi_summary",
      "type": "kpi",
      "dataset_id": "dq_audit",
      "title": "Data Load Health KPIs",
      "metric_ids": ["FRESHNESS_MINUTES", "LOAD_SUCCESS_RATE", "STG_TRGT_DIFF_PCT"]
    },
    {
      "id": "line_trend",
      "type": "line",
      "dataset_id": "dq_audit",
      "title": "Target Volume Trend (by day)",
      "x": "EDL_LOAD_DTM",
      "y": "TRGT_RCRD_CNT"
    },
    {
      "id": "bar_diff_pct",
      "type": "bar",
      "dataset_id": "dq_audit",
      "title": "Stage→Target Diff % by Table",
      "x": "TBL_NM",
      "y": "STG_TRGT_DIFF_PCT",
      "is_percentage": true
    },
    {
      "id": "bar_success_rate",
      "type": "bar",
      "dataset_id": "dq_audit",
      "title": "Load Success Rate % by Table",
      "x": "TBL_NM",
      "y": "LOAD_SUCCESS_RATE",
      "is_percentage": true
    },
    {
      "id": "data_table",
      "type": "table",
      "dataset_id": "dq_audit",
      "title": "Detailed Records"
    }
  ],
  "pages": [
    {
      "id": "overview",
      "title": "Overview",
      "widget_ids": ["kpi_summary", "line_trend", "bar_diff_pct", "bar_success_rate", "data_table"]
    }
  ]
}
