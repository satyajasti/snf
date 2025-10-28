# app.py
import json
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd
import plotly.express as px
import streamlit as st

# Use your connection helper (JSON creds or secrets)
from snowflake_conn import run_query


# =============================================================
# CONFIG
# =============================================================
def load_config(path: str = "dashboard_config.json") -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception as e:
        st.error(f"❌ Failed to load config '{path}': {e}")
        st.stop()

    # Set safe defaults for missing keys
    cfg.setdefault("app", {"title": "Dashboard", "logo_url": ""})
    cfg.setdefault("theme", {})
    cfg.setdefault("datasets", [])
    cfg.setdefault("filters", [])
    cfg.setdefault("widgets", [])
    cfg.setdefault("pages", [])

    return cfg


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
      .metric-bad  {{ border-left: 6px solid {theme.get('kpi_bad',  '#C62828')}; padding-left: 8px; }}
      .kpi-card {{ background: white; border-radius: 12px; padding: 12px 16px; box-shadow: 0 1px 3px rgba(0,0,0,.08); min-height: 90px; }}
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)


# =============================================================
# FILTERS
# =============================================================
def default_date_range(days_back: int = 7):
    end = datetime.utcnow().date()
    start = end - timedelta(days=days_back)
    return start, end


def get_distinct_values(view: str, column: str) -> List[str]:
    sql = f"SELECT DISTINCT {column} AS v FROM {view} WHERE {column} IS NOT NULL ORDER BY 1 LIMIT 2000"
    df = run_query(sql)
    if df is None or df.empty:
        return []
    # Be robust to casing/aliasing
    first_col = df.columns[0]
    return df[first_col].dropna().astype(str).tolist()


def render_filters(cfg: Dict[str, Any]) -> Dict[str, Any]:
    st.sidebar.header("Filters")
    filters_cfg = cfg.get("filters", [])
    datasets = {d.get("id", f"ds_{i}"): d for i, d in enumerate(cfg.get("datasets", []))}
    values: Dict[str, Any] = {}

    for f in filters_cfg:
        f_id = f.get("id")
        if not f_id:
            continue
        label = f.get("label", f_id)
        f_type = f.get("type", "text")
        ds_id = f.get("dataset_id") or list(datasets.keys())[0] if datasets else None
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

        else:  # simple text
            values[f_id] = {"text": st.sidebar.text_input(label, ""), "bound_column": bound_col}

    return values


def build_where_clause(dataset_cfg: Dict[str, Any], filters_cfg: List[Dict[str, Any]], filter_values: Dict[str, Any]) -> Tuple[str, Tuple[Any, ...]]:
    conditions: List[str] = []
    params: List[Any] = []
    time_col = dataset_cfg.get("time_column")

    # map by id to filter definition for dataset
    filt_defs = {f.get("id"): f for f in filters_cfg}

    for fid, val in (filter_values or {}).items():
        fdef = filt_defs.get(fid, {})
        col = val.get("bound_column") or fdef.get("bound_column")
        ftype = fdef.get("type", "text")
        if not col:
            # if bound_column is missing, skip safely
            continue

        if ftype == "date_range" and isinstance(val, dict):
            if time_col:
                start, end = val.get("start"), val.get("end")
                if start and end:
                    conditions.append(f"{time_col} BETWEEN %s AND %s")
                    # normalize to datetime (Snowflake supports date as well)
                    params.extend([datetime.combine(start, datetime.min.time()), datetime.combine(end, datetime.max.time())])

        elif ftype == "multiselect":
            vs = val.get("values", [])
            if vs:
                placeholders = ",".join(["%s"] * len(vs))
                conditions.append(f"{col} IN ({placeholders})")
                params.extend(vs)

        elif ftype == "select":
            v = val.get("value")
            if v not in (None, ""):
                conditions.append(f"{col} = %s")
                params.append(v)

        elif ftype == "search":
            t = val.get("text", "").strip()
            if t:
                conditions.append(f"{col} ILIKE %s")
                params.append(f"%{t}%")

        elif ftype == "number_range":
            mn = val.get("min", None)
            mx = val.get("max", None)
            if mn is not None and mx is not None:
                conditions.append(f"{col} BETWEEN %s AND %s")
                params.extend([mn, mx])

        else:
            t = val.get("text", "").strip()
            if t:
                conditions.append(f"{col} = %s")
                params.append(t)

    where_sql = " WHERE " + " AND ".join(conditions) if conditions else ""
    return where_sql, tuple(params)


# =============================================================
# DATA
# =============================================================
def fetch_dataset_df(dataset_cfg: Dict[str, Any], filters_cfg: List[Dict[str, Any]], filter_values: Dict[str, Any]) -> pd.DataFrame:
    view = dataset_cfg.get("snowflake_view")
    if not view:
        st.error("❌ Missing 'snowflake_view' in dataset config.")
        return pd.DataFrame()

    time_col = dataset_cfg.get("time_column")
    dims = dataset_cfg.get("dimension_columns", [])
    measures = dataset_cfg.get("measure_columns", [])

    # Build select list safely
    select_cols: List[str] = []
    if time_col:
        select_cols.append(time_col)

    # include EDL_INCRMNTL_LOAD_DTM for freshness if present in view (if not, query will still pass; Snowflake ignores unknown? No -> must be present)
    # safer approach: just include dims+measures and rely on what config lists
    for c in dims + measures:
        if c and c not in select_cols:
            select_cols.append(c)

    # fallback to * if nothing listed
    select_clause = ", ".join(select_cols) if select_cols else "*"

    where_sql, params = build_where_clause(dataset_cfg, filters_cfg, filter_values)

    order_clause = f" ORDER BY {time_col} DESC NULLS LAST" if time_col else ""
    sql = f"SELECT {select_clause} FROM {view}{where_sql}{order_clause} LIMIT 20000"

    try:
        df = run_query(sql, params)
    except Exception as e:
        st.error(f"❌ Query failed: {e}")
        return pd.DataFrame()

    # Normalize column names to upper for consistent references
    df.columns = [c.upper() for c in df.columns]
    return df


# =============================================================
# METRICS / KPIs
# =============================================================
def compute_row_metrics(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    def has(cols: List[str]) -> bool:
        return all(col in out.columns for col in cols)

    # Freshness (requires EDL_INCRMNTL_LOAD_DTM)
    if "EDL_INCRMNTL_LOAD_DTM" in out.columns:
        now = pd.Timestamp.utcnow()
        tcol = pd.to_datetime(out["EDL_INCRMNTL_LOAD_DTM"], errors="coerce")
        out["FRESHNESS_MINUTES"] = (now - tcol).dt.total_seconds() / 60.0

    # Load Success Rate
    if has(["TRGT_RCRD_CNT", "STG_RCRD_CNT"]):
        stg = out["STG_RCRD_CNT"].replace({0: pd.NA})
        out["LOAD_SUCCESS_RATE"] = out["TRGT_RCRD_CNT"] / stg

    # Raw->Stg Ratio
    if has(["LTST_VRSN_STG_RCRD_CNT", "LTST_VRSN_RAWZ_RCRD_CNT"]):
        rawz = out["LTST_VRSN_RAWZ_RCRD_CNT"].replace({0: pd.NA})
        out["RAW_TO_STG_RATIO"] = out["LTST_VRSN_STG_RCRD_CNT"] / rawz

    # Diffs (as pass-through)
    if "LTST_VRSN_STG_TRGT_RCRD_DFRNC_CNT" in out.columns:
        out["STG_VS_TRGT_DIFF"] = out["LTST_VRSN_STG_TRGT_RCRD_DFRNC_CNT"]
    if "LTST_VRSN_RAWZ_TRGT_RCRD_DFRNC_CNT" in out.columns:
        out["RAW_VS_TRGT_DIFF"] = out["LTST_VRSN_RAWZ_TRGT_RCRD_DFRNC_CNT"]

    # Derived % of diffs
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
        # thresholds for ratios/percentages (pct as fraction 0-1)
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
    # Choose 3 good headline metrics
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


# =============================================================
# CHARTS
# =============================================================
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
    g = df.groupby(table_col, as_index=False)[measure_col].mean()  # mean pct per table; or sum if absolute
    g = g.sort_values(measure_col, ascending=False)
    st.subheader(title)
    fig = px.bar(g, x=table_col, y=measure_col, text=measure_col)
    if pct:
        fig.update_traces(texttemplate="%{y:.2%}", textposition="outside")
    st.plotly_chart(fig, use_container_width=True)


# =============================================================
# MAIN
# =============================================================
def main():
    cfg = load_config()
    st.set_page_config(page_title=cfg["app"]["title"], layout="wide")
    apply_theme(cfg["theme"])

    # Header
    header_cols = st.columns([1, 6])
    logo = cfg["app"].get("logo_url")
    with header_cols[0]:
        if logo:
            st.image(logo, use_column_width=True)
    with header_cols[1]:
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

    # Filters
    filter_values = render_filters(cfg)

    # Data
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

    # Charts (line + bar)
    # Time trend: TRGT_RCRD_CNT over time
    tcol = ds.get("time_column", "EDL_LOAD_DTM").upper()
    if "TRGT_RCRD_CNT" in df.columns:
        render_line_trend(df, tcol, "TRGT_RCRD_CNT", "Target Volume Trend (by day)")

    # Bar by table for diff % and success rate if present
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
