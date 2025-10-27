import json
import os
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Tuple

import pandas as pd
import plotly.express as px
import streamlit as st
import snowflake.connector as sf

# =============================================================
# CONFIG LOADING
# =============================================================

def load_config(default_path: str = "dashboard_config.json") -> Dict[str, Any]:
    """Load JSON config from local path (or an alternate path via query param)."""
    # Support query param: ?config=path/to/file.json
    qs = st.query_params
    cfg_path = qs.get("config", [default_path])[0] if hasattr(qs, "get") else default_path

    # File uploader (optional override)
    st.sidebar.markdown("**Config**")
    uploaded = st.sidebar.file_uploader("Load a different JSON (optional)", type=["json"])

    if uploaded is not None:
        return json.load(uploaded)

    # Fall back to local file
    if not os.path.exists(cfg_path):
        st.stop()
    with open(cfg_path, "r", encoding="utf-8") as f:
        return json.load(f)


# =============================================================
# SNOWFLAKE CONNECTION + QUERY
# =============================================================

def get_conn():
    """Create a Snowflake connection using Streamlit secrets.
    Supports password auth *or* SSO via authenticator=externalbrowser (or oauth).
    """
    creds = st.secrets.get("snowflake", None)
    if not creds:
        st.error("Missing Snowflake credentials in .streamlit/secrets.toml under [snowflake]")
        st.stop()

    # Common required fields
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
    if "authenticator" in creds and creds["authenticator"]:
        # SSO/Browser/OAuth flow (no password needed)
        base_kwargs["authenticator"] = creds["authenticator"]
    else:
        # Password auth
        if not creds.get("password"):
            st.error("No password found in secrets and no authenticator set. Provide one of them.")
            st.stop()
        base_kwargs["password"] = creds["password"]

    return sf.connect(**base_kwargs),
        warehouse=creds.get("warehouse"),
        database=creds.get("database"),
        schema=creds.get("schema"),
    )


def run_query_df(sql: str, params: Tuple[Any, ...] = ()) -> pd.DataFrame:
    """Execute a parameterized query and return DataFrame with proper columns."""
    with get_conn() as con:
        with con.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            cols = [c[0] for c in cur.description]
    return pd.DataFrame(rows, columns=cols)


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
      h1, h2, h3, h4, h5, h6 {{ color: var(--text); font-family: {theme.get('font_family', 'Inter, Arial, sans-serif')}; }}
      .kpi-card {{ background: white; border-radius: 12px; padding: 12px 16px; box-shadow: 0 1px 3px rgba(0,0,0,.08); }}
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)


# =============================================================
# FILTER WIDGETS
# =============================================================

def default_date_range(days_back: int = 7) -> Tuple[date, date]:
    end = datetime.utcnow().date()
    start = end - timedelta(days=days_back)
    return start, end


def distinct_values_sql(view: str, column: str) -> str:
    return f"SELECT DISTINCT {column} AS v FROM {view} WHERE {column} IS NOT NULL ORDER BY 1 LIMIT 1000"


def render_filters(cfg: Dict[str, Any]) -> Dict[str, Any]:
    filters_cfg = cfg.get("filters", [])
    datasets = {d["id"]: d for d in cfg.get("datasets", [])}

    st.sidebar.header("Filters")
    values: Dict[str, Any] = {}

    for f in filters_cfg:
        f_id = f["id"]
        label = f.get("label", f_id)
        f_type = f.get("type", "text")
        ds_id = f.get("dataset_id")
        bound_col = f.get("bound_column")
        dataset = datasets.get(ds_id)
        source_view = dataset.get("snowflake_view") if dataset else None

        if f_type == "date_range":
            dv = f.get("default", {}).get("days_back", 7)
            start, end = default_date_range(dv)
            sel = st.sidebar.date_input(label, value=(start, end))
            if isinstance(sel, tuple) and len(sel) == 2:
                values[f_id] = {"start": sel[0], "end": sel[1]}

        elif f_type in ("select", "multiselect"):
            # Load distinct values from Snowflake (limited to 1000)
            if source_view and bound_col:
                try:
                    df_vals = run_query_df(distinct_values_sql(source_view, bound_col))
                    options = df_vals["v"].tolist()
                except Exception:
                    options = []
            else:
                options = []
            if f_type == "select":
                values[f_id] = st.sidebar.selectbox(label, options)
            else:
                values[f_id] = st.sidebar.multiselect(label, options)

        elif f_type == "search":
            values[f_id] = st.sidebar.text_input(label, value="")

        elif f_type == "number_range":
            mn = st.sidebar.number_input(f"{label} (min)", value=0.0)
            mx = st.sidebar.number_input(f"{label} (max)", value=100.0)
            values[f_id] = {"min": mn, "max": mx}

        else:
            values[f_id] = st.sidebar.text_input(label, value="")

    return values


# =============================================================
# DATA ACCESS PER DATASET
# =============================================================

def where_clause_for_filters(filters: Dict[str, Any], dataset_cfg: Dict[str, Any], all_filters_cfg: List[Dict[str, Any]]) -> Tuple[str, Tuple[Any, ...]]:
    conditions = []
    params: List[Any] = []

    # Map filter ids -> definition for this dataset
    filt_map = {f["id"]: f for f in all_filters_cfg if f.get("dataset_id") == dataset_cfg["id"]}
    time_col = dataset_cfg.get("time_column")

    # Apply only filters that are bound to this dataset
    for fid, val in filters.items():
        fdef = filt_map.get(fid)
        if not fdef:
            continue
        ftype = fdef.get("type")
        col = fdef.get("bound_column")
        if not col:
            continue

        if ftype == "date_range" and isinstance(val, dict) and time_col:
            start = datetime.combine(val["start"], datetime.min.time())
            end = datetime.combine(val["end"], datetime.max.time())
            conditions.append(f"{time_col} BETWEEN %s AND %s")
            params.extend([start, end])

        elif ftype == "multiselect" and val:
            placeholders = ",".join(["%s"] * len(val))
            conditions.append(f"{col} IN ({placeholders})")
            params.extend(val)

        elif ftype == "select" and val:
            conditions.append(f"{col} = %s")
            params.append(val)

        elif ftype == "search" and val:
            conditions.append(f"{col} ILIKE %s")
            params.append(f"%{val}%")

        elif ftype == "number_range" and isinstance(val, dict):
            conditions.append(f"{col} BETWEEN %s AND %s")
            params.extend([val.get("min", 0), val.get("max", 0)])

        else:
            # text input fallback
            if isinstance(val, str) and val.strip():
                conditions.append(f"{col} = %s")
                params.append(val.strip())

    where_sql = (" WHERE " + " AND ".join(conditions)) if conditions else ""
    return where_sql, tuple(params)


def fetch_dataset_df(dataset_cfg: Dict[str, Any], filters_cfg: List[Dict[str, Any]], filter_values: Dict[str, Any]) -> pd.DataFrame:
    view = dataset_cfg["snowflake_view"]
    time_col = dataset_cfg.get("time_column")

    dims = dataset_cfg.get("dimension_columns", [])
    measures = dataset_cfg.get("measure_columns", [])

    # Build a whitelist SELECT of columns we need for all widgets on this dataset.
    select_cols = []
    if time_col:
        select_cols.append(time_col)
    # Always include incremental load dt for freshness metric if present
    if "EDL_INCRMNTL_LOAD_DTM" not in select_cols and "EDL_INCRMNTL_LOAD_DTM" in (measures + dims):
        select_cols.append("EDL_INCRMNTL_LOAD_DTM")
    # Add whitelisted dims and measures
    select_cols.extend([c for c in dims if c not in select_cols])
    select_cols.extend([c for c in measures if c not in select_cols])

    select_clause = ", ".join(select_cols) if select_cols else "*"

    where_sql, params = where_clause_for_filters(filter_values, dataset_cfg, filters_cfg)

    sql = f"SELECT {select_clause} FROM {view}{where_sql} ORDER BY {time_col} DESC NULLS LAST LIMIT 20000" if time_col else f"SELECT {select_clause} FROM {view}{where_sql} LIMIT 20000"

    try:
        df = run_query_df(sql, params)
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()

    return df


# =============================================================
# METRICS
# =============================================================

def compute_row_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Compute standard row-level metrics expected by the config dictionary."""
    out = df.copy()
    # Freshness minutes
    if "EDL_INCRMNTL_LOAD_DTM" in out.columns:
        now = pd.Timestamp.utcnow()
        out["Freshness_Minutes"] = (now - pd.to_datetime(out["EDL_INCRMNTL_LOAD_DTM"]).dt.tz_localize(None)).dt.total_seconds() / 60.0
    # Load success rate
    if "TRGT_RCRD_CNT" in out.columns and "STG_RCRD_CNT" in out.columns:
        out["Load_Success_Rate"] = out["TRGT_RCRD_CNT"] / out["STG_RCRD_CNT"].replace({0: pd.NA})
    # Raw to Stg ratio
    if "LTST_VRSN_STG_RCRD_CNT" in out.columns and "LTST_VRSN_RAWZ_RCRD_CNT" in out.columns:
        out["Raw_to_Stg_Ratio"] = out["LTST_VRSN_STG_RCRD_CNT"] / out["LTST_VRSN_RAWZ_RCRD_CNT"].replace({0: pd.NA})
    # Diff passthrough (already numeric)
    if "LTST_VRSN_STG_TRGT_RCRD_DFRNC_CNT" in out.columns:
        out["Stg_vs_Trgt_Diff"] = out["LTST_VRSN_STG_TRGT_RCRD_DFRNC_CNT"]
    if "LTST_VRSN_RAWZ_TRGT_RCRD_DFRNC_CNT" in out.columns:
        out["Raw_vs_Trgt_Diff"] = out["LTST_VRSN_RAWZ_TRGT_RCRD_DFRNC_CNT"]
    return out


def kpi_value(metric_id: str, df: pd.DataFrame) -> Tuple[float, str]:
    """Aggregate a single KPI metric into one scalar and classify severity."""
    if df.empty or metric_id not in df.columns:
        return float("nan"), "warn"

    severity = "good"
    value = None

    if metric_id == "Freshness_Minutes":
        # show worst (max) freshness as the KPI
        value = float(pd.to_numeric(df[metric_id], errors="coerce").max())
        if value > 180:
            severity = "bad"
        elif value >= 60:
            severity = "warn"

    elif metric_id == "Load_Success_Rate":
        # weighted accuracy: sum(target)/sum(stage)
        if "TRGT_RCRD_CNT" in df.columns and "STG_RCRD_CNT" in df.columns:
            t = df["TRGT_RCRD_CNT"].sum()
            s = df["STG_RCRD_CNT"].sum()
            value = float(t / s) if s else float("nan")
            if value < 0.95:
                severity = "bad"
            elif value < 0.98:
                severity = "warn"
        else:
            value = float(pd.to_numeric(df[metric_id], errors="coerce").mean())

    elif metric_id in ("Stg_vs_Trgt_Diff", "Raw_vs_Trgt_Diff"):
        value = float(pd.to_numeric(df[metric_id], errors="coerce").sum())
        if value > 100:
            severity = "bad"
        elif value > 0:
            severity = "warn"

    elif metric_id == "Raw_to_Stg_Ratio":
        # Show mean ratio
        value = float(pd.to_numeric(df[metric_id], errors="coerce").mean())
        if value < 0.95 or value > 1.05:
            severity = "bad"
        elif value < 0.98 or value > 1.02:
            severity = "warn"

    else:
        # default to mean
        value = float(pd.to_numeric(df[metric_id], errors="coerce").mean())

    return value, severity


def format_kpi(metric_id: str, val: float) -> str:
    if pd.isna(val):
        return "—"
    if metric_id == "Load_Success_Rate" or metric_id.lower().endswith("rate"):
        return f"{val*100:.2f}%"
    if metric_id == "Raw_to_Stg_Ratio":
        return f"{val:.2f}"
    if metric_id.endswith("Minutes"):
        return f"{val:.0f} min"
    return f"{val:,.0f}"


# =============================================================
# WIDGET RENDERERS
# =============================================================

def render_kpi_card(title: str, metric_ids: List[str], df: pd.DataFrame, theme: Dict[str, Any]):
    dfm = compute_row_metrics(df)
    cols = st.columns(len(metric_ids))
    for i, mid in enumerate(metric_ids):
        val, sev = kpi_value(mid, dfm)
        css_class = {"good": "metric-good", "warn": "metric-warn", "bad": "metric-bad"}.get(sev, "")
        with cols[i]:
            st.markdown(f"<div class='kpi-card {css_class}'><div><strong>{title if len(metric_ids)==1 else mid}</strong></div><div style='font-size:28px'>{format_kpi(mid, val)}</div></div>", unsafe_allow_html=True)


def render_table(title: str, df: pd.DataFrame):
    dfm = compute_row_metrics(df)
    st.subheader(title)
    st.dataframe(dfm, use_container_width=True)


def render_line(title: str, df: pd.DataFrame, time_col: str, measure: str):
    if df.empty or time_col not in df.columns or measure not in df.columns:
        st.warning(f"No data for {title}")
        return
    # group by day for simplicity
    g = df.dropna(subset=[time_col]).copy()
    g[time_col] = pd.to_datetime(g[time_col])
    g = g.groupby(g[time_col].dt.date, as_index=False)[measure].sum()
    fig = px.line(g, x=time_col, y=measure, title=None)
    st.subheader(title)
    st.plotly_chart(fig, use_container_width=True)


# =============================================================
# PAGE RENDER
# =============================================================

def render_page(page_cfg: Dict[str, Any], cfg: Dict[str, Any], data_cache: Dict[str, pd.DataFrame]):
    widgets_map = {w["id"]: w for w in cfg.get("widgets", [])}
    datasets_map = {d["id"]: d for d in cfg.get("datasets", [])}

    for wid in page_cfg.get("widget_ids", []):
        w = widgets_map.get(wid)
        if not w:
            continue
        wtype = w.get("type")
        title = w.get("title", w.get("id"))
        ds_id = w.get("dataset_id")
        ds = datasets_map.get(ds_id)
        if not ds:
            continue
        df = data_cache.get(ds_id, pd.DataFrame())

        if wtype == "kpi":
            mids = w.get("metric_ids", [])
            render_kpi_card(title, mids, df, cfg.get("theme", {}))

        elif wtype == "table":
            render_table(title, df)

        elif wtype in ("line", "area"):
            time_col = ds.get("time_column")
            measures = w.get("measures", [])
            measure = measures[0] if measures else None
            if measure:
                render_line(title, df, time_col, measure)

        else:
            st.info(f"Widget type '{wtype}' not implemented in engine.")


# =============================================================
# MAIN APP
# =============================================================

def main():
    cfg = load_config()

    # Page title & theme
    st.set_page_config(page_title=cfg.get("app", {}).get("title", "Dashboard"), layout="wide")
    apply_theme(cfg.get("theme", {}))
    title = cfg.get("app", {}).get("title", "Dashboard")
    logo = cfg.get("app", {}).get("logo_url")

    header_cols = st.columns([1, 5])
    with header_cols[0]:
        if logo:
            st.image(logo, use_column_width=True)
    with header_cols[1]:
        st.title(title)

    # Filters UI
    filter_values = render_filters(cfg)

    # Preload dataset frames (once per run)
    datasets_cfg = cfg.get("datasets", [])
    data_cache: Dict[str, pd.DataFrame] = {}

    with st.spinner("Loading data..."):
        for ds in datasets_cfg:
            df = fetch_dataset_df(ds, cfg.get("filters", []), filter_values)
            data_cache[ds["id"]] = df

    # Pages (tabs)
    pages = cfg.get("pages", [])
    if not pages:
        st.warning("No pages defined in config.")
        return

    tab_labels = [p.get("title", p.get("id")) for p in pages]
    tabs = st.tabs(tab_labels)

    for tab, page in zip(tabs, pages):
        with tab:
            render_page(page, cfg, data_cache)

    # Footer / debug
    with st.expander("Debug: filter values"):
        st.json(filter_values)


if __name__ == "__main__":
    main()
