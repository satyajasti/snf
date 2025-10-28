def fetch_data(cfg: Dict[str, Any], filters: Dict[str, Any]) -> pd.DataFrame:
    """Fetch data from Snowflake based on config and filters."""
    datasets = cfg.get("datasets")
    if not datasets or not isinstance(datasets, list):
        st.error("❌ No datasets found in dashboard_config.json.")
        return pd.DataFrame()

    dataset = datasets[0]
    if not dataset or not isinstance(dataset, dict):
        st.error("❌ Dataset configuration is invalid (expected a dict).")
        return pd.DataFrame()

    view = dataset.get("snowflake_view")
    if not view:
        st.error("❌ Missing 'snowflake_view' in dataset configuration.")
        return pd.DataFrame()

    time_col = dataset.get("time_column")
    where_clauses = []
    params = []

    # ---- Apply Filters ----
    for fid, val in (filters or {}).items():
        if isinstance(val, dict) and "start" in val and time_col:
            where_clauses.append(f"{time_col} BETWEEN %s AND %s")
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

    try:
        df = run_query(sql, tuple(params))
        return df
    except Exception as e:
        st.error(f"❌ Query failed: {e}")
        return pd.DataFrame()








{
  "app": {
    "title": "HOSCDA Data Quality Dashboard",
    "logo_url": ""
  },

  "theme": {
    "primary": "#1565C0",
    "secondary": "#26A69A",
    "background": "#FFFFFF",
    "text": "#1F2937",
    "font_family": "Inter, Arial, sans-serif",
    "kpi_good": "#2E7D32",
    "kpi_warn": "#F9A825",
    "kpi_bad": "#C62828"
  },

  "datasets": [
    {
      "id": "dq_audit",
      "snowflake_view": "UE_CMRCL.ETM_STG.HLTH_OS_VLDTN_CNTRL_AUDT",
      "time_column": "EDL_INCRMNTL_LOAD_DTM",
      "dimension_columns": [
        "LOB_CD",
        "STTS_FLAG_NM",
        "PRCS_NM"
      ],
      "measure_columns": [
        "STG_RCRD_CNT",
        "TRGT_RCRD_CNT",
        "LTST_VRSN_STG_TRGT_RCRD_DFRNC_CNT",
        "LTST_VRSN_RAWZ_TRGT_RCRD_DFRNC_CNT"
      ]
    }
  ],

  "filters": [
    {
      "id": "lob_cd",
      "label": "LOB Code",
      "type": "multiselect",
      "bound_column": "LOB_CD"
    },
    {
      "id": "prcs_nm",
      "label": "Process Name",
      "type": "multiselect",
      "bound_column": "PRCS_NM"
    },
    {
      "id": "date_range",
      "label": "Load Date Range",
      "type": "date_range",
      "default": {
        "days_back": 7
      }
    }
  ],

  "widgets": [
    {
      "id": "kpi_summary",
      "type": "kpi",
      "dataset_id": "dq_audit",
      "title": "Data Load Health KPIs",
      "metric_ids": [
        "Freshness_Minutes",
        "Load_Success_Rate",
        "Stg_vs_Trgt_Diff"
      ]
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
      "id": "main",
      "title": "Dashboard Overview",
      "widget_ids": ["kpi_summary", "data_table"]
    }
  ]
}
