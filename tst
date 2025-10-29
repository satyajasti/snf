if ftype == "date_range" and isinstance(val, dict):
    start, end = val.get("start"), val.get("end")
    if start and end:
        col_to_use = val.get("bound_column") or fdef.get("bound_column") or time_col
        if not col_to_use:
            continue
        # Compare on DATE() so times don't exclude rows unexpectedly
        if time_col and col_to_use.upper() == time_col.upper():
            ts_expr = f"DATE({cast_ts(col_to_use)})"
            conditions.append(f"{ts_expr} BETWEEN %s AND %s")
        else:
            conditions.append(f"{col_to_use} BETWEEN %s AND %s")
        params.extend([start, end])  # pass dates only



def cast_ts(col: str) -> str:
    # Works whether column is VARCHAR or TIMESTAMP
    return f"TRY_TO_TIMESTAMP_NTZ({col})"



@st.cache_data(ttl=60)
def data_window(view: str, time_col: str):
    sql = f"""
      SELECT
        MIN({cast_ts(time_col)}) AS min_ts,
        MAX({cast_ts(time_col)}) AS max_ts,
        COUNT(*) AS total_rows
      FROM {view}
    """
    return run_query(sql)


ds = cfg.get("datasets", [])[0]
dw = None
try:
    dw = data_window(ds["snowflake_view"], ds["time_column"])
except Exception as e:
    st.sidebar.info(f"Data window check failed: {e}")

if dw is not None and not dw.empty:
    m = pd.to_datetime(dw.loc[0, "MIN_TS"])
    x = pd.to_datetime(dw.loc[0, "MAX_TS"])
    t = int(dw.loc[0, "TOTAL_ROWS"])
    st.sidebar.caption(f"Data availability:\n\n• Rows: **{t:,}**\n• From: **{m.date()}**\n• To: **{x.date()}**")



st.sidebar.number_input("Max rows (LIMIT)", min_value=1000, max_value=200000, value=20000, step=1000, key="row_limit")
...
row_limit = st.session_state.get("row_limit", 20000)
sql = f"SELECT {select_clause} FROM {view}{where_sql}{order_clause} LIMIT {row_limit}"
