elif f_type == "multiselect":
    try:
        df_vals = run_query(
            f"SELECT DISTINCT {f['bound_column']} AS v "
            f"FROM {cfg['datasets'][0]['snowflake_view']} "
            f"WHERE {f['bound_column']} IS NOT NULL ORDER BY 1"
        )
        # 🩹 Safe column handling
        if not df_vals.empty:
            first_col = df_vals.columns[0]
            opts = df_vals[first_col].dropna().astype(str).tolist()
        else:
            opts = []
    except Exception as e:
        st.warning(f"Could not load values for {label}: {e}")
        opts = []
    filters[f_id] = st.sidebar.multiselect(label, opts)
