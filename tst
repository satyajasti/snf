from datetime import timedelta

# ...

if ftype == "date_range" and isinstance(val, dict):
    start, end = val.get("start"), val.get("end")
    if start and end:
        col_to_use = val.get("bound_column") or fdef.get("bound_column") or time_col
        if not col_to_use:
            continue

        # Parse the column to TIMESTAMP_NTZ once
        ts_expr = f"{cast_ts(col_to_use)}"

        # Build half-open TS window: [start 00:00:00 , end+1day 00:00:00)
        start_ts = f"{start.strftime('%Y-%m-%d')} 00:00:00"
        end_next_day = end + timedelta(days=1)
        end_ts = f"{end_next_day.strftime('%Y-%m-%d')} 00:00:00"

        # Compare TIMESTAMPs (no DATE(), no BETWEEN)
        conditions.append(f"{ts_expr} >= TO_TIMESTAMP_NTZ(%s) AND {ts_expr} < TO_TIMESTAMP_NTZ(%s)")
        params.extend([start_ts, end_ts])
