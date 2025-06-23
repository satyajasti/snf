from turtle import pd


def get_column_types(conn, database, schema, table):
    query = f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM {database}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
    """
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()

    categorical = [r[0] for r in rows if r[1].upper() in ('TEXT', 'VARCHAR', 'CHAR', 'STRING')]
    numeric = [r[0] for r in rows if r[1].upper() in ('NUMBER', 'FLOAT', 'INT', 'DECIMAL', 'NUMERIC', 'DOUBLE')]
    return categorical, numeric

def get_skew_data_with_query(conn, database, schema, table, columns, top_n=3, skew_threshold=0.8):
    cur = conn.cursor()
    summary = []

    for col in columns:
        query = f"""
            SELECT "{col}", COUNT(*) as cnt
            FROM "{database}"."{schema}"."{table}"
            GROUP BY "{col}"
            ORDER BY cnt DESC
            LIMIT {top_n}
        """.strip()

        cur.execute(query)
        rows = cur.fetchall()

        total_query = f'SELECT COUNT(*) FROM "{database}"."{schema}"."{table}"'
        cur.execute(total_query)
        total_rows = cur.fetchone()[0]

        if rows:
            top_value = rows[0][0]
            top_count = rows[0][1]
            skew = "Yes" if total_rows > 0 and (top_count / total_rows) > skew_threshold else "No"
        else:
            top_value = None
            top_count = 0
            skew = "No"

        summary.append({
            "Column": col,
            "Top_Value": top_value,
            "Top_Count": top_count,
            "Total_Rows": total_rows,
            "Dominance %": round((top_count / total_rows) * 100, 2) if total_rows else 0,
            "Skew_Detected": skew,
            "Query_Used": query
        })

    cur.close()
    return pd.DataFrame(summary)

def get_outlier_data_with_query(conn, database, schema, table, columns):
    summary = []

    for col in columns:
        query = f'SELECT "{col}" FROM "{database}"."{schema}"."{table}" WHERE "{col}" IS NOT NULL'
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        columns_desc = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows, columns=columns_desc)
        cur.close()

        df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df[df[col].notnull()]

        if df.empty:
            summary.append({
                "Column": col,
                "Q1": None,
                "Q3": None,
                "IQR": None,
                "Lower_Bound": None,
                "Upper_Bound": None,
                "Outlier_Count": 0,
                "Sample_Outliers": "Non-numeric values only",
                "Query_Used": query
            })
            continue

        Q1 = float(df[col].quantile(0.25))
        Q3 = float(df[col].quantile(0.75))
        IQR = Q3 - Q1
        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR
        outliers = df[(df[col] < lower) | (df[col] > upper)][col].tolist()

        summary.append({
            "Column": col,
            "Q1": Q1,
            "Q3": Q3,
            "IQR": IQR,
            "Lower_Bound": lower,
            "Upper_Bound": upper,
            "Outlier_Count": len(outliers),
            "Sample_Outliers": ", ".join(map(str, outliers[:3])) if outliers else "None",
            "Query_Used": query
        })

    return pd.DataFrame(summary)
