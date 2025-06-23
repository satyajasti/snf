import pandas as pd

def run_audit_column_validation(conn, database, schema, table, audit_df):
    """
    Checks each audit column for NULLs in the given Snowflake table.
    Returns a DataFrame with null counts and executed queries.
    """
    audit_columns = audit_df['Audit_Column'].dropna().str.strip().str.lower().tolist()

    cursor = conn.cursor()
    result_rows = []

    try:
        # Step 1: Get actual column names from Snowflake table
        col_query = f"""
            SELECT COLUMN_NAME 
            FROM {database}.INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
        """
        cursor.execute(col_query)
        actual_cols = [r[0] for r in cursor.fetchall()]
        actual_cols_lower = {col.lower(): col for col in actual_cols}  # map lowercase to actual name

        # Step 2: Filter valid audit columns
        valid_audit_cols = [actual_cols_lower[col] for col in audit_columns if col in actual_cols_lower]

        for col in valid_audit_cols:
            query = f'SELECT COUNT(*) FROM "{database}"."{schema}"."{table}" WHERE "{col}" IS NULL'
            cursor.execute(query)
            null_count = cursor.fetchone()[0]
            result_rows.append({
                "Database": database,
                "Schema": schema,
                "Table": table,
                "Audit_Column": col,
                "Null_Count": null_count,
                "Query": query.strip()
            })

        if not result_rows:
            result_rows.append({
                "Database": database,
                "Schema": schema,
                "Table": table,
                "Audit_Column": "None Found",
                "Null_Count": "N/A",
                "Query": "No valid audit columns matched"
            })

        return pd.DataFrame(result_rows)

    except Exception as e:
        return pd.DataFrame([{
            "Database": database,
            "Schema": schema,
            "Table": table,
            "Audit_Column": "Error",
            "Null_Count": "Error",
            "Query": str(e)
        }])

    finally:
        cursor.close()
