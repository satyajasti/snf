
import pandas as pd

def build_table_ref(database, schema, table):
    return f"{database}.{schema}.{table}" if database else f"{schema}.{table}"

def generate_null_query(table_ref, columns):
    parts = ["COUNT(*) AS total_rows"]
    for c in columns:
        c = c.strip()
        parts.append(f"COUNT({c}) AS {c}_not_null")
        parts.append(f"COUNT(*) - COUNT({c}) AS {c}_null")
        parts.append(f"ROUND(100.0 * COUNT({c}) / COUNT(*), 2) AS {c}_not_null_pct")
        parts.append(f"ROUND(100.0 * (COUNT(*) - COUNT({c})) / COUNT(*), 2) AS {c}_null_pct")
    return ("NullStats", "SELECT \n  " + ",\n  ".join(parts) + f"\nFROM {table_ref};")

def generate_distinct_preview_query(table_ref, columns):
    col_list = ", ".join(columns[:5])  # limit preview to first 5 columns if too many
    return ("DistinctPreview", f"SELECT DISTINCT {col_list} FROM {table_ref} LIMIT 3;")

def generate_phone_query(table_ref, columns):
    parts = [f"COUNT(*) FILTER (WHERE {c} ~ '^\\d{{3}}-\\d{{3}}-\\d{{4}}$') AS {c}_valid_phone" for c in columns]
    return ("PhoneFormat", "SELECT \n  " + ",\n  ".join(parts) + f"\nFROM {table_ref};")

def generate_dateformat_query(table_ref, columns):
    date_cols = [c for c in columns if 'date' in c.lower()]
    if not date_cols:
        return None
    parts = [f"COUNT(*) FILTER (WHERE {c} ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}$') AS {c}_valid_date" for c in date_cols]
    return ("DateFormat", "SELECT \n  " + ",\n  ".join(parts) + f"\nFROM {table_ref};")

def run_validation(input_file='input.xlsx', output_file='sql_output_v3.xlsx'):
    df = pd.read_excel(input_file)
    df["Database"] = df["Database"].fillna("")
    df.columns = [col.strip() for col in df.columns]
    df["Clmns"] = df["Clmns"].astype(str).str.strip()
    df["Data_Type"] = df["Data_Type"].astype(str).str.strip()

    all_queries = []
    grouped = df.groupby(["Database", "Schema", "Table"])

    for (database, schema, table), group_df in grouped:
        table_ref = build_table_ref(database, schema, table)
        all_columns = group_df["Clmns"].tolist()

        #  Generate null query once per group
        all_queries.append(generate_null_query(table_ref, all_columns))

        #  Filter varchar columns
        varchar_cols = group_df[group_df["Data_Type"].str.lower().str.startswith("varchar")]["Clmns"].tolist()

        #  Apply phone format only on varchar
        if varchar_cols:
            all_queries.append(generate_phone_query(table_ref, varchar_cols))

        #  Apply date format only to varchar columns that contain 'date'
        date_q = generate_dateformat_query(table_ref, varchar_cols)
        if date_q:
            all_queries.append(date_q)

        #  Generate distinct preview (one row, up to 5 columns)
        all_queries.append(generate_distinct_preview_query(table_ref, all_columns))

    # Convert to DataFrame
    output_rows = []
    for entry in all_queries:
        if isinstance(entry, tuple):
            output_rows.append({"Validator": entry[0], "Query": entry[1]})
        elif isinstance(entry, dict):
            output_rows.append(entry)

    pd.DataFrame(output_rows).to_excel(output_file, index=False)
    print(f" All enhanced queries saved to {output_file}")

if __name__ == '__main__':
    run_validation()
