
import pandas as pd

def build_table_ref(database, schema, table):
    return f"{database}.{schema}.{table}" if database else f"{schema}.{table}"

def generate_null_query_per_column(table_ref, columns):
    queries = []
    for c in columns:
        query = f"""SELECT 
  '{c}' AS Clmns,
  COUNT(*) AS Total_rows,
  COUNT({c}) AS Not_null,
  ROUND(100.0 * COUNT({c}) / COUNT(*), 2) AS Not_null_percentage,
  COUNT(*) - COUNT({c}) AS Null,
  ROUND(100.0 * (COUNT(*) - COUNT({c})) / COUNT(*), 2) AS Null_percentage
FROM {table_ref}"""
        queries.append({"Validator": "NullStats", "Query": query})
    return queries

def generate_combined_distinct_query(table_ref, columns):
    col_list = ", ".join(columns)
    query = f"SELECT DISTINCT {col_list} FROM {table_ref} LIMIT 3;"
    return {"Validator": "DistinctPreview", "Query": query}

def generate_dateformat_query(table_ref, columns):
    parts = [
        f"COUNT(*) FILTER (WHERE {c} ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}$') AS {c}_valid_date"
        for c in columns
        if 'date' in c.lower()
    ]
    if parts:
        return ("DateFormat", "SELECT \n  " + ",\n  ".join(parts) + f"\nFROM {table_ref};")
    return None

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

        all_queries += generate_null_query_per_column(table_ref, all_columns)
        all_queries.append(generate_combined_distinct_query(table_ref, all_columns))

        varchar_cols = group_df[group_df["Data_Type"].str.lower().str.startswith("varchar")]["Clmns"].tolist()
        dateformat_q = generate_dateformat_query(table_ref, varchar_cols)
        if dateformat_q:
            all_queries.append({"Validator": dateformat_q[0], "Query": dateformat_q[1]})

    pd.DataFrame(all_queries).to_excel(output_file, index=False)
    print(f" Enhanced queries saved to {output_file}")

if __name__ == '__main__':
    run_validation()
