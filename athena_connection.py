import pandas as pd

def build_table_ref(database, schema, table):
    if pd.isna(database) or database == "":
        return f"{schema}.{table}"
    return f"{database}.{schema}.{table}"

def generate_null_query(table_ref, columns):
    lines = ["COUNT(*) AS total_rows"]
    for col in columns:
        lines.append(f"COUNT({col}) AS {col}_not_null")
        lines.append(f"COUNT(*) - COUNT({col}) AS {col}_null")
    query = f"SELECT \n  " + ",\n  ".join(lines) + f"\nFROM {table_ref};"
    return {"Validator": "Null", "Query": query}

def generate_distinct_query(table_ref, columns):
    lines = [f"COUNT(DISTINCT {col}) AS {col}_distinct" for col in columns]
    query = f"SELECT \n  " + ",\n  ".join(lines) + f"\nFROM {table_ref};"
    return {"Validator": "Distinct", "Query": query}

def generate_per_column_queries(table_ref, columns):
    queries = []
    for col in columns:
        queries.append({
            "Validator": "Select",
            "Query": f"SELECT {col} FROM {table_ref};"
        })
        queries.append({
            "Validator": "Length",
            "Query": f"""SELECT 
  MAX(LENGTH({col})) AS max_len, 
  MIN(LENGTH({col})) AS min_len 
FROM {table_ref};"""
        })
        queries.append({
            "Validator": "IsNumeric",
            "Query": f"SELECT {col} FROM {table_ref} WHERE {col} ~ '^[0-9]+$';"
        })
        queries.append({
            "Validator": "BlankSpaces",
            "Query": f"SELECT {col} FROM {table_ref} WHERE TRIM({col}) = '';"
        })
    return queries

def main():
    input_file = "input.xlsx"  # Replace with your actual Excel file
    df = pd.read_excel(input_file)

    all_queries = []

    # Group by Database, Schema, Table to gather all columns per table
    grouped = df.groupby(["Database", "Schema", "Table"])

    for (database, schema, table), group_df in grouped:
        table_ref = build_table_ref(database, schema, table)
        columns = group_df["Clmns"].dropna().unique().tolist()

        # Generate: Null, Distinct (grouped)
        all_queries.append(generate_null_query(table_ref, columns))
        all_queries.append(generate_distinct_query(table_ref, columns))

        # Generate: One-per-column validations
        all_queries.extend(generate_per_column_queries(table_ref, columns))

    # Write to Excel
    output_df = pd.DataFrame(all_queries)
    output_df.to_excel("sql_output.xlsx", sheet_name="generated_sql", index=False)
    print("✅ SQL queries written to 'sql_output.xlsx' in sheet 'generated_sql'")

if __name__ == "__main__":
    main()
