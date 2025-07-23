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
    input_file = "input.xlsx"  # Your input file
    output_file = "sql_output.xlsx"

    try:
        df = pd.read_excel(input_file)
        print(f"✅ Read {len(df)} rows from '{input_file}'")
    except Exception as e:
        print(f"❌ Failed to read Excel file: {e}")
        return

    expected_cols = ['Database', 'Schema', 'Table', 'Clmns']
    if not all(col in df.columns for col in expected_cols):
        print(f"❌ Missing one or more required columns: {expected_cols}")
        print(f"🧾 Found columns: {list(df.columns)}")
        return

    all_queries = []

    # Group by Database, Schema, Table
    grouped = df.groupby(["Database", "Schema", "Table"])

    for (database, schema, table), group_df in grouped:
        table_ref = build_table_ref(database, schema, table)
        columns = group_df["Clmns"].dropna().astype(str).unique().tolist()

        if not columns:
            print(f"⚠️ No columns found for table {table_ref}, skipping.")
            continue

        print(f"\n📌 Generating queries for table: {table_ref}")
        print(f"🔹 Columns: {columns}")

        # Generate grouped validations
        null_query = generate_null_query(table_ref, columns)
        distinct_query = generate_distinct_query(table_ref, columns)

        all_queries.append(null_query)
        all_queries.append(distinct_query)

        # Print to console
        print(f"\n🔍 Null Query:\n{null_query['Query']}")
        print(f"\n🔍 Distinct Query:\n{distinct_query['Query']}")

        # Generate per-column queries
        per_col = generate_per_column_queries(table_ref, columns)
        for q in per_col:
            print(f"\n🔎 {q['Validator']} Query for column:\n{q['Query']}")
        all_queries.extend(per_col)

    if all_queries:
        output_df = pd.DataFrame(all_queries)
        output_df.to_excel(output_file, sheet_name="generated_sql", index=False)
        print(f"\n✅ SQL queries written to '{output_file}' in sheet 'generated_sql'")
    else:
        print("⚠️ No queries generated. Check your input.")

if __name__ == "__main__":
    main()
