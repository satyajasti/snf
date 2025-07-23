import pandas as pd

def build_table_ref(database, schema, table):
    return f"{database}.{schema}.{table}" if database else f"{schema}.{table}"

def generate_null_query(table_ref, columns):
    parts = ["COUNT(*) AS total_rows"]
    parts += [f"COUNT({c}) AS {c}_not_null,\n  COUNT(*) - COUNT({c}) AS {c}_null" for c in columns]
    return ("Null", f"SELECT \n  " + ",\n  ".join(parts) + f"\nFROM {table_ref};")

def generate_distinct_query(table_ref, columns):
    parts = [f"COUNT(DISTINCT {c}) AS {c}_distinct" for c in columns]
    return ("Distinct", f"SELECT \n  " + ",\n  ".join(parts) + f"\nFROM {table_ref};")

def generate_length_query(table_ref, columns):
    parts = [f"MAX(LENGTH({c})) AS {c}_max_len,\n  MIN(LENGTH({c})) AS {c}_min_len" for c in columns]
    return ("Length", f"SELECT \n  " + ",\n  ".join(parts) + f"\nFROM {table_ref};")

def generate_blankspaces_query(table_ref, columns):
    parts = [f"COUNT(*) FILTER (WHERE TRIM({c}) = '') AS {c}_blank" for c in columns]
    return ("BlankSpaces", f"SELECT \n  " + ",\n  ".join(parts) + f"\nFROM {table_ref};")

def generate_isnumeric_query(table_ref, columns):
    parts = [f"COUNT(*) FILTER (WHERE {c} ~ '^[0-9]+$') AS {c}_numeric" for c in columns]
    return ("IsNumeric", f"SELECT \n  " + ",\n  ".join(parts) + f"\nFROM {table_ref};")

def main():
    input_file = "input.xlsx"
    output_file = "sql_output.xlsx"
    
    df = pd.read_excel(input_file)
    df.columns = [col.strip() for col in df.columns]
    df["Database"] = df["Database"].fillna("")  # Handle missing DBs

    all_queries = []

    grouped = df.groupby(["Database", "Schema", "Table"])

    for (database, schema, table), group_df in grouped:
        table_ref = build_table_ref(database, schema, table)
        columns = group_df["Clmns"].dropna().astype(str).str.strip()
        columns = columns[columns != ""].unique().tolist()

        if not columns:
            continue

        print(f"\n📌 Table: {table_ref}")

        for generator in [
            generate_null_query,
            generate_distinct_query,
            generate_length_query,
            generate_blankspaces_query,
            generate_isnumeric_query,
        ]:
            validator, query = generator(table_ref, columns)
            print(f"🔹 Validator: {validator}")
            print(f"{query}\n")
            all_queries.append({"Validator": validator, "Query": query})

    if all_queries:
        pd.DataFrame(all_queries).to_excel(output_file, sheet_name="generated_sql", index=False)
        print(f"✅ All queries written to {output_file}")
    else:
        print("⚠️ No queries generated. Check your input.")

if __name__ == "__main__":
    main()
