import pandas as pd

# 🧠 This function generates all the required validation queries for a given column
def generate_queries(schema, table, column):
    fq_table = f"{schema}.{table}"  # Fully qualified table: schema.table
    queries = []

    # 1️⃣ SELECT Query
    queries.append({
        "Validator": "Select",
        "Query": f"SELECT {column} FROM {fq_table};"
    })

    # 2️⃣ NULL Check Query
    queries.append({
        "Validator": "Null",
        "Query": f"""SELECT 
    COUNT(*) AS total_rows,
    COUNT({column}) AS non_null_count,
    COUNT(*) - COUNT({column}) AS null_count
FROM {fq_table};"""
    })

    # 3️⃣ DISTINCT Count Query
    queries.append({
        "Validator": "Distinct",
        "Query": f"SELECT COUNT(DISTINCT {column}) AS distinct_count FROM {fq_table};"
    })

    # 4️⃣ LENGTH Check Query
    queries.append({
        "Validator": "Length",
        "Query": f"""SELECT 
    MAX(LENGTH({column})) AS max_len, 
    MIN(LENGTH({column})) AS min_len 
FROM {fq_table};"""
    })

    # 5️⃣ IS NUMERIC Check Query (checks if column contains only digits)
    queries.append({
        "Validator": "IsNumeric",
        "Query": f"SELECT {column} FROM {fq_table} WHERE {column} ~ '^[0-9]+$';"
    })

    # 6️⃣ BLANK SPACES Check Query (detects values that are just empty strings or spaces)
    queries.append({
        "Validator": "BlankSpaces",
        "Query": f"SELECT {column} FROM {fq_table} WHERE TRIM({column}) = '';"
    })

    return queries

# 🧪 Main driver function to read Excel and generate SQLs
def main():
    input_file = "input.xlsx"  # Update this with your actual input Excel file
    df = pd.read_excel(input_file)
    all_queries = []

    # Loop through each row in the input
    for _, row in df.iterrows():
        schema = row["Schema"]
        table = row["Database"]
        # Multiple columns may be comma-separated — clean and split them
        column_list = [col.strip() for col in str(row["Clmns"]).split(",")]

        # For each column, generate all queries
        for col in column_list:
            query_set = generate_queries(schema, table, col)
            all_queries.extend(query_set)

    # Convert list of queries into a DataFrame
    output_df = pd.DataFrame(all_queries)

    # Write to Excel
    output_df.to_excel("sql_output.xlsx", sheet_name="generated_sql", index=False)
    print("✅ SQL queries written to sql_output.xlsx")

# 📌 Execute main
if __name__ == "__main__":
    main()
