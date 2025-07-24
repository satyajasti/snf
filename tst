import pandas as pd

# Input/Output
input_excel = "input_columns.xlsx"
output_excel = "generated_validations.xlsx"

# Read metadata from Excel
df = pd.read_excel(input_excel)

# Result rows
validation_rows = []

# Generate for each table
for table in df['Table'].unique():
    subset = df[df['Table'] == table]
    
    # Resolve schema.table or just table
    schema = subset['Schema'].dropna().unique()
    database = subset['Database'].dropna().unique()

    full_table = table
    if len(schema) > 0:
        full_table = f"{schema[0]}.{table}"
    if len(database) > 0:
        full_table = f"{database[0]}.{full_table}"

    # Build NULL check SQL
    null_parts = []
    for col in subset['Clmns']:
        sql = f"""
        SELECT '{col}' AS Clmns,
               COUNT(*) AS Total_row,
               COUNT({col}) AS Not_null,
               ROUND((COUNT({col}) * 100.0) / COUNT(*), 2) AS Not_null_percentage,
               COUNT(*) - COUNT({col}) AS Null,
               ROUND(((COUNT(*) - COUNT({col})) * 100.0) / COUNT(*), 2) AS Null_percentage
        FROM {full_table}
        """
        null_parts.append(sql.strip())
    
    full_null_sql = "\nUNION ALL\n".join(null_parts)

    validation_rows.append({
        'Validator': 'Null_Check',
        'SQL': full_null_sql
    })

# Write result to Excel
out_df = pd.DataFrame(validation_rows)
out_df.to_excel(output_excel, index=False, sheet_name="Validation_SQLs")

print(f"Validation SQLs written to {output_excel}")
