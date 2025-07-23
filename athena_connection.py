
import pandas as pd

def build_table_ref(database, schema, table):
    return f"{database}.{schema}.{table}" if database else f"{schema}.{table}"

def generate_null_query(table_ref, columns):
    parts = ["COUNT(*) AS total_rows"]
    parts += [f"COUNT({c}) AS {c}_not_null, COUNT(*) - COUNT({c}) AS {c}_null" for c in columns]
    return ("Null", f"SELECT \n  " + ",\n  ".join(parts) + f"\nFROM {table_ref};")

def generate_distinct_query(table_ref, columns):
    parts = [f"COUNT(DISTINCT {c}) AS {c}_distinct" for c in columns]
    return ("Distinct", f"SELECT \n  " + ",\n  ".join(parts) + f"\nFROM {table_ref};")

def generate_length_query(table_ref, columns):
    parts = [f"MAX(LENGTH({c})) AS {c}_max_len, MIN(LENGTH({c})) AS {c}_min_len" for c in columns]
    return ("Length", f"SELECT \n  " + ",\n  ".join(parts) + f"\nFROM {table_ref};")

def generate_blankspaces_query(table_ref, columns):
    parts = [f"COUNT(*) FILTER (WHERE TRIM(CAST({c} AS VARCHAR)) = '') AS {c}_blank" for c in columns]
    return ("BlankSpaces", f"SELECT \n  " + ",\n  ".join(parts) + f"\nFROM {table_ref};")

def generate_isnumeric_query(table_ref, columns):
    parts = [f"COUNT(*) FILTER (WHERE {c} ~ '^[0-9]+$') AS {c}_numeric" for c in columns]
    return ("IsNumeric", f"SELECT \n  " + ",\n  ".join(parts) + f"\nFROM {table_ref};")

def generate_specialchar_query(table_ref, columns):
    parts = [f"COUNT(*) FILTER (WHERE {c} ~ '[^a-zA-Z0-9 ]') AS {c}_special_chars" for c in columns]
    return ("SpecialChars", f"SELECT \n  " + ",\n  ".join(parts) + f"\nFROM {table_ref};")

def generate_email_query(table_ref, columns):
    email_cols = [c for c in columns if 'email' in c.lower()]
    if not email_cols:
        return None
    parts = [f"COUNT(*) FILTER (WHERE {c} ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{{2,}}$') AS {c}_valid_email" for c in email_cols]
    return ("EmailFormat", f"SELECT \n  " + ",\n  ".join(parts) + f"\nFROM {table_ref};")

def generate_phone_query(table_ref, columns):
    parts = [f"COUNT(*) FILTER (WHERE {c} ~ '^\\d{{3}}-\\d{{3}}-\\d{{4}}$') AS {c}_valid_phone" for c in columns]
    return ("PhoneFormat", f"SELECT \n  " + ",\n  ".join(parts) + f"\nFROM {table_ref};")

def generate_dateformat_query(table_ref, columns):
    parts = [f"COUNT(*) FILTER (WHERE {c} ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}$') AS {c}_valid_date" for c in columns]
    return ("DateFormat", f"SELECT \n  " + ",\n  ".join(parts) + f"\nFROM {table_ref};")

def generate_constant_query(table_ref, columns):
    parts = [f"(SELECT CASE WHEN COUNT(DISTINCT {c}) = 1 THEN 'constant' ELSE 'variable' END AS {c}_consistency FROM {table_ref})" for c in columns]
    return ("ConstantCheck", ";\nUNION ALL\n".join(parts) + ";")

def generate_outliers_query(table_ref, columns):
    parts = [f"MIN({c}) AS {c}_min, MAX({c}) AS {c}_max, AVG({c}) AS {c}_avg" for c in columns]
    return ("Outliers", f"SELECT \n  " + ",\n  ".join(parts) + f"\nFROM {table_ref};")

def generate_leading_trailing_query(table_ref, columns):
    parts = [f"COUNT(*) FILTER (WHERE {c} != TRIM(CAST({c} AS VARCHAR))) AS {c}_trim_mismatch" for c in columns]
    return ("LeadingTrailing", f"SELECT \n  " + ",\n  ".join(parts) + f"\nFROM {table_ref};")

def generate_invalid_date_query(table_ref, columns):
    parts = [f"COUNT(*) FILTER (WHERE {c} > current_date OR {c} < DATE '1900-01-01') AS {c}_invalid_date" for c in columns]
    return ("InvalidDateValues", f"SELECT \n  " + ",\n  ".join(parts) + f"\nFROM {table_ref};")

def generate_reference_check(table_ref, column, ref_values):
    values = ", ".join([f"'{val}'" for val in ref_values])
    return {
        "Validator": f"RefCheck_{column}",
        "Query": f"SELECT COUNT(*) AS {column}_not_in_ref FROM {table_ref} WHERE {column} NOT IN ({values});"
    }

def generate_duplicate_check(table_ref, columns):
    col_list = ", ".join(columns)
    return ("Duplicates", f"SELECT {col_list}, COUNT(*) AS duplicate_count FROM {table_ref} GROUP BY {col_list} HAVING COUNT(*) > 1;")

def run_validation(input_file='input.xlsx', output_file='sql_output.xlsx'):
    df = pd.read_excel(input_file)
    df["Database"] = df["Database"].fillna("")
    df.columns = [col.strip() for col in df.columns]

    grouped = df.groupby(["Database", "Schema", "Table"])
    all_queries = []

    for (database, schema, table), group_df in grouped:
        table_ref = build_table_ref(database, schema, table)
        columns = group_df["Clmns"].dropna().astype(str).str.strip().unique().tolist()

        all_queries.append(generate_null_query(table_ref, columns))
        all_queries.append(generate_distinct_query(table_ref, columns))
        all_queries.append(generate_length_query(table_ref, columns))
        all_queries.append(generate_blankspaces_query(table_ref, columns))
        all_queries.append(generate_isnumeric_query(table_ref, columns))
        all_queries.append(generate_specialchar_query(table_ref, columns))
        email_q = generate_email_query(table_ref, columns)
        if email_q: all_queries.append(email_q)
        all_queries.append(generate_phone_query(table_ref, columns))
        all_queries.append(generate_dateformat_query(table_ref, columns))
        all_queries.append(generate_constant_query(table_ref, columns))
        all_queries.append(generate_outliers_query(table_ref, columns))
        all_queries.append(generate_leading_trailing_query(table_ref, columns))
        all_queries.append(generate_invalid_date_query(table_ref, columns))
        all_queries.append(generate_duplicate_check(table_ref, columns))
        if "state_cd" in columns:
            ref_states = ['CA', 'TX', 'NY', 'FL', 'IL']
            all_queries.append(generate_reference_check(table_ref, "state_cd", ref_states))

    pd.DataFrame(all_queries, columns=["Validator", "Query"]).to_excel(output_file, index=False)
    print(f" Queries saved to {output_file}")

if __name__ == '__main__':
    run_validation()
