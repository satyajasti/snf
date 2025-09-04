Feature: Snowflake DDL parity between source and target tables
  As a tester
  I want to compare table and column metadata (DDL) between two Snowflake tables
  So that I can detect schema drift without reading data

  Background:
    Given an Excel file "ddl_pairs.xlsx" with columns:
      | src_db | src_schema | src_table | tgt_db | tgt_schema | tgt_table |
    And optional flags in Excel (default values shown):
      | enforce_column_order | N |
      | ignore_comments      | Y |
      | ignore_collation     | Y |
    And a valid Snowflake connection via SQLAlchemy
    And the script will use:
      | DESCRIBE TABLE |
      | SHOW TABLES    |
      | SHOW COLUMNS IN TABLE |
      | SHOW PARAMETERS IN TABLE |
      | SHOW TAGS ON TABLE |


  @existence
  Scenario Outline: T01 – Both tables exist
    Given the source table <src_db>.<src_schema>.<src_table>
    And the target table <tgt_db>.<tgt_schema>.<tgt_table>
    When I run SHOW TABLES for both schemas with LIKE on the table name
    Then exactly one row is returned for each table
    And the result is recorded as "PASS"
    And if any table is missing, mark "BLOCKER" and stop further checks for that pair

    Examples:
      | src_db | src_schema | src_table | tgt_db | tgt_schema | tgt_table |
      | HOSCDA | PUBLIC     | ABC       | CDA    | PUBLIC     | XYA       |


  @table_type
  Scenario: T02 – Table kind matches (Permanent/Transient/Temporary)
    Given both tables exist
    When I read "kind" from SHOW TABLES
    Then the kinds must be equal
    And differences are reported as "MAJOR"


  @table_params
  Scenario: T03 – Data retention (time-travel) matches
    Given both tables exist
    When I read DATA_RETENTION_TIME_IN_DAYS via SHOW PARAMETERS IN TABLE
    Then the values must be equal
    And differences are reported under table_props_diff

  @table_params
  Scenario: T04 – Change tracking matches
    Given both tables exist
    When I read CHANGE_TRACKING via SHOW PARAMETERS IN TABLE
    Then the values must be equal
    And differences are reported under table_props_diff

  @clustering
  Scenario: T05 – Clustering keys match (or both none)
    Given both tables exist
    When I retrieve clustering definition (from SHOW TABLES or DESCRIBE TABLE footer)
    Then normalized cluster expressions must match
    And differences are reported as "MAJOR"


  @columns
  Scenario Outline: C01 – Column presence is identical (set equality)
    Given both tables exist
    When I DESCRIBE TABLE for source and target
    Then the set of column names (kind='COLUMN') must match case-insensitively for unquoted identifiers
    And any columns only in source are listed in sheet schema_src_only
    And any columns only in target are listed in sheet schema_tgt_only
    And if any exist, mark "BLOCKER"

    Examples:
      | src_db | src_schema | src_table | tgt_db | tgt_schema | tgt_table |
      | HOSCDA | PUBLIC     | ABC       | CDA    | PUBLIC     | XYA       |


  @order
  Scenario: C02 – Column order matches when enforced
    Given enforce_column_order is "Y" for the pair
    When I compare the ordinal sequence of column names
    Then sequences must match exactly
    And mismatches are reported as "MINOR" if enforce_column_order="N", else "BLOCKER"


  @datatype
  Scenario: C03 – Data type matches per column (precision/scale/length included)
    Given both tables exist and column sets match
    When I compare the "type" field from DESCRIBE TABLE for each common column
    Then the types must be identical after whitespace normalization
    And any mismatch is recorded in schema_attr_diff with type_src vs type_tgt
    And severity is "BLOCKER"


  @nullability
  Scenario: C04 – Nullability matches per column
    Given both tables exist and column sets match
    When I compare the "null?" flag for each common column
    Then values must be equal (Y/N)
    And differences are recorded in schema_attr_diff
    And severity is "BLOCKER"


  @default
  Scenario: C05 – Default expressions match
    Given both tables exist and column sets match
    When I compare the "default" for each common column
    Then normalized default text must be equal (e.g., CURRENT_TIMESTAMP and CURRENT_TIMESTAMP())
    And differences are recorded in schema_attr_diff
    And severity is "MAJOR"


  @identity
  Scenario: C06 – Identity (autoincrement) definition matches
    Given both tables exist and column sets match
    When I check identity clauses within "type" (START, INCREMENT)
    Then identity presence and parameters must match
    And differences are recorded as "BLOCKER"


  @pk @access-conditional
  Scenario: C07 – Primary key membership matches (if visible)
    Given role has privilege to view PK via DESCRIBE TABLE or SHOW PRIMARY KEYS
    When I collect PK columns and their order from both tables
    Then sets (and order if enforced) must match
    And differences are recorded as "BLOCKER"
    But if privileges are insufficient, mark result "SKIPPED – insufficient privilege"


  @unique @access-conditional
  Scenario: C08 – Unique key membership matches (if visible)
    Given role has privilege to view unique constraints
    When I collect unique columns per constraint
    Then definitions must match
    And differences are recorded as "MAJOR"
    But if privileges are insufficient, mark "SKIPPED – insufficient privilege"


  @check @access-conditional
  Scenario: C09 – Check constraint text matches (if used)
    Given role can view check constraints via DESCRIBE TABLE
    When I compare "check" expressions per column
    Then normalized expressions must match
    And differences are recorded as "MAJOR"
    But if privileges are insufficient, mark "SKIPPED – insufficient privilege"


  @computed
  Scenario: C10 – Computed (expression) columns match
    Given both tables exist and column sets match
    When I compare the "expression" field where present
    Then expressions must match after whitespace normalization
    And differences are recorded as "MAJOR"


  @collation
  Scenario: C11 – Collation matches unless ignored
    Given both tables exist and ignore_collation="N"
    When I compare collation from SHOW COLUMNS or type annotations
    Then collations must match
    And differences are recorded as "MINOR"
    But if ignore_collation="Y", mark "SKIPPED – ignored by config"


  @masking
  Scenario: C12 – Masking policy applied columns match
    Given both tables exist
    When I compare "policy name" from DESCRIBE TABLE for each column
    Then policy names must match (both null or same policy)
    And differences are recorded as "MAJOR"


  @comments
  Scenario: C13 – Column comments match unless ignored
    Given both tables exist and ignore_comments="N"
    When I compare "comment" text for each column
    Then comments must match
    And differences are recorded as "MINOR"
    But if ignore_comments="Y", mark "SKIPPED – ignored by config"


  @table_tags
  Scenario: T06 – Table tags and values match (if used)
    Given both tables exist
    When I run SHOW TAGS ON TABLE for source and target
    Then tag names and values must match
    And differences are recorded as "MAJOR"


  @reporting
  Scenario: R01 – Results written to Excel per pair
    Given the comparison completes (pass/fail/skip)
    Then create the following sheets for the pair:
      | schema_src_only |
      | schema_tgt_only |
      | schema_attr_diff |
      | table_props_diff |
      | notes |
    And each sheet contains only metadata (no data rows from the tables)
    And console logs summarize PASS/FAIL/SKIP with severities


  @errors
  Scenario: E01 – Invalid Excel headers are handled
    Given required columns are missing or misspelled in Excel
    When I parse the input
    Then I raise a clear error listing missing headers
    And no Snowflake calls are executed


  @multi
  Scenario: R02 – Multiple pairs in one run
    Given the Excel contains multiple rows
    When I execute comparisons sequentially
    Then each pair is processed independently
    And failures in one pair do not stop others




------------------

# validators/ddl_lib.py
import re
from dataclasses import dataclass, field
from typing import Dict, List, Tuple
import pandas as pd
from sqlalchemy import text

# ---- Normalization helpers ---------------------------------------------------
def _norm_spaces(s: str | None) -> str | None:
    if s is None:
        return None
    return re.sub(r"\s+", " ", str(s)).strip()

def _norm_ident(s: str | None) -> str | None:
    if s is None:
        return None
    # Snowflake unquoted idents are folded to UPPER
    return s.strip('"').upper()

def _norm_type(s: str | None) -> str | None:
    # normalize type text (handles minor whitespace/case noise)
    if s is None:
        return None
    return _norm_spaces(s).upper()

# ---- Result aggregator -------------------------------------------------------
@dataclass
class PairResult:
    """Holds all sheets for one src-vs-tgt pair; later written to Excel."""
    key: str
    sheets: Dict[str, pd.DataFrame] = field(default_factory=dict)
    notes: List[str] = field(default_factory=list)

    def add_sheet(self, name: str, df: pd.DataFrame):
        # Excel sheet name limit 31
        safe = (name[:31]).replace("/", "_")
        self.sheets[safe] = df

    def add_note(self, msg: str):
        self.notes.append(msg)

# ---- Snowflake metadata queries ----------------------------------------------
def qualify(db: str, schema: str, table: str) -> str:
    if not (db and schema and table):
        raise ValueError("Database, schema, and table are required.")
    return f'"{db}"."{schema}"."{table}"'

def show_tables_like(conn, db: str, schema: str, table: str) -> pd.DataFrame:
    sql = f'SHOW TABLES LIKE \'{table}\' IN SCHEMA "{db}"."{schema}";'
    return pd.read_sql(text(sql), conn)

def describe_table(conn, fqtn: str) -> pd.DataFrame:
    sql = f"DESCRIBE TABLE {fqtn}"
    return pd.read_sql(text(sql), conn)

def show_params_table(conn, fqtn: str) -> pd.DataFrame:
    sql = f"SHOW PARAMETERS IN TABLE {fqtn}"
    return pd.read_sql(text(sql), conn)

def show_columns_in_table(conn, fqtn: str) -> pd.DataFrame:
    sql = f"SHOW COLUMNS IN TABLE {fqtn}"
    return pd.read_sql(text(sql), conn)

def show_tags_on_table(conn, fqtn: str) -> pd.DataFrame:
    sql = f"SHOW TAGS ON {fqtn}"
    return pd.read_sql(text(sql), conn)

# ---- Core comparisons ---------------------------------------------------------
def table_exists(conn, db: str, schema: str, table: str) -> bool:
    df = show_tables_like(conn, db, schema, table)
    # MATCHING rows have "name" == table (case-insensitive in Snowflake)
    return (df["name"].str.upper() == table.upper()).sum() == 1

def get_table_kind(conn, db: str, schema: str, table: str) -> str | None:
    df = show_tables_like(conn, db, schema, table)
    if df.empty:
        return None
    # columns vary slightly by client; "kind" or "is_transient" exist.
    if "kind" in df.columns:
        return str(df.loc[0, "kind"]).upper()
    if "is_transient" in df.columns:
        return "TRANSIENT" if str(df.loc[0, "is_transient"]).upper() == "Y" else "PERMANENT"
    return None

def get_cluster_by(conn, db: str, schema: str, table: str) -> str:
    df = show_tables_like(conn, db, schema, table)
    if "cluster_by" in df.columns and not df.empty:
        return _norm_spaces(str(df.loc[0, "cluster_by"] or "")) or ""
    # fallback: sometimes appears as a footer row in DESCRIBE TABLE (ignored here)
    return ""

def table_parameters(conn, fqtn: str) -> Dict[str, str]:
    df = show_params_table(conn, fqtn)
    if df.empty:
        return {}
    key_col = "key" if "key" in df.columns else "parameter_name"
    val_col = "value" if "value" in df.columns else "parameter_value"
    params = {}
    for _, r in df.iterrows():
        k = str(r[key_col]).upper()
        v = str(r[val_col])
        params[k] = v
    return params

def columns_from_describe(df_desc: pd.DataFrame) -> pd.DataFrame:
    # Keep only columns (exclude footer rows like "cluster by")
    cols = df_desc[df_desc["kind"].str.upper() == "COLUMN"].copy()
    # Normalize
    cols["name_n"] = cols["name"].apply(_norm_ident)
    cols["type_n"] = cols["type"].apply(_norm_type)
    cols["null_n"] = cols["null?"].astype(str).str.upper()
    # Optional presence columns in DESCRIBE TABLE
    for opt in ["default", "comment", "policy name", "check", "expression", "collation", "primary key", "unique key"]:
        if opt not in cols.columns:
            cols[opt] = None
    return cols

def diff_column_sets(src_cols: pd.DataFrame, tgt_cols: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    src_only = sorted(set(src_cols["name_n"]) - set(tgt_cols["name_n"]))
    tgt_only = sorted(set(tgt_cols["name_n"]) - set(src_cols["name_n"]))
    return (pd.DataFrame({"column": src_only}),
            pd.DataFrame({"column": tgt_only}))

def diff_column_attrs(src_cols: pd.DataFrame, tgt_cols: pd.DataFrame,
                      ignore_comments=True, ignore_collation=True) -> pd.DataFrame:
    merged = src_cols.merge(tgt_cols, on="name_n", how="inner", suffixes=("_src", "_tgt"))
    rows = []
    for _, r in merged.iterrows():
        diffs = {}
        if r["type_n_src"] != r["type_n_tgt"]:
            diffs["type_src"], diffs["type_tgt"] = r["type_n_src"], r["type_n_tgt"]
        if r["null_n_src"] != r["null_n_tgt"]:
            diffs["null_src"], diffs["null_tgt"] = r["null_n_src"], r["null_n_tgt"]
        # Default (normalize light)
        def_s = _norm_spaces(r["default_src"])
        def_t = _norm_spaces(r["default_tgt"])
        if def_s != def_t:
            diffs["default_src"], diffs["default_tgt"] = def_s, def_t
        # Identity detection (lives in type text)
        # (already covered by type comparison)

        # Unique/PK/Check/Expression
        for fld in ["primary key", "unique key", "check", "expression", "policy name"]:
            f_src = _norm_spaces(r.get(fld + "_src"))
            f_tgt = _norm_spaces(r.get(fld + "_tgt"))
            if f_src != f_tgt:
                diffs[fld.replace(" ", "_") + "_src"] = f_src
                diffs[fld.replace(" ", "_") + "_tgt"] = f_tgt

        # Collation & comments (optional)
        if not ignore_collation:
            col_s = _norm_spaces(r.get("collation_src"))
            col_t = _norm_spaces(r.get("collation_tgt"))
            if col_s != col_t:
                diffs["collation_src"], diffs["collation_tgt"] = col_s, col_t
        if not ignore_comments:
            c_s = _norm_spaces(r.get("comment_src"))
            c_t = _norm_spaces(r.get("comment_tgt"))
            if c_s != c_t:
                diffs["comment_src"], diffs["comment_tgt"] = c_s, c_t

        if diffs:
            diffs["column"] = r["name_n"]
            rows.append(diffs)
    return pd.DataFrame(rows)

def diff_table_props(conn, src_fq: str, tgt_fq: str,
                     src_q: Tuple[str, str, str], tgt_q: Tuple[str, str, str]) -> pd.DataFrame:
    # kind
    s_kind = get_table_kind(conn, *src_q)
    t_kind = get_table_kind(conn, *tgt_q)
    # retention & change tracking
    s_params = table_parameters(conn, src_fq)
    t_params = table_parameters(conn, tgt_fq)
    s_ret = s_params.get("DATA_RETENTION_TIME_IN_DAYS")
    t_ret = t_params.get("DATA_RETENTION_TIME_IN_DAYS")
    s_ct  = (s_params.get("CHANGE_TRACKING") or "").upper()
    t_ct  = (t_params.get("CHANGE_TRACKING") or "").upper()
    # clustering
    s_cluster = get_cluster_by(conn, *src_q)
    t_cluster = get_cluster_by(conn, *tgt_q)

    rows = []
    if s_kind != t_kind:
        rows.append({"property":"table_kind", "source": s_kind, "target": t_kind})
    if s_ret != t_ret:
        rows.append({"property":"data_retention_days", "source": s_ret, "target": t_ret})
    if s_ct != t_ct:
        rows.append({"property":"change_tracking", "source": s_ct, "target": t_ct})
    if s_cluster != t_cluster:
        rows.append({"property":"cluster_by", "source": s_cluster, "target": t_cluster})
    return pd.DataFrame(rows)

/environment.py

# features/support/environment.py
import os
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

from snowflake_connection import get_engine  # your existing function
from validators.ddl_lib import PairResult

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def before_all(context):
    context.engine = get_engine()
    context.report_by_pair = {}     # key -> PairResult
    context.flags = {
        "enforce_column_order": False,
        "ignore_comments": True,
        "ignore_collation": True,
    }
    context.current_pair_key = None

def after_all(context):
    # write a single workbook containing all pairs/sheets
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(OUTPUT_DIR, f"ddl_compare_{ts}.xlsx")
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        for pair_key, result in context.report_by_pair.items():
            # notes sheet
            if result.notes:
                pd.DataFrame({"notes": result.notes}).to_excel(
                    writer, sheet_name=(pair_key + "__notes")[:31], index=False
                )
            # all collected sheets
            for sname, df in result.sheets.items():
                df.to_excel(writer, sheet_name=(pair_key + "__" + sname)[:31], index=False)
    print(f"[INFO] DDL report written: {out_path}")




ddl_steps

# features/steps/ddl_steps.py
import pandas as pd
from behave import given, when, then, step
from validators.ddl_lib import (
    PairResult, qualify, table_exists, describe_table,
    columns_from_describe, diff_column_sets, diff_column_attrs,
    diff_table_props
)

# ---------- Utility -----------------------------------------------------------
def _pair_key(src_db, src_schema, src_table, tgt_db, tgt_schema, tgt_table):
    left = f"{src_db}.{src_schema}.{src_table}"
    right = f"{tgt_db}.{tgt_schema}.{tgt_table}"
    key = (left + "__VS__" + right).replace('"', '').replace(" ", "_")
    return key[:31]  # keep Excel-friendly

def _ensure_pair(context, src_db, src_schema, src_table, tgt_db, tgt_schema, tgt_table):
    key = _pair_key(src_db, src_schema, src_table, tgt_db, tgt_schema, tgt_table)
    if key not in context.report_by_pair:
        context.report_by_pair[key] = PairResult(key=key)
    context.current_pair_key = key
    return key, context.report_by_pair[key]

# ---------- Background steps --------------------------------------------------
@given('an Excel file "{path}" with columns')
def step_read_excel_columns(context, path):
    # This step documents the input; real reading typically happens in your driver.
    context.input_excel_path = path

@given('optional flags in Excel (default values shown)')
def step_optional_flags(context):
    # Using defaults defined in environment.py; you can wire Excel-driven overrides in your runner.
    pass

@given('a valid Snowflake connection via SQLAlchemy')
def step_have_connection(context):
    assert context.engine is not None, "Snowflake engine is not initialized."

# ---------- Existence ---------------------------------------------------------
@given('the source table {src_db}.{src_schema}.{src_table}')
def step_src_table(context, src_db, src_schema, src_table):
    context.src = (src_db, src_schema, src_table)

@given('the target table {tgt_db}.{tgt_schema}.{tgt_table}')
def step_tgt_table(context, tgt_db, tgt_schema, tgt_table):
    context.tgt = (tgt_db, tgt_schema, tgt_table)
    _ensure_pair(context, *context.src, *context.tgt)

@when('I run SHOW TABLES for both schemas with LIKE on the table name')
def step_show_tables_like(context):
    src_db, src_schema, src_table = context.src
    tgt_db, tgt_schema, tgt_table = context.tgt
    with context.engine.connect() as conn:
        context.src_exists = table_exists(conn, src_db, src_schema, src_table)
        context.tgt_exists = table_exists(conn, tgt_db, tgt_schema, tgt_table)

@then('exactly one row is returned for each table')
def step_expect_exists(context):
    key, res = _ensure_pair(context, *context.src, *context.tgt)
    if not context.src_exists or not context.tgt_exists:
        res.add_note("BLOCKER: One or both tables are missing.")
    assert context.src_exists and context.tgt_exists, "Table existence failed."

@then('the result is recorded as "PASS"')
def step_record_pass(context):
    key, res = _ensure_pair(context, *context.src, *context.tgt)
    res.add_note("PASS: Both tables exist.")

@then('if any table is missing, mark "BLOCKER" and stop further checks for that pair')
def step_blocker_stop(context):
    if not (context.src_exists and context.tgt_exists):
        context.scenario.skip("Blocking failure: table missing.")

# ---------- Table properties --------------------------------------------------
@when('I read "kind" from SHOW TABLES')
def step_read_kind(context):
    # handled inside diff_table_props later
    pass

@then('the kinds must be equal')
def step_kinds_equal(context):
    src_db, src_schema, src_table = context.src
    tgt_db, tgt_schema, tgt_table = context.tgt
    src_fq = qualify(*context.src); tgt_fq = qualify(*context.tgt)

    with context.engine.connect() as conn:
        df = diff_table_props(conn, src_fq, tgt_fq, context.src, context.tgt)
    key, res = _ensure_pair(context, *context.src, *context.tgt)
    if not df.empty:
        # keep all table prop diffs in one sheet
        res.add_sheet("table_props_diff", df)

# Retention, change tracking, clustering are covered in the same props diff step.

# ---------- Columns: presence & attributes -----------------------------------
@when('I DESCRIBE TABLE for source and target')
def step_describe_tables(context):
    src_fq = qualify(*context.src); tgt_fq = qualify(*context.tgt)
    with context.engine.connect() as conn:
        dsrc = describe_table(conn, src_fq)
        dtgt = describe_table(conn, tgt_fq)
    context.desc_src = dsrc
    context.desc_tgt = dtgt

@then('the set of column names (kind=\'COLUMN\') must match case-insensitively for unquoted identifiers')
def step_column_set_equal(context):
    src_cols = columns_from_describe(context.desc_src)
    tgt_cols = columns_from_describe(context.desc_tgt)
    src_only, tgt_only = diff_column_sets(src_cols, tgt_cols)

    key, res = _ensure_pair(context, *context.src, *context.tgt)
    if not src_only.empty:
        res.add_sheet("schema_src_only", src_only)
    if not tgt_only.empty:
        res.add_sheet("schema_tgt_only", tgt_only)
    # If either non-empty, this is a blocker in your policy
    assert src_only.empty and tgt_only.empty, "BLOCKER: Column sets differ."

@then('I compare per-column attributes with configured tolerances')
def step_compare_attrs(context):
    src_cols = columns_from_describe(context.desc_src)
    tgt_cols = columns_from_describe(context.desc_tgt)
    df = diff_column_attrs(
        src_cols, tgt_cols,
        ignore_comments=context.flags["ignore_comments"],
        ignore_collation=context.flags["ignore_collation"]
    )
    key, res = _ensure_pair(context, *context.src, *context.tgt)
    if not df.empty:
        res.add_sheet("schema_attr_diff", df)

# ---------- Order (optional) --------------------------------------------------
@then('the ordinal sequence must match when enforce_column_order is "Y"')
def step_order_check(context):
    # You can set this flag via config/Excel; defaults to False
    if not context.flags["enforce_column_order"]:
        return
    s = columns_from_describe(context.desc_src)["name_n"].tolist()
    t = columns_from_describe(context.desc_tgt)["name_n"].tolist()
    if s != t:
        key, res = _ensure_pair(context, *context.src, *context.tgt)
        res.add_sheet("column_order_diff", pd.DataFrame({"src_order": s, "tgt_order": t}))
        raise AssertionError('BLOCKER: Column order differs with enforcement enabled.')

# ---------- Tags / comments / policies (already covered in attrs diff) -------
# Masking policy and comments are compared in diff_column_attrs via "policy name" and "comment".




/ddl_compare.feature
Feature: Snowflake DDL parity between source and target tables (no data checks)
  As a tester
  I want to compare table and column metadata (DDL) between two Snowflake tables
  So that I can detect schema drift without reading any data

  Background:
    Given an Excel file "ddl_pairs.xlsx" with columns
      | src_db | src_schema | src_table | tgt_db | tgt_schema | tgt_table |
    And optional flags in Excel (default values shown)
      | enforce_column_order | N |
      | ignore_comments      | Y |
      | ignore_collation     | Y |
    And a valid Snowflake connection via SQLAlchemy

  @existence @schema @props
  Scenario Outline: DDL parity checks for a table pair
    Given the source table <src_db>.<src_schema>.<src_table>
    And the target table <tgt_db>.<tgt_schema>.<tgt_table>

    # Existence
    When I run SHOW TABLES for both schemas with LIKE on the table name
    Then exactly one row is returned for each table
    And the result is recorded as "PASS"
    And if any table is missing, mark "BLOCKER" and stop further checks for that pair

    # Column set & attributes (no data read)
    When I DESCRIBE TABLE for source and target
    Then the set of column names (kind='COLUMN') must match case-insensitively for unquoted identifiers
    And I compare per-column attributes with configured tolerances
    And the ordinal sequence must match when enforce_column_order is "Y"

    # Table-level properties
    When I read "kind" from SHOW TABLES
    Then the kinds must be equal

    Examples:
      | src_db | src_schema | src_table | tgt_db | tgt_schema | tgt_table |
      | HOSCDA | PUBLIC     | ABC       | CDA    | PUBLIC     | XYA       |



| src\_db | src\_schema | src\_table | tgt\_db | tgt\_schema | tgt\_table | enforce\_column\_order | ignore\_comments | ignore\_collation |
| ------- | ----------- | ---------- | ------- | ----------- | ---------- | ---------------------- | ---------------- | ----------------- |
| HOSCDA  | PUBLIC      | ABC        | CDA     | PUBLIC      | XYA        | N                      | Y                | Y                 |

