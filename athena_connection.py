import pandas as pd
from behave import given, when, then
from common.snowflake_connection import get_snowflake_connection
from common.athena_connection import get_athena_connection
from common.config_loader import load_config_from_root
from common.table_loader import iterate_table_rows
from common.excel_writer import write_df_to_excel
from common.validation_tracker import ValidationTracker

@given('the config is loaded')
def step_load_config(context):
    config, config_path = load_config_from_root()
    print("Loaded keys", config.keys())
    context.app_config = config
    context.config_path = config_path
    print(f" Loaded config from : {config_path}")

@when('connect to {source_type}')
def step_connect_to_source(context, source_type):
    source_type = source_type.strip().lower()
    context.source_type = source_type
    config, config_path = load_config_from_root()
    context.app_config = config
    context.config_path = config_path

    if source_type == "snowflake":
        conn, database, schema, table_name, schema1, table_name1 = get_snowflake_connection(config_path)
        context.database = database
        context.schema = schema
        context.table_name = table_name
        context.schema1 = schema1
        context.table_name1 = table_name1
    elif source_type == "athena":
        conn, schema = get_athena_connection(config_path)
        context.database = "athena"
        context.schema = schema
        context.table_name = context.table_name1 = ""
        context.schema1 = ""
    else:
        raise ValueError(f"Unsupported source type: {source_type}")

    context.conn = conn
    context.tracker = ValidationTracker(context.conn)
    print(f" Connected to {source_type} successfully.")

@then('load table list from "{excel_path}"')
def step_load_table_list_from_excel(context, excel_path):
    context.table_rows = list(iterate_table_rows(excel_path))
    print(f" Loaded {len(context.table_rows)} tables from {excel_path}")

@then("write validation summary report")
def step_write_validation_summary(context):

    if not hasattr(context, "tracker"):
        print("No validation tracker found in context.")
        return

    summary_df = context.tracker.get_summary_df()

    write_df_to_excel("output", "summary", "validation_summary", summary_df)
    print(" Summary report saved to output/summary.xlsx")




{
  "source_type": "snowflake",  // not needed anymore if passed from feature
  "snowflake": {
    "user": "",
    "account": "",
    "authenticator": "externalbrowser",
    "warehouse": "",
    "database": "",
    "role": "",
    "schema": "",
    "schema1": "DG_DX"
  },
  "aws_athena": {
    "aws_access_key_id": "YOUR_KEY",
    "aws_secret_access_key": "YOUR_SECRET",
    "region_name": "us-east-1",
    "s3_staging_dir": "s3://your-staging-bucket/athena/",
    "schema_name": "your_athena_database",
    "workgroup": "primary"
  },
  "table_name": "",
  "table_name1": "",
  "primary_keys": ["S"]
}




from common.athena_connection import get_athena_connection

@when('connect to {source_type}')
def step_connect_to_source(context, source_type):
    source_type = source_type.strip().lower()
    context.source_type = source_type
    config, config_path = load_config_from_root()
    context.app_config = config
    context.config_path = config_path

    if source_type == "snowflake":
        conn, database, schema, table_name, schema1, table_name1 = get_snowflake_connection(config_path)
        context.database = database
        context.schema = schema
        context.table_name = table_name
        context.schema1 = schema1
        context.table_name1 = table_name1
    elif source_type == "athena":
        conn, schema = get_athena_connection(config_path)
        context.database = "athena"
        context.schema = schema
        context.table_name = context.table_name1 = ""
        context.schema1 = ""
    else:
        raise ValueError(f"Unsupported source type: {source_type}")

    context.conn = conn
    context.tracker = ValidationTracker(context.conn)
    print(f" Connected to {source_type} successfully.")
