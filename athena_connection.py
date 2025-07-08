from pyathena import connect
import json
import os

def get_athena_connection(config_file="config.json"):
    """
    Establishes a connection to AWS Athena using configuration from JSON file.
    Returns the connection and schema name.
    """
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Config file not found: {config_file}")

    with open(config_file) as f:
        config = json.load(f)

    if "aws_athena" not in config:
        raise KeyError("Missing 'aws_athena' section in config file")

    athena = config["aws_athena"]

    required_keys = ["aws_access_key_id", "aws_secret_access_key", "region_name", "s3_staging_dir", "schema_name"]
    for key in required_keys:
        if key not in athena:
            raise KeyError(f"Missing required key in aws_athena config: {key}")

    conn = connect(
        aws_access_key_id=athena["aws_access_key_id"],
        aws_secret_access_key=athena["aws_secret_access_key"],
        region_name=athena["region_name"],
        s3_staging_dir=athena["s3_staging_dir"],
        schema_name=athena["schema_name"],
        work_group=athena.get("workgroup", "primary")
    )

    print(" Athena connection established.")
    return conn, athena["schema_name"]




  "source_type": "athena",  // or "snowflake"

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
        "aws_access_key_id": "YOUR_ACCESS_KEY",
        "aws_secret_access_key": "YOUR_SECRET_KEY",
        "region_name": "us-east-1",
        "s3_staging_dir": "s3://your-staging-bucket/athena/",
        "schema_name": "your_athena_database",
        "workgroup": "primary"
    },
