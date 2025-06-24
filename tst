from common.edl_utils import fetch_latest_edl_timestamp
import pandas as pd

# Define all validation types in one place
VALIDATION_KEYS = [
    "Null",
    "Duplicate",
    "Distinct",
    "Primary Key",
    "Bad Data",
    "Pattern",
    "Dup Val Exl Aud",
    "Audit",
    "Skew"
]

class ValidationTracker:
    def __init__(self, conn):
        self.conn = conn
        self.status = {}  # key = table_ref str, value = dict of validations

    def start_table(self, table_ref):
        key = str(table_ref)
        db, schema, table = table_ref.get_parts()
        edl_dtm = fetch_latest_edl_timestamp(self.conn, db, schema, table)

        self.status[key] = {
            "Database": db,
            "Schema": schema,
            "Table": table,
            "edl_load_dtm": edl_dtm,
            "Error": []
        }

        for vkey in VALIDATION_KEYS:
            self.status[key][vkey] = "Not Run"

    def update_status(self, table_ref, validation_type, result="Success", error=""):
        key = str(table_ref)
        if key not in self.status:
            self.start_table(table_ref)

        self.status[key][validation_type] = result
        if error:
            self.status[key]["Error"].append(f"{validation_type}: {error}")

    def get_summary_df(self):
        formatted_status = []
        for row in self.status.values():
            row_copy = dict(row)
            row_copy["Error"] = " | ".join(row_copy["Error"]) if isinstance(row_copy["Error"], list) else row_copy["Error"]
            formatted_status.append(row_copy)
        return pd.DataFrame(formatted_status)
