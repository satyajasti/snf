import pandas as pd
import chardet
import os

DEFAULT_CSV = "data/hoscda_ingest.csv"

if not os.path.exists(DEFAULT_CSV):
    raise FileNotFoundError(f" CSV file not found: {DEFAULT_CSV}")

# --- Detect encoding automatically ---
with open(DEFAULT_CSV, 'rb') as f:
    enc = chardet.detect(f.read(100000))['encoding']

print(f" Detected encoding: {enc}")

# --- Try reading CSV with auto-detected encoding and robust fallback ---
try:
    df = pd.read_csv(DEFAULT_CSV, encoding=enc, sep=',', parse_dates=['load_dt'])
except Exception as e:
    print(f"  Issue reading with comma separator: {e}")
    try:
        df = pd.read_csv(DEFAULT_CSV, encoding=enc, sep=';', parse_dates=['load_dt'])
        print(" Successfully read using semicolon separator.")
    except Exception as e2:
        print(f"  Failed again, reading without date parsing: {e2}")
        df = pd.read_csv(DEFAULT_CSV, encoding=enc, sep=',', engine='python')

# --- If load_dt missing, fix and log ---
if 'load_dt' not in df.columns:
    print("  Column 'load_dt' not found — skipping date parsing.")
else:
    df['load_dt'] = pd.to_datetime(df['load_dt'], errors='coerce')

print(" CSV successfully loaded. Columns detected:")
print(df.columns.tolist())
print(df.head(5))
