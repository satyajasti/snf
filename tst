import pandas as pd
import chardet

# --- Detect encoding automatically ---
with open(DEFAULT_CSV, 'rb') as f:
    enc = chardet.detect(f.read(100000))['encoding']

# --- Try reading with auto-detected encoding and best-guess separator ---
try:
    df = pd.read_csv(DEFAULT_CSV, encoding=enc, sep=',', parse_dates=['load_dt'])
except Exception:
    # Retry with semicolon (some Excel versions save that way)
    df = pd.read_csv(DEFAULT_CSV, encoding=enc, sep=';', parse_dates=['load_dt'])

# --- If load_dt is missing, skip parse_dates and warn ---
if 'load_dt' not in df.columns:
    print(" Column 'load_dt' not found — reading without date parsing.")
    df = pd.read_csv(DEFAULT_CSV, encoding=enc, sep=',')
