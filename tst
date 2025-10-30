import json
import os
import glob
import pandas as pd

# === CONFIG ===
JSON_FOLDER = r"path\to\your\json_folder"   # <--- change this
OUTPUT_XLSX = "output_chunks.xlsx"
CHUCK_USE_BASENAME = True                    # True -> "abc"; False -> "abc.json"
CONFIG_JSON = None                           # e.g., r"path\to\config.json" or None
EXTRA_FIELDS = ["doc_type"]                  # fallback if CONFIG_JSON is None or missing
# ==============

def first_existing_key(d, *keys):
    """Return the value for the first key (case-insensitive) in dict d, else None."""
    if not isinstance(d, dict):
        return None
    lower_map = {k.lower(): k for k in d.keys()}
    for k in keys:
        if k.lower() in lower_map:
            return d[lower_map[k.lower()]]
    return None

def get_case_insensitive(d, wanted_key):
    """Fetch d[wanted_key] case-insensitively; return None if not found."""
    if not isinstance(d, dict):
        return None
    lk = wanted_key.lower()
    for k, v in d.items():
        if k.lower() == lk:
            return v
    return None

def ensure_list(x):
    """Coerce a value to list; if None -> []; if int/str -> [x]."""
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]

def nice_filename(name):
    """Remove extension and capitalize first letter (Satya from satya.txt)."""
    if not name:
        return ""
    base = os.path.splitext(str(name))[0]
    return base[:1].upper() + base[1:]

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# Load extra fields from config (if provided)
if CONFIG_JSON:
    try:
        cfg = load_json(CONFIG_JSON)
        if isinstance(cfg, dict) and "extra_fields" in cfg and isinstance(cfg["extra_fields"], list):
            EXTRA_FIELDS = cfg["extra_fields"]
    except Exception as e:
        print(f"Warning: failed to read CONFIG_JSON {CONFIG_JSON}: {e}")

# Collect all .json files
input_files = sorted(glob.glob(os.path.join(JSON_FOLDER, "*.json")))
if not input_files:
    raise FileNotFoundError(f"No .json files found in: {JSON_FOLDER}")

records = []
for jf in input_files:
    # Chuck: json filename (base or full)
    chuck = os.path.basename(jf)
    if CHUCK_USE_BASENAME:
        chuck = os.path.splitext(chuck)[0]

    try:
        data = load_json(jf)
    except Exception as e:
        print(f"Skipping {jf}: failed to parse JSON ({e})")
        continue

    # Items under "chinks"/"chunks" or a top-level list
    if isinstance(data, dict):
        items_key = None
        for k in data.keys():
            if k.lower() in {"chinks", "chunks"}:
                items_key = k
                break
        items = data.get(items_key, []) if items_key else []
    elif isinstance(data, list):
        items = data
    else:
        items = []

    for item in items:
        if not isinstance(item, dict):
            continue

        # filename
        fn_raw = first_existing_key(item, "Filename", "filename", "file_name") or ""
        filename = nice_filename(fn_raw)

        # pages
        pages = ensure_list(first_existing_key(item, "page_numbers", "pages", "page"))

        # dynamic extras (case-insensitive per key)
        extras = []
        for fld in EXTRA_FIELDS:
            val = get_case_insensitive(item, fld)
            extras.append(val if val is not None else "")

        for p in pages:
            try:
                p = int(p)
            except Exception:
                pass
            # record = [Chuck, Filename, pagenumber, *extras...]
            records.append([chuck, filename, p, *extras])

# Build dynamic columns
cols = ["Chuck", "Filename", "pagenumber"] + EXTRA_FIELDS
df = pd.DataFrame(records, columns=cols)

# Keep rows with a filename
df = df[df["Filename"].astype(str).str.len() > 0]

# Deduplicate/sort pages per unique combo (Chuck, Filename, extras)
group_keys = ["Chuck", "Filename"] + EXTRA_FIELDS
grouped = (
    df.groupby(group_keys, dropna=False)["pagenumber"]
      .apply(lambda s: sorted(pd.unique(s.dropna())))
      .reset_index()
)

# Expand to one row per page, repeating all keys on each row
rows = []
for _, r in grouped.iterrows():
    key_vals = [r[k] for k in group_keys]
    pages = r["pagenumber"] or []
    if not pages:
        rows.append([*key_vals, ""])
    else:
        for pg in pages:
            rows.append([*key_vals, pg])

final_cols = group_keys + ["pagenumber"]
final_df = pd.DataFrame(rows, columns=final_cols)

# Write to Excel
final_df.to_excel(OUTPUT_XLSX, index=False)
print(f"✅ Done. Wrote {len(final_df)} rows to {OUTPUT_XLSX}")
