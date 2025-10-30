import json
import os
import glob
import pandas as pd

# === CONFIG ===
JSON_FOLDER = r"path\to\your\json_folder"   # <-- change this
OUTPUT_XLSX = "output_chunks.xlsx"
CHUCK_USE_BASENAME = True                    # True -> "abc"; False -> "abc.json"
INCLUDE_PRETTY_FILENAME = True               # Add pretty 'Filename' column
EXTRA_FIELDS = ["file_name", "filename", "case_name", "total_chunks"]  # <-- EXACT headers you want
# ==============

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def ensure_list(x):
    if x is None:
        return []
    return x if isinstance(x, list) else [x]

def normalize_key(k: str) -> str:
    # compares keys case-insensitively and treats '-', '_', ' ' the same
    return str(k).lower().replace("-", "_").replace(" ", "_")

def fuzzy_get(d: dict, *candidates):
    """Get a value using case/sep-insensitive keys. Returns None if not found."""
    if not isinstance(d, dict):
        return None
    norm_map = {normalize_key(k): k for k in d.keys()}
    for c in candidates:
        nc = normalize_key(c)
        if nc in norm_map:
            return d[norm_map[nc]]
    return None

def get_any(item: dict, root: dict, key: str):
    """Prefer item[key], else fallback to root[key]. Exact header name preserved."""
    v = fuzzy_get(item, key)
    if v is None:
        v = fuzzy_get(root, key)
    return v

def pretty_name_from_item(item):
    """Make a pretty 'Filename' from item['filename'|'file_name'|'file']."""
    raw = fuzzy_get(item, "filename", "file_name", "file")
    if not raw:
        return ""
    base = os.path.splitext(str(raw))[0]
    return base[:1].upper() + base[1:]

# --- collect files ---
files = sorted(glob.glob(os.path.join(JSON_FOLDER, "*.json")))
if not files:
    raise FileNotFoundError(f"No .json files found in: {JSON_FOLDER}")

rows = []

for path in files:
    # Chuck is the JSON file name
    chuck = os.path.basename(path)
    if CHUCK_USE_BASENAME:
        chuck = os.path.splitext(chuck)[0]

    try:
        data = load_json(path)
    except Exception as e:
        print(f"Skip {path}: {e}")
        continue

    # locate list under 'chunks'/'chinks' or accept a top-level list
    if isinstance(data, dict):
        items_key = None
        for k in data.keys():
            if normalize_key(k) in {"chunks", "chinks"}:
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

        # pages
        pages = ensure_list(fuzzy_get(item, "page_numbers", "pages", "page"))

        # optional pretty filename (for display)
        pretty_fn = pretty_name_from_item(item) if INCLUDE_PRETTY_FILENAME else None

        # build base dict for this item (repeat on every page)
        base = {"Chuck": chuck}
        if INCLUDE_PRETTY_FILENAME:
            base["Filename"] = pretty_fn

        # add EXTRA_FIELDS exactly as requested (input header = output header)
        for fld in EXTRA_FIELDS:
            base[fld] = get_any(item, data, fld)

        # one row per page
        if pages:
            for p in pages:
                try:
                    p = int(p)
                except Exception:
                    pass
                row = dict(base)
                row["pagenumber"] = p
                rows.append(row)
        else:
            # if no pages, still emit a row (blank pagenumber)
            row = dict(base)
            row["pagenumber"] = ""
            rows.append(row)

# --- build DataFrame with exact column order ---
leading = ["Chuck"]
if INCLUDE_PRETTY_FILENAME:
    leading.append("Filename")
cols = leading + EXTRA_FIELDS + ["pagenumber"]

df = pd.DataFrame(rows)
# ensure all columns exist even if some never appeared
for c in cols:
    if c not in df.columns:
        df[c] = ""
df = df[cols]

# (Optional) sort
df = df.sort_values(by=["Chuck"] + ([ "Filename" ] if INCLUDE_PRETTY_FILENAME else []) + EXTRA_FIELDS + ["pagenumber"])

df.to_excel(OUTPUT_XLSX, index=False)
print(f"✅ Done. Columns = {cols}. Rows = {len(df)}. Wrote {OUTPUT_XLSX}")
