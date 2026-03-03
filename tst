import os
import re
import gzip
from datetime import datetime
import pandas as pd

# ----------------------------
# CONFIG (EDIT THESE)
# ----------------------------
ROOT_FOLDER = r"C:\your\root\folder"   # <-- change
KEYWORD = "12345"                      # <-- change
USE_REGEX = False                      # True = KEYWORD treated as regex
ONLY_GZ_ENDING = ".txt.gz"             # set to ".gz" if you want all gz files
MAX_BYTES_TO_READ = 20_000_000         # 20MB safety limit
OUTPUT_XLSX = f"txt_gz_search_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"


def compile_pattern(keyword: str, use_regex: bool) -> re.Pattern:
    return re.compile(keyword if use_regex else re.escape(keyword), re.IGNORECASE)


def decode_bytes(raw: bytes) -> str:
    # try common encodings
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return raw.decode(enc)
        except Exception:
            pass
    return raw.decode("utf-8", errors="replace")


def main():
    pattern = compile_pattern(KEYWORD, USE_REGEX)
    rows = []

    for dirpath, _, filenames in os.walk(ROOT_FOLDER):
        for fn in filenames:
            lower = fn.lower()

            if ONLY_GZ_ENDING:
                if not lower.endswith(ONLY_GZ_ENDING.lower()):
                    continue
            else:
                if not lower.endswith(".gz"):
                    continue

            gz_path = os.path.join(dirpath, fn)

            try:
                with gzip.open(gz_path, "rb") as f:
                    raw = f.read(MAX_BYTES_TO_READ) if MAX_BYTES_TO_READ else f.read()

                text = decode_bytes(raw)

                for line_no, line in enumerate(text.splitlines(), start=1):
                    if pattern.search(line):
                        snippet = line.strip()
                        if len(snippet) > 200:
                            snippet = snippet[:200] + "..."
                        rows.append({
                            "folder_path": dirpath,
                            "gz_file": fn,
                            "keyword": KEYWORD,
                            "line_no": line_no,
                            "match_snippet": snippet,
                            "full_path": gz_path
                        })

            except Exception as e:
                rows.append({
                    "folder_path": dirpath,
                    "gz_file": fn,
                    "keyword": KEYWORD,
                    "line_no": "",
                    "match_snippet": f"ERROR: {type(e).__name__}: {e}",
                    "full_path": gz_path
                })

    df = pd.DataFrame(rows)
    if df.empty:
        df = pd.DataFrame([{
            "folder_path": "",
            "gz_file": "",
            "keyword": KEYWORD,
            "line_no": "",
            "match_snippet": "No matches found",
            "full_path": ""
        }])

    df.to_excel(OUTPUT_XLSX, index=False)
    print(f"Done. Results written to: {OUTPUT_XLSX}")


if __name__ == "__main__":
    main()
