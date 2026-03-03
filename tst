import os
import re
import zipfile
from datetime import datetime

import pandas as pd


# ----------------------------
# CONFIG
# ----------------------------
ROOT_FOLDER = r"C:\your\root\folder"   # <-- change this
KEYWORD = r"12345"                    # <-- change this (word or number)
OUTPUT_XLSX = f"keyword_search_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

# If True, keyword treated as regex. If False, treated as plain text.
USE_REGEX = False

# Only scan these file extensions inside ZIP (set to None to scan all)
ALLOWED_EXTENSIONS = {".txt", ".csv", ".log", ".xml", ".json", ".sql"}

# Read limit per file to avoid huge files (None = no limit)
MAX_BYTES_TO_READ = 5_000_000  # 5 MB


def compile_pattern(keyword: str, use_regex: bool) -> re.Pattern:
    if use_regex:
        return re.compile(keyword, re.IGNORECASE)
    return re.compile(re.escape(keyword), re.IGNORECASE)


def is_allowed_file(filename: str) -> bool:
    if ALLOWED_EXTENSIONS is None:
        return True
    _, ext = os.path.splitext(filename.lower())
    return ext in ALLOWED_EXTENSIONS


def read_zip_member_as_text(zf: zipfile.ZipFile, member_name: str) -> str | None:
    """
    Tries to read a zip member as text using common encodings.
    Returns text if successful, else None (e.g., binary file).
    """
    try:
        with zf.open(member_name) as f:
            raw = f.read(MAX_BYTES_TO_READ) if MAX_BYTES_TO_READ else f.read()
    except Exception:
        return None

    # Try common encodings
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return raw.decode(enc, errors="strict")
        except Exception:
            continue

    # Last resort (won’t crash, but may look messy)
    try:
        return raw.decode("utf-8", errors="replace")
    except Exception:
        return None


def search_in_text(text: str, pattern: re.Pattern, context_chars: int = 120):
    """
    Returns list of (line_no, snippet) for each matching line.
    """
    results = []
    for i, line in enumerate(text.splitlines(), start=1):
        if pattern.search(line):
            line_clean = line.strip()
            # Keep snippet short
            if len(line_clean) > context_chars:
                line_clean = line_clean[:context_chars] + "..."
            results.append((i, line_clean))
    return results


def main():
    pattern = compile_pattern(KEYWORD, USE_REGEX)
    rows = []

    for dirpath, _, filenames in os.walk(ROOT_FOLDER):
        for fn in filenames:
            if not fn.lower().endswith(".zip"):
                continue

            zip_path = os.path.join(dirpath, fn)

            try:
                with zipfile.ZipFile(zip_path, "r") as zf:
                    for member in zf.namelist():
                        # skip directories
                        if member.endswith("/"):
                            continue

                        if not is_allowed_file(member):
                            continue

                        text = read_zip_member_as_text(zf, member)
                        if text is None:
                            continue

                        matches = search_in_text(text, pattern)
                        if matches:
                            for line_no, snippet in matches:
                                rows.append({
                                    "folder_path": dirpath,
                                    "zip_file": fn,
                                    "file_in_zip": member,
                                    "keyword": KEYWORD,
                                    "line_no": line_no,
                                    "match_snippet": snippet,
                                    "zip_full_path": zip_path,
                                })

            except zipfile.BadZipFile:
                rows.append({
                    "folder_path": dirpath,
                    "zip_file": fn,
                    "file_in_zip": "",
                    "keyword": KEYWORD,
                    "line_no": "",
                    "match_snippet": "ERROR: Bad zip file",
                    "zip_full_path": zip_path,
                })
            except Exception as e:
                rows.append({
                    "folder_path": dirpath,
                    "zip_file": fn,
                    "file_in_zip": "",
                    "keyword": KEYWORD,
                    "line_no": "",
                    "match_snippet": f"ERROR: {type(e).__name__}: {e}",
                    "zip_full_path": zip_path,
                })

    df = pd.DataFrame(rows)

    # If no matches, still create a file with a message
    if df.empty:
        df = pd.DataFrame([{
            "folder_path": "",
            "zip_file": "",
            "file_in_zip": "",
            "keyword": KEYWORD,
            "line_no": "",
            "match_snippet": "No matches found",
            "zip_full_path": "",
        }])

    df.to_excel(OUTPUT_XLSX, index=False)
    print(f"Done. Results written to: {OUTPUT_XLSX}")


if __name__ == "__main__":
    main()
