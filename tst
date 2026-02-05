import os
import pandas as pd
from datetime import datetime

def scan_root_folder(root_path: str) -> pd.DataFrame:
    """
    Scans each immediate subfolder under root_path.
    For each subfolder, counts .zip files (only at that level) and lists zip names.
    """
    rows = []

    if not os.path.isdir(root_path):
        raise ValueError(f"Root path does not exist or is not a folder: {root_path}")

    for item in sorted(os.listdir(root_path)):
        folder_path = os.path.join(root_path, item)

        # Only process folders (like _AIMANTHBCBS_SC, 1HEALTH_M_AGG, etc.)
        if not os.path.isdir(folder_path):
            continue

        # Find .zip files directly inside this folder (not inside Failed/Finished etc.)
        zip_files = [
            f for f in os.listdir(folder_path)
            if os.path.isfile(os.path.join(folder_path, f)) and f.lower().endswith(".zip")
        ]

        rows.append({
            "File Name": item,
            "NoofFiles to be processed": len(zip_files),
            "FileNameZip": ", ".join(zip_files) if zip_files else ""
        })

    return pd.DataFrame(rows)


def write_to_excel(df: pd.DataFrame, output_excel_path: str):
    # Create folder if needed
    out_dir = os.path.dirname(output_excel_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    with pd.ExcelWriter(output_excel_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="zip_report", index=False)

    print(f"✅ Excel created: {output_excel_path}")


if __name__ == "__main__":
    # Change this to your main/root folder path
    ROOT_FOLDER = r"C:\path\to\your\main\folder"

    # Output excel in the same root folder with timestamp
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    OUTPUT_EXCEL = os.path.join(ROOT_FOLDER, f"folder_zip_report_{ts}.xlsx")

    print(f"Scanning root folder: {ROOT_FOLDER}")
    report_df = scan_root_folder(ROOT_FOLDER)

    print("Preview:")
    print(report_df.head(20).to_string(index=False))

    write_to_excel(report_df, OUTPUT_EXCEL)