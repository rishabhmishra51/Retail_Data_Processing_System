
import os
import pandas as pd

RAW_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")


def ingest_file(filename: str, raw_dir: str = RAW_DIR) -> pd.DataFrame:

    filepath = os.path.join(raw_dir, filename)
    if not os.path.exists(filepath):
        print(f"[INGEST] WARNING: File not found — {filepath}")
        return pd.DataFrame()

    ext = os.path.splitext(filename)[1].lower()
    if ext==".xlsx":
        df=pd.read_excel(filepath, engine="openpyxl")
    elif ext==".xls":
        df=pd.read_excel(filepath)
    elif ext==".csv":
        df=pd.read_csv(filepath)
    else:
        print(f"[INGEST] Unsupported format: {ext} — skipping {filename}")
        return pd.DataFrame()

    print(f"[INGEST] Loaded {filename}: {len(df)} rows, {len(df.columns)} columns")
    return df


SUPPORTED_EXTENSIONS = {".xlsx", ".xls", ".csv"}


def ingest_all(raw_dir: str = RAW_DIR) -> dict:
    data = {}
    if not os.path.isdir(raw_dir):
        print(f"[INGEST] Raw directory does not exist: {raw_dir}")
        return data

    for f in sorted(os.listdir(raw_dir)):
        ext=os.path.splitext(f)[1].lower()
        if ext in SUPPORTED_EXTENSIONS:
            table_name = os.path.splitext(f)[0]
            data[table_name] = ingest_file(f, raw_dir)

    print(f"[INGEST] Total files ingested: {len(data)}")
    return data
