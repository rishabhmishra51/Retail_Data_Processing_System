import os
import pandas as pd
from database.setup import get_connection

CLEANED_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "cleaned")
REJECTED_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "rejected")


def load_table(df: pd.DataFrame, table_name: str) -> None:
    """Load cleaned DataFrame into the SQLite table."""
    if df.empty:
        print(f"[LOAD] Skipping empty table: {table_name}")
        return

    conn = get_connection()
    try:
        # Temporarily disable foreign keys for loading
        conn.execute("PRAGMA foreign_keys = OFF;")
        df.to_sql(table_name, conn, if_exists="append", index=False)
        conn.execute("PRAGMA foreign_keys = ON;")
        print(f"[LOAD] Loaded {len(df)} rows into {table_name}")
    except Exception as e:
        print(f"[LOAD] Error loading {table_name}: {e}")
    finally:
        conn.close()


def load_rejects(df: pd.DataFrame, table_name: str) -> None:
    """Load rejected DataFrame into the rejected table."""
    if df.empty:
        print(f"[LOAD] Skipping empty rejects: rejected_{table_name}")
        return

    reject_table = f"rejected_{table_name}"
    conn = get_connection()
    try:
        df.to_sql(reject_table, conn, if_exists="append", index=False)
        print(f"[LOAD] Loaded {len(df)} rejects into {reject_table}")
    except Exception as e:
        print(f"[LOAD] Error loading rejects {reject_table}: {e}")
    finally:
        conn.close()


def save_cleaned_csv(df: pd.DataFrame, table_name: str) -> None:
    """Save cleaned DataFrame to CSV in data/cleaned/."""
    if df.empty:
        print(f"[SAVE] Skipping empty cleaned CSV: {table_name}")
        return

    os.makedirs(CLEANED_DIR, exist_ok=True)
    filepath = os.path.join(CLEANED_DIR, f"{table_name}_cleaned.csv")
    df.to_csv(filepath, index=False)
    print(f"[SAVE] Saved {len(df)} rows to {filepath}")


def save_rejected_csv(df: pd.DataFrame, table_name: str) -> None:
    """Save rejected DataFrame to CSV in data/rejected/."""
    if df.empty:
        print(f"[SAVE] Skipping empty rejected CSV: {table_name}")
        return

    os.makedirs(REJECTED_DIR, exist_ok=True)
    filepath = os.path.join(REJECTED_DIR, f"{table_name}_rejected.csv")
    df.to_csv(filepath, index=False)
    print(f"[SAVE] Saved {len(df)} rejects to {filepath}")