import os
import sqlite3
import pandas as pd

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "database", "retail.db")


def _get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    # FK checks disabled during ETL loading to avoid dependency-order issues
    conn.execute("PRAGMA foreign_keys = OFF;")
    return conn


def load_table(cleaned_df: pd.DataFrame, table_name: str, db_path: str = DB_PATH) -> None:
    
    if cleaned_df.empty:
        print(f"[LOAD] Skipping {table_name} — no data.")
        return

    conn = _get_connection(db_path)

    # Check if table exists; if so, keep only columns that exist in the table
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info('{table_name}')")
    table_cols = [row[1] for row in cursor.fetchall()]

    if table_cols:
        # Table exists — delete old rows and append (preserves schema)
        # Only insert columns present in BOTH the DataFrame and the table
        common_cols = [c for c in cleaned_df.columns if c in table_cols]
        insert_df = cleaned_df[common_cols]
        cursor.execute(f"DELETE FROM [{table_name}]")
        insert_df.to_sql(table_name, conn, if_exists="append", index=False)
    else:
        # Table does not exist — create from DataFrame
        cleaned_df.to_sql(table_name, conn, if_exists="replace", index=False)

    conn.commit()
    conn.close()
    print(f"[LOAD] Loaded {len(cleaned_df)} rows into '{table_name}'.")


def load_rejects(rejected_df: pd.DataFrame, table_name: str, db_path: str = DB_PATH) -> None:
    """
    Load rejected rows into the corresponding reject table.
    """
    reject_table = f"rejected_{table_name}"
    if rejected_df.empty:
        print(f"[LOAD] No rejects for {table_name}.")
        return