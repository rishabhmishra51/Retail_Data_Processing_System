import os
import pandas as pd

def save_cleaned_csv(cleaned_df: pd.DataFrame, table_name: str) -> None:
   
    cleaned_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "cleaned")
    os.makedirs(cleaned_dir, exist_ok=True)

    out_path = os.path.join(cleaned_dir, f"{table_name}_cleaned.csv")
    cleaned_df.to_csv(out_path, index=False)

    print(f"[LOAD] Saved cleaned CSV → {out_path}")


def save_rejected_csv(rejected_df: pd.DataFrame, table_name: str) -> None:
  
    rejected_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "rejected")
    os.makedirs(rejected_dir, exist_ok=True)

    if rejected_df.empty:
        print(f"[LOAD] No rejected rows for {table_name} — nothing to save.")
        return

    cols_to_drop = [c for c in rejected_df.columns if c.startswith("_")]
    rejected_df = rejected_df.drop(columns=cols_to_drop, errors="ignore")

    out_path = os.path.join(rejected_dir, f"{table_name}_rejected.csv")
    rejected_df.to_csv(out_path, index=False)
    
    print(f"[LOAD] Saved rejected CSV → {out_path}")