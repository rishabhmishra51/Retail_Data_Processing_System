# 
from etl.load import data_loader
from etl import ingest
from etl.ingest import ingest_all
from etl.validate import validate
from etl.load import load_table, load_rejects, save_cleaned_csv, save_rejected_csv
from analytics.loyalty import calculate_loyalty

def step_run_etl():
    """Step 2: Ingest raw files → Validate → Load into SQLite."""
    print("\n" + "=" * 60)
    print("STEP 2: ETL PIPELINE")
    print("=" * 60)

    raw_data = ingest_all()

    if not raw_data:
        print("[ETL] No supported files found in data/raw/ — skipping ETL.")
        return

    for table_name, raw_df in raw_data.items():
        cleaned_df, rejected_df = validate(raw_df, table_name)
        load_table(cleaned_df, table_name)
        load_rejects(rejected_df, table_name)
        save_cleaned_csv(cleaned_df, table_name)
        save_rejected_csv(rejected_df, table_name)

    print("[ETL] Pipeline complete.")

    def step_calculate_loyalty():
     """ Step 3: Loyalty points calculation ."""
    print("\n" + "=" * 60)
    print("STEP 3: LOYALTY CALCULATION")
    print("=" * 60)
    points_df = calculate_loyalty()
    if not points_df.empty:
        print(points_df.head(10).to_string(index=False))

def main():
    path="data//raw//Store sales.xlsx"
    df=data_loader(path)
  

if __name__=="__main__":
    main()

