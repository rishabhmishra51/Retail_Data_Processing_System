from etl.ingest import ingest_all
from etl.validate import validate
from etl.load import load_table, load_rejects, save_cleaned_csv, save_rejected_csv
from analytics.predictive import run_predictive

def step_run_etl():
    # run etl
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

def step_run_predictive():
    #run predictive analytics
    print("\n" + "=" * 60)
    print("STEP 5: PREDICTIVE ANALYTICS")
    print("=" * 60)
    results = run_predictive()
    for key, df in results.items():
        if not df.empty:
            print(f"\n--- {key} (top 5) ---")
            print(df.head(5).to_string(index=False))