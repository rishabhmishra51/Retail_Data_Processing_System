import os
import sys


PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from database.setup import setup_database, DB_PATH
from etl.ingest import ingest_all
from etl.validate import validate
from etl.load import load_table, load_rejects, save_cleaned_csv, save_rejected_csv
from analytics.loyalty import calculate_loyalty
from analytics.segmentation import perform_segmentation
from analytics.predictive import run_predictive
from dashboard.dashboard import launch_dashboard, generate_dashboard, list_stores


def step_setup_database():
    """Step 1: Create database and tables."""
    print("\n" + "=" * 60)
    print("STEP 1: DATABASE SETUP")
    print("=" * 60)
    setup_database()


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
    """Step 3: Loyalty points calculation."""
    print("\n" + "=" * 60)
    print("STEP 3: LOYALTY CALCULATION")
    print("=" * 60)
    points_df = calculate_loyalty()
    if not points_df.empty:
        print(points_df.head(10).to_string(index=False))


def step_perform_segmentation():
    """Step 4: RFM segmentation."""
    print("\n" + "=" * 60)
    print("STEP 4: RFM SEGMENTATION")
    print("=" * 60)
    rfm = perform_segmentation()
    if not rfm.empty:
        print(rfm.head(10).to_string(index=False))


def step_run_predictive():
    """Step 5: Predictive analytics."""
    print("\n" + "=" * 60)
    print("STEP 5: PREDICTIVE ANALYTICS")
    print("=" * 60)
    results = run_predictive()
    for key, df in results.items():
        if not df.empty:
            print(f"\n--- {key} (top 5) ---")
            print(df.head(5).to_string(index=False))


def step_launch_dashboard():
    """Step 6: Generate dashboards for all stores (non-interactive)."""
    print("\n" + "=" * 60)
    print("STEP 6: DASHBOARD GENERATION")
    print("=" * 60)
    stores = list_stores()
    if stores.empty:
        print("[DASHBOARD] No stores — skipping.")
        return
    print(stores.to_string(index=False))
    for sid in stores["store_id"]:
        generate_dashboard(sid)
    print("[DASHBOARD] All store dashboards generated.")




def main():
    """Execute the full pipeline end-to-end."""
    print("╔══════════════════════════════════════════════════════════╗")
    print("║   Retail Analytics & Customer Intelligence System       ║")
    print("╚══════════════════════════════════════════════════════════╝")

    step_setup_database()
    step_run_etl()
    step_calculate_loyalty()
    step_perform_segmentation()
    step_run_predictive()
    step_launch_dashboard()

    print("\n  Pipeline finished successfully.")


if __name__ == "__main__":
    main()
