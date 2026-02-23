import os
import sqlite3
import pandas as pd
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "database", "retail.db")


def _get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def perform_segmentation(db_path: str = DB_PATH) -> pd.DataFrame:
    """
    Perform RFM (Recency, Frequency, Monetary) segmentation on customers.

    Returns
    -------
    pd.DataFrame  — RFM scores and segments per customer.
    """
    conn = _get_connection(db_path)

    # Read transactions
    txn = pd.read_sql("SELECT * FROM store_sales_header", conn)
    if txn.empty:
        print("[SEGMENTATION] No transactions found — skipping RFM.")
        conn.close()
        return pd.DataFrame()

    txn["transaction_date"] = pd.to_datetime(txn["transaction_date"], errors="coerce")
    txn = txn.dropna(subset=["transaction_date"])

    # Calculate RFM
    now = txn["transaction_date"].max()

    rfm = txn.groupby("customer_id").agg(
        recency=("transaction_date", lambda x: (now - x.max()).days),
        frequency=("transaction_id", "count"),
        monetary=("total_amount", "sum"),
    ).reset_index()

    # Score each component (1-5 scale, 5 being best)
    try:
        rfm["R_score"] = pd.qcut(rfm["recency"], 5, labels=[5, 4, 3, 2, 1], duplicates="drop")
    except ValueError:
        rfm["R_score"] = pd.cut(rfm["recency"], 5, labels=[5, 4, 3, 2, 1])
    try:
        rfm["F_score"] = pd.qcut(rfm["frequency"], 5, labels=[1, 2, 3, 4, 5], duplicates="drop")
    except ValueError:
        rfm["F_score"] = pd.cut(rfm["frequency"], 5, labels=[1, 2, 3, 4, 5])
    try:
        rfm["M_score"] = pd.qcut(rfm["monetary"], 5, labels=[1, 2, 3, 4, 5], duplicates="drop")
    except ValueError:
        rfm["M_score"] = pd.cut(rfm["monetary"], 5, labels=[1, 2, 3, 4, 5])

    # Combine scores
    rfm["RFM_score"] = rfm["R_score"].astype(str) + rfm["F_score"].astype(str) + rfm["M_score"].astype(str)

    # Segment based on RFM score
    def _segment(score):
        r, f, m = int(score[0]), int(score[1]), int(score[2])
        if r >= 4 and f >= 4 and m >= 4:
            return "Champions"
        elif r >= 3 and f >= 3 and m >= 3:
            return "Loyal Customers"
        elif r >= 3 and f >= 1 and m >= 1:
            return "Potential Loyalists"
        elif r >= 2 and f >= 2 and m >= 2:
            return "At Risk"
        elif r >= 1 and f >= 1 and m >= 1:
            return "Lost"
        else:
            return "New Customers"

    rfm["segment"] = rfm["RFM_score"].apply(_segment)

    # Update customer_details
    cursor = conn.cursor()
    for _, row in rfm.iterrows():
        cursor.execute(
            "UPDATE customer_details SET segment_id = ? WHERE customer_id = ?",
            (row["segment"], row["customer_id"]),
        )

    conn.commit()
    conn.close()

    print(f"[SEGMENTATION] RFM calculated for {len(rfm)} customers.")
    print(rfm["segment"].value_counts().to_string())
    return rfm