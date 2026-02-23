import os
import sqlite3
import pandas as pd

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "database", "retail.db")


def _get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def calculate_loyalty(db_path: str = DB_PATH) -> pd.DataFrame:
    """
    Calculate loyalty points for every transaction and update customer records.

    Returns
    -------
    pd.DataFrame  — points earned per transaction (for auditing).
    """
    conn = _get_connection(db_path)

    # Read required tables
    transactions = pd.read_sql("SELECT * FROM store_sales_header", conn)
    rules = pd.read_sql("SELECT * FROM loyalty_rules", conn)

    if transactions.empty or rules.empty:
        print("[LOYALTY] No transactions or loyalty rules found — skipping.")
        conn.close()
        return pd.DataFrame()

    # Convert dates
    transactions["transaction_date"] = pd.to_datetime(
        transactions["transaction_date"], errors="coerce"
    )
    rules["start_date"] = pd.to_datetime(rules["start_date"], errors="coerce")
    rules["end_date"] = pd.to_datetime(rules["end_date"], errors="coerce")

    results = []

    for _, txn in transactions.iterrows():
        txn_date = txn["transaction_date"]
        total_amt = txn["total_amount"]
        cust_id = txn["customer_id"]

        # Find applicable rule (transaction date within rule period)
        applicable = rules[
            (rules["start_date"] <= txn_date) & (rules["end_date"] >= txn_date)
        ]

        if applicable.empty:
            # Fall back to the first rule if none match date window
            applicable = rules.head(1)

        rule = applicable.iloc[0]
        points = total_amt * rule["points_per_unit_spend"]

        if total_amt > rule["min_spend_threshold"]:
            points += rule["bonus_points"]

        results.append(
            {
                "transaction_id": txn["transaction_id"],
                "customer_id": cust_id,
                "total_amount": total_amt,
                "points_earned": round(points, 2),
            }
        )

    points_df = pd.DataFrame(results)

    # Aggregate points per customer
    customer_points = (
        points_df.groupby("customer_id")["points_earned"].sum().reset_index()
    )
    customer_points.rename(
        columns={"points_earned": "total_loyalty_points"}, inplace=True
    )

    # Update customer_details in DB
    cursor = conn.cursor()
    for _, row in customer_points.iterrows():
        cursor.execute(
            """
            UPDATE customer_details
            SET total_loyalty_points = ?
            WHERE customer_id = ?
            """,
            (round(row["total_loyalty_points"], 2), row["customer_id"]),
        )

    # Also update loyalty_status based on points thresholds
    cursor.execute("""
        UPDATE customer_details SET loyalty_status = 
            CASE
                WHEN total_loyalty_points >= 1000 THEN 'Gold'
                WHEN total_loyalty_points >= 500  THEN 'Silver'
                ELSE 'Bronze'
            END
    """)

    conn.commit()
    conn.close()

    print(f"[LOYALTY] Calculated points for {len(points_df)} transactions, "
          f"updated {len(customer_points)} customers.")
    return points_df

"""
analytics/loyalty.py
---------------------
Loyalty-points calculation engine.

For each transaction:
  points = total_amount * points_per_unit_spend
  if total_amount > min_spend_threshold  →  add bonus_points
Update total_loyalty_points in customer_details.
"""