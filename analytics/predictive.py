

import os
import sqlite3
from datetime import datetime

import numpy as np
import pandas as pd

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "database", "retail.db")


def _get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn




def predict_future_spend(db_path: str = DB_PATH) -> pd.DataFrame:
    
    conn = _get_connection(db_path)
    txn = pd.read_sql("SELECT * FROM store_sales_header", conn)
    conn.close()

    if txn.empty:
        print("[PREDICT] No transactions — cannot predict spend.")
        return pd.DataFrame()

    txn["transaction_date"] = pd.to_datetime(txn["transaction_date"], errors="coerce")
    txn["year_month"] = txn["transaction_date"].dt.to_period("M")

    monthly = (
        txn.groupby(["customer_id", "year_month"])["total_amount"]
        .sum()
        .reset_index()
    )

    # Last 3 months per customer
    monthly = monthly.sort_values(["customer_id", "year_month"])
    last3 = monthly.groupby("customer_id").tail(3)

    future = (
        last3.groupby("customer_id")["total_amount"]
        .mean()
        .reset_index()
        .rename(columns={"total_amount": "predicted_next_month_spend"})
    )
    future["predicted_next_month_spend"] = future["predicted_next_month_spend"].round(2)

    print(f"[PREDICT] Future spend estimated for {len(future)} customers.")
    return future


def stock_out_risk(db_path: str = DB_PATH) -> pd.DataFrame:
    
    conn = _get_connection(db_path)
    line_items = pd.read_sql("SELECT * FROM store_sales_line_items", conn)
    products = pd.read_sql("SELECT * FROM products", conn)
    txn = pd.read_sql("SELECT transaction_id, transaction_date FROM store_sales_header", conn)

    if line_items.empty or products.empty:
        print("[PREDICT] Insufficient data for stock-out risk.")
        conn.close()
        return pd.DataFrame()

    # Merge to get transaction dates
    merged = line_items.merge(txn, on="transaction_id", how="left")
    merged["transaction_date"] = pd.to_datetime(merged["transaction_date"], errors="coerce")

    # Calculate date range for avg daily sales
    date_range = (merged["transaction_date"].max() - merged["transaction_date"].min()).days
    if date_range <= 0:
        date_range = 1

    daily_sales = (
        merged.groupby("product_id")["quantity"]
        .sum()
        .reset_index()
        .rename(columns={"quantity": "total_qty_sold"})
    )
    daily_sales["avg_daily_sales"] = (daily_sales["total_qty_sold"] / date_range).round(2)

    risk = daily_sales.merge(
        products[["product_id", "product_name", "current_stock_level"]],
        on="product_id",
        how="left",
    )
    risk["weekly_demand"] = (risk["avg_daily_sales"] * 7).round(2)
    risk["stock_out_risk"] = risk["weekly_demand"] > risk["current_stock_level"]

    # Update restock_flag in products table
    at_risk_ids = risk.loc[risk["stock_out_risk"], "product_id"].tolist()
    if at_risk_ids:
        placeholders = ",".join("?" * len(at_risk_ids))
        cursor = conn.cursor()
        cursor.execute(
            f"UPDATE products SET restock_flag = 1 WHERE product_id IN ({placeholders})",
            at_risk_ids,
        )
        conn.commit()

    conn.close()
    print(f"[PREDICT] Stock-out risk: {risk['stock_out_risk'].sum()} products flagged.")
    return risk


def promotion_sensitivity(db_path: str = DB_PATH) -> pd.DataFrame:
    
    conn = _get_connection(db_path)
    line_items = pd.read_sql("SELECT * FROM store_sales_line_items", conn)
    txn = pd.read_sql("SELECT transaction_id, customer_id FROM store_sales_header", conn)

    if line_items.empty:
        print("[PREDICT] No line items — cannot compute promo sensitivity.")
        conn.close()
        return pd.DataFrame()

    merged = line_items.merge(txn, on="transaction_id", how="left")

    total_per_cust = merged.groupby("customer_id")["line_item_id"].count().reset_index()
    total_per_cust.rename(columns={"line_item_id": "total_purchases"}, inplace=True)

    promo_mask = merged["promotion_id"].notna() & (merged["promotion_id"] != 0)
    promo_per_cust = (
        merged.loc[promo_mask]
        .groupby("customer_id")["line_item_id"]
        .count()
        .reset_index()
    )
    promo_per_cust.rename(columns={"line_item_id": "promo_purchases"}, inplace=True)

    result = total_per_cust.merge(promo_per_cust, on="customer_id", how="left")
    result["promo_purchases"] = result["promo_purchases"].fillna(0).astype(int)
    result["response_rate"] = (
        result["promo_purchases"] / result["total_purchases"] * 100
    ).round(2)

    def _classify(rate):
        if rate > 50:
            return "HIGH"
        elif rate >= 20:
            return "MEDIUM"
        return "LOW"

    result["promotion_sensitivity"] = result["response_rate"].apply(_classify)

    # Update customer_details
    cursor = conn.cursor()
    for _, row in result.iterrows():
        cursor.execute(
            "UPDATE customer_details SET promotion_sensitivity = ? WHERE customer_id = ?",
            (row["promotion_sensitivity"], row["customer_id"]),
        )
    conn.commit()
    conn.close()

    print(f"[PREDICT] Promotion sensitivity calculated for {len(result)} customers.")
    print(result["promotion_sensitivity"].value_counts().to_string())
    return result


def run_predictive(db_path: str = DB_PATH) -> dict:
    
    spend = predict_future_spend(db_path)
    stock = stock_out_risk(db_path)
    promo = promotion_sensitivity(db_path)
    return {"future_spend": spend, "stock_out_risk": stock, "promo_sensitivity": promo}
