import pandas as pd
import numpy as np
import re


AMOUNT_COLUMNS = {
    "unit_price", "total_amount", "line_item_amount",
    "discount_percentage", "points_per_unit_spend",
    "min_spend_threshold", "bonus_points", "total_loyalty_points",
}


NUMERIC_COLUMNS = {
    "store_id", "promotion_id",
    "rule_id", "transaction_id", "line_item_id",
    "quantity", "current_stock_level", "restock_flag",
    "unit_price", "total_amount", "line_item_amount",
    "discount_percentage", "points_per_unit_spend",
    "min_spend_threshold", "bonus_points", "total_loyalty_points",
}


NULLABLE_COLUMNS = {
    "last_purchase_date", "segment_id", "promotion_sensitivity",
    "loyalty_status", "total_loyalty_points", "restock_flag",
    "promotion_id", "customer_phone",
}


def _clean_amount(value):
    #strip dollar sign from total_amount
    if pd.isna(value):
        return np.nan
    s = str(value).strip()
    s = re.sub(r"[^\d.\-]", "", s)  # keep digits, dot, minus
    try:
        return float(s) if s else np.nan
    except ValueError:
        return np.nan


def validate(df: pd.DataFrame, table_name: str = "") -> tuple:
    
    if df.empty:
        return df.copy(), pd.DataFrame()

    original_len = len(df)
    working = df.copy()

    # --- 1. Track reject reasons ---
    working["_reject_reason"] = ""

    # --- 2. Clean amount / numeric columns ---
    cols_present_amount = [c for c in working.columns if c in AMOUNT_COLUMNS]
    for col in cols_present_amount:
        working[col] = working[col].apply(_clean_amount)

    cols_present_numeric = [c for c in working.columns if c in NUMERIC_COLUMNS]
    for col in cols_present_numeric:
        working[col] = pd.to_numeric(working[col], errors="coerce")

    # --- 3. Flag rows with ALL null values (across original columns, not _reject) ---
    orig_cols = [c for c in working.columns if c != "_reject_reason"]
    all_null_mask = working[orig_cols].isna().all(axis=1)
    working.loc[all_null_mask, "_reject_reason"] = "all_null"

    # --- 4. Flag rows with any null primary (non-nullable) fields ---
    required_cols = [c for c in orig_cols if c not in NULLABLE_COLUMNS]
    if required_cols:
        null_any_mask = working[required_cols].isna().any(axis=1) & ~all_null_mask
    else:
        null_any_mask = pd.Series(False, index=working.index)
    working.loc[null_any_mask, "_reject_reason"] = working.loc[
        null_any_mask, "_reject_reason"
    ].apply(lambda r: r + ";has_null" if r else "has_null")

    # --- 5. Flag negative numeric values ---
    for col in cols_present_numeric:
        neg_mask = working[col] < 0
        working.loc[neg_mask, "_reject_reason"] = working.loc[
            neg_mask, "_reject_reason"
        ].apply(lambda r: r + f";negative_{col}" if r else f"negative_{col}")

    # --- 6. Split into clean vs rejected ---
    rejected_mask = working["_reject_reason"].astype(bool)
    rejected_df = working.loc[rejected_mask].copy()
    rejected_df.rename(columns={"_reject_reason": "reject_reason"}, inplace=True)

    cleaned_df = working.loc[~rejected_mask].drop(columns=["_reject_reason"])

    # Remove duplicate primary-key rows (keep first)
    cleaned_df = cleaned_df.drop_duplicates()

    print(
        f"[VALIDATE] {table_name}: {original_len} raw → "
        f"{len(cleaned_df)} clean, {len(rejected_df)} rejected"
    )
    return cleaned_df, rejected_df