"""
database/setup.py
-----------------
Creates the SQLite retail database and all required tables.
"""

import sqlite3
import os


DB_PATH = os.path.join(os.path.dirname(__file__), "retail.db")


def get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    """Return a connection to the retail SQLite database."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def setup_database(db_path: str = DB_PATH) -> None:
    """Create all tables if they do not already exist."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.executescript("""
    -- Stores
    CREATE TABLE IF NOT EXISTS stores (
        store_id        INTEGER PRIMARY KEY,
        store_name      TEXT NOT NULL,
        store_city      TEXT,
        store_region    TEXT,
        opening_date    TEXT
    );

    -- Products
    CREATE TABLE IF NOT EXISTS products (
        product_id          TEXT PRIMARY KEY,
        product_name        TEXT NOT NULL,
        product_category    TEXT,
        unit_price          REAL,
        current_stock_level INTEGER DEFAULT 0,
        restock_flag        INTEGER DEFAULT 0
    );

    -- Customer Details
    CREATE TABLE IF NOT EXISTS customer_details (
        customer_id             TEXT PRIMARY KEY,
        first_name              TEXT,
        email                   TEXT,
        loyalty_status          TEXT DEFAULT 'Bronze',
        total_loyalty_points    REAL DEFAULT 0,
        last_purchase_date      TEXT,
        segment_id              TEXT,
        customer_phone          TEXT,
        customer_since          TEXT,
        promotion_sensitivity   TEXT DEFAULT 'LOW'
    );

    -- Promotion Details
    CREATE TABLE IF NOT EXISTS promotion_details (
        promotion_id        INTEGER PRIMARY KEY,
        promotion_name      TEXT,
        start_date          TEXT,
        end_date            TEXT,
        discount_percentage REAL,
        applicable_category TEXT
    );

    -- Loyalty Rules
    CREATE TABLE IF NOT EXISTS loyalty_rules (
        rule_id             INTEGER PRIMARY KEY,
        rule_name           TEXT,
        points_per_unit_spend REAL,
        min_spend_threshold   REAL,
        bonus_points          REAL,
        start_date            TEXT,
        end_date              TEXT
    );

    -- Store Sales Header
    CREATE TABLE IF NOT EXISTS store_sales_header (
        transaction_id   INTEGER PRIMARY KEY,
        customer_id      TEXT,
        store_id         INTEGER,
        transaction_date TEXT,
        total_amount     REAL,
        FOREIGN KEY (customer_id) REFERENCES customer_details(customer_id),
        FOREIGN KEY (store_id)    REFERENCES stores(store_id)
    );

    -- Store Sales Line Items
    CREATE TABLE IF NOT EXISTS store_sales_line_items (
        line_item_id     INTEGER PRIMARY KEY,
        transaction_id   INTEGER,
        product_id       TEXT,
        promotion_id     INTEGER,
        quantity          INTEGER,
        line_item_amount  REAL,
        FOREIGN KEY (transaction_id) REFERENCES store_sales_header(transaction_id),
        FOREIGN KEY (product_id)     REFERENCES products(product_id),
        FOREIGN KEY (promotion_id)   REFERENCES promotion_details(promotion_id)
    );

    -- Reject tables (for ETL rejects)
    CREATE TABLE IF NOT EXISTS rejected_stores        (store_id INTEGER, store_name TEXT, store_city TEXT, store_region TEXT, opening_date TEXT, reject_reason TEXT);
    CREATE TABLE IF NOT EXISTS rejected_products      (product_id TEXT, product_name TEXT, product_category TEXT, unit_price REAL, current_stock_level INTEGER, restock_flag INTEGER, reject_reason TEXT);
    CREATE TABLE IF NOT EXISTS rejected_customer_details (customer_id TEXT, first_name TEXT, email TEXT, loyalty_status TEXT, total_loyalty_points REAL, last_purchase_date TEXT, segment_id TEXT, customer_phone TEXT, customer_since TEXT, promotion_sensitivity TEXT, reject_reason TEXT);
    CREATE TABLE IF NOT EXISTS rejected_promotion_details (promotion_id INTEGER, promotion_name TEXT, start_date TEXT, end_date TEXT, discount_percentage REAL, applicable_category TEXT, reject_reason TEXT);
    CREATE TABLE IF NOT EXISTS rejected_loyalty_rules (rule_id INTEGER, rule_name TEXT, points_per_unit_spend REAL, min_spend_threshold REAL, bonus_points REAL, start_date TEXT, end_date TEXT, reject_reason TEXT);
    CREATE TABLE IF NOT EXISTS rejected_store_sales_header (transaction_id INTEGER, customer_id TEXT, store_id INTEGER, transaction_date TEXT, total_amount REAL, reject_reason TEXT);
    CREATE TABLE IF NOT EXISTS rejected_store_sales_line_items (line_item_id INTEGER, transaction_id INTEGER, product_id TEXT, promotion_id INTEGER, quantity INTEGER, line_item_amount REAL, reject_reason TEXT);
    """)

    conn.commit()
    conn.close()
    print("[DB] Database setup complete — all tables created.")


if __name__ == "__main__":
    setup_database()
