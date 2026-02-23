

import os
import sqlite3

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "database", "retail.db")
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

# Database helpers


def _get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def list_stores(db_path: str = DB_PATH) -> pd.DataFrame:
    
    conn = _get_connection(db_path)
    stores = pd.read_sql("SELECT store_id, store_name, store_city FROM stores", conn)
    conn.close()
    return stores


def fetch_sales(store_id: int, db_path: str = DB_PATH) -> pd.DataFrame:
    conn = _get_connection(db_path)
    df = pd.read_sql(
        """
        SELECT h.transaction_date, h.total_amount
        FROM store_sales_header h
        WHERE h.store_id = ?
        ORDER BY h.transaction_date
        """,
        conn,
        params=(store_id,),
    )
    conn.close()
    return df


def fetch_top_products(store_id: int, db_path: str = DB_PATH) -> pd.DataFrame:
    conn = _get_connection(db_path)
    df = pd.read_sql(
        """
        SELECT p.product_name, SUM(li.quantity) AS total_qty
        FROM store_sales_line_items li
        JOIN store_sales_header h ON li.transaction_id = h.transaction_id
        JOIN products p            ON li.product_id    = p.product_id
        WHERE h.store_id = ?
        GROUP BY p.product_name
        ORDER BY total_qty DESC
        LIMIT 10
        """,
        conn,
        params=(store_id,),
    )
    conn.close()
    return df


def fetch_loyalty(store_id: int, db_path: str = DB_PATH) -> pd.DataFrame:
    conn = _get_connection(db_path)
    df = pd.read_sql(
        """
        SELECT c.total_loyalty_points
        FROM customer_details c
        JOIN store_sales_header h ON c.customer_id = h.customer_id
        WHERE h.store_id = ?
        GROUP BY c.customer_id
        """,
        conn,
        params=(store_id,),
    )
    conn.close()
    return df


def fetch_store_name(store_id: int, db_path: str = DB_PATH) -> str:
    conn = _get_connection(db_path)
    row = pd.read_sql(
        "SELECT store_name FROM stores WHERE store_id = ?", conn, params=(store_id,)
    )
    conn.close()
    return row.iloc[0]["store_name"] if not row.empty else f"Store {store_id}"



# Chart builders  (return matplotlib Figure objects)


def chart_sales_trend(sales: pd.DataFrame, store_label: str):
    fig, ax = plt.subplots(figsize=(7, 4.5))
    if not sales.empty:
        sales = sales.copy()
        sales["transaction_date"] = pd.to_datetime(sales["transaction_date"], errors="coerce")
        trend = sales.groupby("transaction_date")["total_amount"].sum().sort_index()
        ax.plot(trend.index, trend.values, marker="o", linewidth=1.5, color="#1f77b4")
        ax.fill_between(trend.index, trend.values, alpha=0.15, color="#1f77b4")
        ax.tick_params(axis="x", rotation=45)
    ax.set_title("Sales Trend", fontsize=13, fontweight="bold")
    ax.set_xlabel("Date")
    ax.set_ylabel("Total Sales (\u20b9)")
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    fig.tight_layout()
    return fig


def chart_top_products(top_products: pd.DataFrame, store_label: str):
    fig, ax = plt.subplots(figsize=(7, 4.5))
    if not top_products.empty:
        ax.barh(top_products["product_name"], top_products["total_qty"], color="#2ca02c", edgecolor="white")
        ax.invert_yaxis()
        for i, v in enumerate(top_products["total_qty"]):
            ax.text(v + 0.5, i, str(int(v)), va="center", fontsize=9)
    ax.set_title("Top 10 Selling Products", fontsize=13, fontweight="bold")
    ax.set_xlabel("Quantity Sold")
    ax.grid(axis="x", linestyle="--", alpha=0.4)
    fig.tight_layout()
    return fig


def chart_loyalty_distribution(loyalty: pd.DataFrame, store_label: str):
    fig, ax = plt.subplots(figsize=(7, 4.5))
    if not loyalty.empty:
        ax.hist(loyalty["total_loyalty_points"].dropna(), bins=15, color="#ff7f0e", edgecolor="black")
    ax.set_title("Loyalty Points Distribution", fontsize=13, fontweight="bold")
    ax.set_xlabel("Total Loyalty Points")
    ax.set_ylabel("Number of Customers")
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    fig.tight_layout()
    return fig



# generate_dashboard  — used by main.py pipeline (saves PNG)


def generate_dashboard(store_id: int, db_path: str = DB_PATH, output_dir: str = OUTPUT_DIR) -> str:
    """Build and save a 3-panel dashboard PNG for the given store."""
    sales = fetch_sales(store_id, db_path)
    top_products = fetch_top_products(store_id, db_path)
    loyalty = fetch_loyalty(store_id, db_path)
    store_label = fetch_store_name(store_id, db_path)

    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    fig.suptitle(f"Store-Wise Retail Analytics Dashboard \u2014 {store_label}", fontsize=14, fontweight="bold")

    # 1. Sales Trend
    ax1 = axes[0]
    if not sales.empty:
        s = sales.copy()
        s["transaction_date"] = pd.to_datetime(s["transaction_date"], errors="coerce")
        trend = s.groupby("transaction_date")["total_amount"].sum().sort_index()
        ax1.plot(trend.index, trend.values, marker="o", linewidth=1.5, color="#1f77b4")
        ax1.tick_params(axis="x", rotation=45)
    ax1.set_title("Sales Trend")
    ax1.set_xlabel("Date")
    ax1.set_ylabel("Total Sales (\u20b9)")

    # 2. Top 10 Products
    ax2 = axes[1]
    if not top_products.empty:
        ax2.barh(top_products["product_name"], top_products["total_qty"], color="#2ca02c")
        ax2.invert_yaxis()
    ax2.set_title("Top 10 Selling Products")
    ax2.set_xlabel("Quantity Sold")

    # 3. Loyalty Distribution
    ax3 = axes[2]
    if not loyalty.empty:
        ax3.hist(loyalty["total_loyalty_points"].dropna(), bins=15, color="#ff7f0e", edgecolor="black")
    ax3.set_title("Loyalty Points Distribution")
    ax3.set_xlabel("Total Loyalty Points")
    ax3.set_ylabel("Number of Customers")

    plt.tight_layout(rect=[0, 0, 1, 0.93])

    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, f"dashboard_store_{store_id}.png")
    fig.savefig(out_path, dpi=150)
    plt.close(fig)

    print(f"[DASHBOARD] Saved \u2192 {out_path}")
    return out_path



# launch_dashboard  — called by main.py (non-interactive batch mode)


def launch_dashboard(db_path: str = DB_PATH) -> None:
    """Generate dashboards for all stores (batch / pipeline mode)."""
    stores = list_stores(db_path)
    if stores.empty:
        print("[DASHBOARD] No stores found in database.")
        return
    print(stores.to_string(index=False))
    for sid in stores["store_id"]:
        generate_dashboard(sid, db_path)
    print("[DASHBOARD] All store dashboards generated.")



# Streamlit UI  (only runs when executed via `streamlit run`)


def _run_streamlit_app():
    import streamlit as st

    st.set_page_config(
        page_title="Retail Analytics Dashboard",
        page_icon="\U0001f3ea",
        layout="wide",
    )

    st.markdown(
        "<h1 style='text-align:center;'>\U0001f3ea Store-Wise Retail Analytics Dashboard</h1>",
        unsafe_allow_html=True,
    )
    st.markdown("---")

    #  Sidebar: store selector 
    stores = list_stores()
    if stores.empty:
        st.warning("No stores found in the database. Run the ETL pipeline first.")
        return

    store_options = {
        f"{row.store_name}  ({row.store_city})": row.store_id
        for row in stores.itertuples()
    }

    selected_label = st.selectbox(
        "\U0001f50d Select a Store",
        options=list(store_options.keys()),
    )
    selected_id = store_options[selected_label]
    store_label = fetch_store_name(selected_id)

    st.markdown(f"### Showing analytics for **{store_label}**")

    # Fetch data
    sales = fetch_sales(selected_id)
    top_products = fetch_top_products(selected_id)
    loyalty = fetch_loyalty(selected_id)

    # KPI row
    k1, k2, k3 = st.columns(3)
    total_sales = sales["total_amount"].sum() if not sales.empty else 0
    num_txn = len(sales) if not sales.empty else 0
    avg_loyalty = loyalty["total_loyalty_points"].mean() if not loyalty.empty else 0
    k1.metric("Total Sales (\u20b9)", f"\u20b9{total_sales:,.2f}")
    k2.metric("Transactions", f"{num_txn:,}")
    k3.metric("Avg Loyalty Points", f"{avg_loyalty:,.0f}")

    st.markdown("---")

    #  Charts in 3 columns 
    col1, col2, col3 = st.columns(3)

    with col1:
        fig1 = chart_sales_trend(sales, store_label)
        st.pyplot(fig1)
        plt.close(fig1)

    with col2:
        fig2 = chart_top_products(top_products, store_label)
        st.pyplot(fig2)
        plt.close(fig2)

    with col3:
        fig3 = chart_loyalty_distribution(loyalty, store_label)
        st.pyplot(fig3)
        plt.close(fig3)

    st.markdown("---")
    st.caption("Retail Analytics & Customer Intelligence System  \u2022  Powered by SQLite + pandas + matplotlib")



# Entry point


def _is_streamlit_running() -> bool:
    """Check if we are being executed by the Streamlit runtime."""
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx
        return get_script_run_ctx() is not None
    except (ImportError, ModuleNotFoundError):
        return False


if _is_streamlit_running():
    _run_streamlit_app()
elif __name__ == "__main__":
    launch_dashboard()
