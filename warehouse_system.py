"""
Warehouse Data Management System
==================================
A complete warehouse management system with:
- Fully normalised PostgreSQL schema (5 tables)
- Automated inventory tracking with reorder triggers
- Purchase order management
- Stock movement logging
- Management reporting queries

Tables:
    suppliers     - 30 Ghana suppliers
    products      - 200 warehouse products
    inventory     - current stock levels + reorder logic
    stock_movements - all in/out stock transactions
    purchase_orders - supplier purchase orders

Author: Lawrence Koomson
GitHub: github.com/lawrykoomson
"""

import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("warehouse.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "warehouse_db"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

PROCESSED_PATH = Path("data/processed/")

CATEGORIES = ["Electronics","Groceries","Clothing","Hardware","Pharmaceuticals","Automotive"]
REGIONS    = ["Greater Accra","Ashanti","Western","Eastern","Northern"]
MOVEMENT_TYPES = ["INBOUND","OUTBOUND","ADJUSTMENT","RETURN","DAMAGE"]


def create_schema(conn):
    """Create all warehouse tables in PostgreSQL."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS warehouse_dw;

            CREATE TABLE IF NOT EXISTS warehouse_dw.suppliers (
                supplier_id     VARCHAR(10) PRIMARY KEY,
                supplier_name   VARCHAR(100) NOT NULL,
                contact_person  VARCHAR(80),
                phone           VARCHAR(20),
                region          VARCHAR(50),
                category        VARCHAR(30),
                rating          NUMERIC(3,1),
                is_active       BOOLEAN DEFAULT TRUE,
                created_at      TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS warehouse_dw.products (
                product_id      VARCHAR(10) PRIMARY KEY,
                product_name    VARCHAR(100) NOT NULL,
                category        VARCHAR(30),
                supplier_id     VARCHAR(10) REFERENCES warehouse_dw.suppliers(supplier_id),
                unit_cost_ghs   NUMERIC(12,2),
                unit_price_ghs  NUMERIC(12,2),
                unit_of_measure VARCHAR(20),
                reorder_point   INT,
                reorder_qty     INT,
                is_active       BOOLEAN DEFAULT TRUE,
                created_at      TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS warehouse_dw.inventory (
                inventory_id        SERIAL PRIMARY KEY,
                product_id          VARCHAR(10) REFERENCES warehouse_dw.products(product_id),
                quantity_on_hand    INT DEFAULT 0,
                quantity_reserved   INT DEFAULT 0,
                quantity_available  INT GENERATED ALWAYS AS (quantity_on_hand - quantity_reserved) STORED,
                warehouse_location  VARCHAR(20),
                last_counted_date   DATE,
                needs_reorder       BOOLEAN DEFAULT FALSE,
                updated_at          TIMESTAMP DEFAULT NOW(),
                UNIQUE(product_id)
            );

            CREATE TABLE IF NOT EXISTS warehouse_dw.stock_movements (
                movement_id     SERIAL PRIMARY KEY,
                product_id      VARCHAR(10) REFERENCES warehouse_dw.products(product_id),
                movement_type   VARCHAR(15),
                quantity        INT,
                unit_cost_ghs   NUMERIC(12,2),
                total_value_ghs NUMERIC(14,2),
                reference_no    VARCHAR(20),
                notes           TEXT,
                movement_date   TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS warehouse_dw.purchase_orders (
                po_id           VARCHAR(15) PRIMARY KEY,
                supplier_id     VARCHAR(10) REFERENCES warehouse_dw.suppliers(supplier_id),
                product_id      VARCHAR(10) REFERENCES warehouse_dw.products(product_id),
                quantity_ordered INT,
                unit_cost_ghs   NUMERIC(12,2),
                total_cost_ghs  NUMERIC(14,2),
                status          VARCHAR(15) DEFAULT 'PENDING',
                order_date      DATE,
                expected_date   DATE,
                received_date   DATE,
                created_at      TIMESTAMP DEFAULT NOW()
            );
        """)
        conn.commit()
    logger.info("[SCHEMA] All warehouse tables created successfully.")


def generate_suppliers(n=30) -> pd.DataFrame:
    np.random.seed(42)
    return pd.DataFrame({
        "supplier_id":    [f"SUP{str(i).zfill(4)}" for i in range(1, n+1)],
        "supplier_name":  [f"Ghana Supplier {i} Ltd" for i in range(1, n+1)],
        "contact_person": [f"Contact Person {i}" for i in range(1, n+1)],
        "phone":          [f"024{np.random.randint(1000000,9999999)}" for _ in range(n)],
        "region":         np.random.choice(REGIONS, n),
        "category":       np.random.choice(CATEGORIES, n),
        "rating":         np.round(np.random.uniform(3.0, 5.0, n), 1),
        "is_active":      np.random.choice([True, False], n, p=[0.90, 0.10]),
    })


def generate_products(suppliers_df: pd.DataFrame, n=200) -> pd.DataFrame:
    np.random.seed(42)
    supplier_ids = suppliers_df["supplier_id"].tolist()
    unit_costs   = np.abs(np.random.lognormal(4.5, 1.0, n)).round(2)
    return pd.DataFrame({
        "product_id":      [f"PROD{str(i).zfill(5)}" for i in range(1, n+1)],
        "product_name":    [f"Product {i}" for i in range(1, n+1)],
        "category":        np.random.choice(CATEGORIES, n),
        "supplier_id":     np.random.choice(supplier_ids, n),
        "unit_cost_ghs":   unit_costs,
        "unit_price_ghs":  (unit_costs * np.random.uniform(1.15, 1.50, n)).round(2),
        "unit_of_measure": np.random.choice(["PCS","KG","LTR","BOX","CTN"], n),
        "reorder_point":   np.random.randint(10, 100, n),
        "reorder_qty":     np.random.randint(50, 500, n),
        "is_active":       np.random.choice([True, False], n, p=[0.95, 0.05]),
    })


def generate_inventory(products_df: pd.DataFrame) -> pd.DataFrame:
    np.random.seed(42)
    n = len(products_df)
    qty_on_hand = np.random.randint(0, 500, n)
    qty_reserved = np.minimum(
        np.random.randint(0, 50, n),
        qty_on_hand
    )
    reorder_points = products_df["reorder_point"].values
    needs_reorder  = qty_on_hand < reorder_points

    locations = [
        f"{np.random.choice(['A','B','C','D'])}{np.random.randint(1,10):02d}-{np.random.randint(1,5)}"
        for _ in range(n)
    ]
    last_counted = [
        (datetime.now() - timedelta(days=int(np.random.randint(1, 90)))).date()
        for _ in range(n)
    ]

    return pd.DataFrame({
        "product_id":         products_df["product_id"].values,
        "quantity_on_hand":   qty_on_hand,
        "quantity_reserved":  qty_reserved,
        "warehouse_location": locations,
        "last_counted_date":  last_counted,
        "needs_reorder":      needs_reorder,
    })


def generate_stock_movements(products_df: pd.DataFrame, n=1000) -> pd.DataFrame:
    np.random.seed(42)
    product_ids = products_df["product_id"].tolist()
    costs       = products_df.set_index("product_id")["unit_cost_ghs"].to_dict()
    base_date   = datetime(2024, 1, 1)
    timestamps  = [
        base_date + timedelta(days=int(np.random.randint(0, 365)),
                              hours=int(np.random.randint(6, 22)))
        for _ in range(n)
    ]
    product_ids_col = np.random.choice(product_ids, n)
    quantities      = np.random.randint(1, 100, n)
    unit_costs_col  = [costs.get(pid, 50.0) for pid in product_ids_col]
    total_values    = [round(q * c, 2) for q, c in zip(quantities, unit_costs_col)]

    return pd.DataFrame({
        "product_id":      product_ids_col,
        "movement_type":   np.random.choice(MOVEMENT_TYPES, n,
                               p=[0.40,0.40,0.10,0.07,0.03]),
        "quantity":        quantities,
        "unit_cost_ghs":   unit_costs_col,
        "total_value_ghs": total_values,
        "reference_no":    [f"REF{str(i).zfill(8)}" for i in range(1, n+1)],
        "notes":           [f"Movement record {i}" for i in range(1, n+1)],
        "movement_date":   timestamps,
    })


def generate_purchase_orders(suppliers_df, products_df, n=150) -> pd.DataFrame:
    np.random.seed(42)
    supplier_ids = suppliers_df["supplier_id"].tolist()
    product_ids  = products_df["product_id"].tolist()
    costs        = products_df.set_index("product_id")["unit_cost_ghs"].to_dict()
    base_date    = datetime(2024, 1, 1)

    order_dates    = [
        (base_date + timedelta(days=int(np.random.randint(0, 300)))).date()
        for _ in range(n)
    ]
    expected_dates = [
        d + timedelta(days=int(np.random.randint(7, 30)))
        for d in order_dates
    ]
    statuses = np.random.choice(
        ["PENDING","APPROVED","ORDERED","RECEIVED","CANCELLED"],
        n, p=[0.15,0.20,0.25,0.35,0.05]
    )
    received_dates = [
        e + timedelta(days=int(np.random.randint(0, 5)))
        if s == "RECEIVED" else None
        for e, s in zip(expected_dates, statuses)
    ]
    product_ids_col  = np.random.choice(product_ids, n)
    supplier_ids_col = np.random.choice(supplier_ids, n)
    quantities       = np.random.randint(50, 500, n)
    unit_costs_col   = [costs.get(pid, 50.0) for pid in product_ids_col]
    total_costs      = [round(q * c, 2) for q, c in zip(quantities, unit_costs_col)]

    return pd.DataFrame({
        "po_id":           [f"PO-{str(i).zfill(8)}" for i in range(1, n+1)],
        "supplier_id":     supplier_ids_col,
        "product_id":      product_ids_col,
        "quantity_ordered": quantities,
        "unit_cost_ghs":   unit_costs_col,
        "total_cost_ghs":  total_costs,
        "status":          statuses,
        "order_date":      order_dates,
        "expected_date":   expected_dates,
        "received_date":   received_dates,
    })


def load_all(conn, suppliers, products, inventory, movements, orders):
    """Load all warehouse data into PostgreSQL."""
    with conn.cursor() as cur:
        execute_values(cur,
            "INSERT INTO warehouse_dw.suppliers "
            "(supplier_id,supplier_name,contact_person,phone,region,category,rating,is_active) "
            "VALUES %s ON CONFLICT (supplier_id) DO NOTHING",
            [tuple(r) for r in suppliers.itertuples(index=False)]
        )
        execute_values(cur,
            "INSERT INTO warehouse_dw.products "
            "(product_id,product_name,category,supplier_id,unit_cost_ghs,unit_price_ghs,"
            "unit_of_measure,reorder_point,reorder_qty,is_active) "
            "VALUES %s ON CONFLICT (product_id) DO NOTHING",
            [tuple(r) for r in products.itertuples(index=False)]
        )
        execute_values(cur,
            "INSERT INTO warehouse_dw.inventory "
            "(product_id,quantity_on_hand,quantity_reserved,warehouse_location,"
            "last_counted_date,needs_reorder) "
            "VALUES %s ON CONFLICT (product_id) DO UPDATE SET "
            "quantity_on_hand=EXCLUDED.quantity_on_hand, "
            "needs_reorder=EXCLUDED.needs_reorder",
            [tuple(r) for r in inventory.itertuples(index=False)]
        )
        execute_values(cur,
            "INSERT INTO warehouse_dw.stock_movements "
            "(product_id,movement_type,quantity,unit_cost_ghs,total_value_ghs,"
            "reference_no,notes,movement_date) VALUES %s",
            [tuple(r) for r in movements.itertuples(index=False)]
        )
        execute_values(cur,
            "INSERT INTO warehouse_dw.purchase_orders "
            "(po_id,supplier_id,product_id,quantity_ordered,unit_cost_ghs,"
            "total_cost_ghs,status,order_date,expected_date,received_date) "
            "VALUES %s ON CONFLICT (po_id) DO NOTHING",
            [tuple(r) for r in orders.itertuples(index=False)]
        )
        conn.commit()
    logger.info("[LOAD] All warehouse data loaded into PostgreSQL successfully.")


def print_summary(suppliers, products, inventory, movements, orders):
    reorder_needed = inventory["needs_reorder"].sum()
    total_stock_value = (
        inventory["quantity_on_hand"].values *
        products.set_index("product_id").loc[
            inventory["product_id"].values, "unit_cost_ghs"
        ].values
    ).sum()

    print("\n" + "="*68)
    print("   WAREHOUSE DATA MANAGEMENT SYSTEM — RUN SUMMARY")
    print("="*68)
    print(f"  Suppliers Loaded        : {len(suppliers):,}")
    print(f"  Products Loaded         : {len(products):,}")
    print(f"  Inventory Records       : {len(inventory):,}")
    print(f"  Stock Movements         : {len(movements):,}")
    print(f"  Purchase Orders         : {len(orders):,}")
    print(f"  Products Needing Reorder: {reorder_needed:,}")
    print(f"  Total Stock Value       : GHS {total_stock_value:,.2f}")
    print("-"*68)
    print("  INVENTORY BY CATEGORY:")
    cat_inv = products.merge(inventory, on="product_id") \
        .groupby("category")["quantity_on_hand"].sum() \
        .sort_values(ascending=False)
    for cat, qty in cat_inv.items():
        print(f"    {cat:<20} : {qty:,} units")
    print("-"*68)
    print("  PURCHASE ORDERS BY STATUS:")
    po_status = orders["status"].value_counts()
    for status, count in po_status.items():
        print(f"    {status:<15} : {count:,}")
    print("="*68 + "\n")


def run_system():
    logger.info("=" * 62)
    logger.info("  WAREHOUSE DATA MANAGEMENT SYSTEM — STARTED")
    logger.info("=" * 62)
    start = datetime.now()

    PROCESSED_PATH.mkdir(parents=True, exist_ok=True)

    suppliers = generate_suppliers(30)
    products  = generate_products(suppliers, 200)
    inventory = generate_inventory(products)
    movements = generate_stock_movements(products, 1000)
    orders    = generate_purchase_orders(suppliers, products, 150)

    logger.info(f"[GENERATE] Suppliers: {len(suppliers)} | Products: {len(products)} | "
                f"Inventory: {len(inventory)} | Movements: {len(movements)} | Orders: {len(orders)}")

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        create_schema(conn)
        load_all(conn, suppliers, products, inventory, movements, orders)
        conn.close()
    except Exception as e:
        logger.warning(f"[DB] PostgreSQL unavailable ({e}) — saving to CSV")
        suppliers.to_csv(PROCESSED_PATH / "suppliers.csv", index=False)
        products.to_csv(PROCESSED_PATH  / "products.csv",  index=False)
        inventory.to_csv(PROCESSED_PATH / "inventory.csv", index=False)
        movements.to_csv(PROCESSED_PATH / "movements.csv", index=False)
        orders.to_csv(PROCESSED_PATH    / "orders.csv",    index=False)

    print_summary(suppliers, products, inventory, movements, orders)
    duration = (datetime.now() - start).total_seconds()
    logger.info(f"SYSTEM COMPLETED in {duration:.2f} seconds")

    return suppliers, products, inventory, movements, orders


if __name__ == "__main__":
    run_system()