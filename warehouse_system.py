"""
Warehouse Data Management System
==================================
A complete warehouse management system built on SQLite with:
- Fully normalised relational database schema (5 tables)
- Automated inventory tracking with reorder triggers
- Purchase order management
- Stock movement logging
- Management reporting queries
- CSV export of all reports

Tables:
    suppliers     — 30 Ghana suppliers
    products      — 200 warehouse products
    inventory     — current stock levels + reorder logic
    purchase_orders — supplier orders
    stock_movements — full audit trail of all stock changes

Author: Lawrence Koomson
GitHub: github.com/lawrykoomson
"""

import sqlite3
import pandas as pd
import numpy as np
import random
import logging
from datetime import datetime, timedelta
from pathlib import Path

# ─────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("warehouse.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────
random.seed(42)
np.random.seed(42)

DB_PATH        = Path("data/raw/warehouse.db")
PROCESSED_PATH = Path("data/processed/")
REPORTS_PATH   = Path("data/reports/")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
PROCESSED_PATH.mkdir(parents=True, exist_ok=True)
REPORTS_PATH.mkdir(parents=True, exist_ok=True)

GHANA_CITIES = [
    "Accra", "Tema", "Kumasi", "Takoradi", "Tamale",
    "Cape Coast", "Koforidua", "Ho", "Sunyani", "Wa"
]

PRODUCT_CATEGORIES = [
    "Electronics", "Food & Beverage", "Clothing & Apparel",
    "Home & Furniture", "Health & Pharma", "Industrial Supplies",
    "Stationery & Office", "Agriculture"
]

SUPPLIER_NAMES = [
    "Accra Trading Co.", "Kumasi Supplies Ltd.", "GhanaTech Imports",
    "West Africa Distributors", "Tema Port Traders", "Capital City Goods",
    "Northern Suppliers Ghana", "Volta River Merchants", "Ashanti Wholesale",
    "Eastern Regional Traders", "Pacific Ghana Imports", "Coastal Merchants Ltd.",
    "Inland Logistics Ghana", "Premium Goods GH", "Alpha Supplies Accra",
    "Beta Trading Ltd.", "Gamma Distributors", "Delta Wholesale GH",
    "Epsilon Merchants", "Zeta Importers Ghana", "Eta Supplies Co.",
    "Theta Trading House", "Iota Goods Limited", "Kappa Merchants GH",
    "Lambda Distributors", "Mu Trading Ghana", "Nu Wholesale Ltd.",
    "Xi Supplies Accra", "Omicron Trading Co.", "Pi Merchants Ghana"
]


# ─────────────────────────────────────────────
#  STEP 1 — CREATE DATABASE SCHEMA
# ─────────────────────────────────────────────
def create_schema(conn):
    """Create fully normalised warehouse schema with referential integrity."""
    cur = conn.cursor()

    cur.executescript("""
        PRAGMA foreign_keys = ON;

        DROP TABLE IF EXISTS stock_movements;
        DROP TABLE IF EXISTS purchase_orders;
        DROP TABLE IF EXISTS inventory;
        DROP TABLE IF EXISTS products;
        DROP TABLE IF EXISTS suppliers;

        -- Suppliers table
        CREATE TABLE suppliers (
            supplier_id     INTEGER PRIMARY KEY AUTOINCREMENT,
            supplier_name   TEXT NOT NULL UNIQUE,
            contact_person  TEXT,
            phone           TEXT,
            email           TEXT,
            city            TEXT,
            region          TEXT,
            payment_terms   TEXT DEFAULT '30 days',
            is_active       INTEGER DEFAULT 1,
            created_at      TEXT DEFAULT (datetime('now'))
        );

        -- Products table
        CREATE TABLE products (
            product_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            product_code    TEXT NOT NULL UNIQUE,
            product_name    TEXT NOT NULL,
            category        TEXT,
            supplier_id     INTEGER,
            unit_cost_ghs   REAL,
            unit_price_ghs  REAL,
            unit_of_measure TEXT DEFAULT 'units',
            weight_kg       REAL,
            is_active       INTEGER DEFAULT 1,
            created_at      TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
        );

        -- Inventory table (current stock levels)
        CREATE TABLE inventory (
            inventory_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id          INTEGER NOT NULL UNIQUE,
            quantity_on_hand    INTEGER DEFAULT 0,
            quantity_reserved   INTEGER DEFAULT 0,
            quantity_available  INTEGER GENERATED ALWAYS AS
                                (quantity_on_hand - quantity_reserved) VIRTUAL,
            reorder_point       INTEGER DEFAULT 50,
            reorder_quantity    INTEGER DEFAULT 200,
            max_stock_level     INTEGER DEFAULT 1000,
            warehouse_location  TEXT,
            last_counted_at     TEXT,
            last_updated_at     TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        );

        -- Purchase orders table
        CREATE TABLE purchase_orders (
            po_id           INTEGER PRIMARY KEY AUTOINCREMENT,
            po_number       TEXT NOT NULL UNIQUE,
            supplier_id     INTEGER NOT NULL,
            product_id      INTEGER NOT NULL,
            quantity_ordered INTEGER,
            unit_cost_ghs   REAL,
            total_cost_ghs  REAL GENERATED ALWAYS AS
                            (quantity_ordered * unit_cost_ghs) VIRTUAL,
            order_date      TEXT,
            expected_date   TEXT,
            received_date   TEXT,
            status          TEXT DEFAULT 'Pending',
            notes           TEXT,
            FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id),
            FOREIGN KEY (product_id)  REFERENCES products(product_id)
        );

        -- Stock movements table (full audit trail)
        CREATE TABLE stock_movements (
            movement_id     INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id      INTEGER NOT NULL,
            movement_type   TEXT NOT NULL,
            quantity        INTEGER NOT NULL,
            quantity_before INTEGER,
            quantity_after  INTEGER,
            reference_id    TEXT,
            reference_type  TEXT,
            notes           TEXT,
            created_by      TEXT DEFAULT 'system',
            created_at      TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        );
    """)
    conn.commit()
    logger.info("[SCHEMA] All 5 tables created with referential integrity.")


# ─────────────────────────────────────────────
#  STEP 2 — SEED DATA
# ─────────────────────────────────────────────
def seed_suppliers(conn):
    """Insert 30 Ghana suppliers."""
    cur = conn.cursor()
    regions = ["Greater Accra","Ashanti","Western","Eastern","Northern","Central","Volta"]
    payment_terms = ["15 days","30 days","45 days","60 days","Cash on delivery"]

    for i, name in enumerate(SUPPLIER_NAMES, 1):
        city   = random.choice(GHANA_CITIES)
        region = random.choice(regions)
        cur.execute("""
            INSERT INTO suppliers
            (supplier_name, contact_person, phone, email, city, region, payment_terms)
            VALUES (?,?,?,?,?,?,?)
        """, (
            name,
            f"Contact Person {i}",
            f"024{random.randint(1000000,9999999)}",
            f"info@supplier{i}.com.gh",
            city, region,
            random.choice(payment_terms)
        ))
    conn.commit()
    logger.info(f"[SEED] {len(SUPPLIER_NAMES)} suppliers inserted.")


def seed_products(conn):
    """Insert 200 warehouse products across 8 categories."""
    cur      = conn.cursor()
    products = []

    category_products = {
        "Electronics":          ["Smartphone","Laptop","Tablet","Monitor","Keyboard",
                                  "Mouse","Printer","Scanner","Router","UPS Unit"],
        "Food & Beverage":      ["Rice 50kg","Cooking Oil 25L","Sugar 50kg","Flour 50kg",
                                  "Tomato Paste Case","Sardines Case","Milo 500g","Tea Bags Box",
                                  "Salt 25kg","Bottled Water Case"],
        "Clothing & Apparel":   ["Men Shirts Pack","Women Dresses Pack","Children Wear Pack",
                                  "School Uniforms","Ankara Fabric Roll","Plain Fabric Roll",
                                  "Work Boots Pair","Safety Vest","Hard Hat","Gloves Pack"],
        "Home & Furniture":     ["Plastic Chairs Set","Dining Table","Office Desk","Filing Cabinet",
                                  "Mattress Single","Mattress Double","Fan Standing","Fan Ceiling",
                                  "Water Dispenser","Gas Cooker"],
        "Health & Pharma":      ["Paracetamol Box","Amoxicillin Box","Surgical Gloves Box",
                                  "Face Masks Box","Hand Sanitizer Case","BP Monitor","Thermometer Box",
                                  "Bandages Box","Antiseptic 5L","ORS Sachets Box"],
        "Industrial Supplies":  ["Cement Bags Pallet","Iron Rods Bundle","PVC Pipes Bundle",
                                  "Paint 20L","Nails 5kg","Screws Assorted","Power Drill",
                                  "Angle Grinder","Measuring Tape Box","Safety Goggles Box"],
        "Stationery & Office":  ["A4 Paper Ream","Ballpoint Pens Box","Notebooks Pack",
                                  "Staplers Box","Paper Clips Box","Folders Pack","Markers Box",
                                  "Correction Fluid Box","Envelopes Pack","Stamps Pad Box"],
        "Agriculture":          ["Fertilizer 50kg","Pesticide 5L","Maize Seeds 10kg",
                                  "Tomato Seeds Pack","Irrigation Hose Roll","Hand Sprayer",
                                  "Cutlass Dozen","Hoe Dozen","Watering Can","Farm Boots Pair"],
    }

    pid = 1
    for category, items in category_products.items():
        for item in items:
            supplier_id = random.randint(1, 30)
            cost        = round(random.uniform(20, 2000), 2)
            price       = round(cost * random.uniform(1.25, 1.60), 2)
            products.append((
                f"PRD{str(pid).zfill(4)}",
                item, category, supplier_id,
                cost, price,
                random.choice(["units","cartons","bags","rolls","pallets","boxes"]),
                round(random.uniform(0.1, 50), 2)
            ))
            pid += 1

    cur.executemany("""
        INSERT INTO products
        (product_code, product_name, category, supplier_id,
         unit_cost_ghs, unit_price_ghs, unit_of_measure, weight_kg)
        VALUES (?,?,?,?,?,?,?,?)
    """, products)
    conn.commit()
    logger.info(f"[SEED] {len(products)} products inserted across {len(category_products)} categories.")
    return len(products)


def seed_inventory(conn, n_products):
    """Initialise inventory for all products with realistic stock levels."""
    cur       = conn.cursor()
    locations = ["Zone A","Zone B","Zone C","Zone D","Zone E","Cold Storage","Overflow"]

    for pid in range(1, n_products + 1):
        qty_on_hand = random.randint(0, 800)
        qty_reserved = random.randint(0, min(qty_on_hand, 100))
        reorder_pt   = random.randint(30, 100)
        reorder_qty  = random.randint(100, 500)
        last_counted = datetime.now() - timedelta(days=random.randint(1, 90))

        cur.execute("""
            INSERT INTO inventory
            (product_id, quantity_on_hand, quantity_reserved,
             reorder_point, reorder_quantity, max_stock_level,
             warehouse_location, last_counted_at)
            VALUES (?,?,?,?,?,?,?,?)
        """, (
            pid, qty_on_hand, qty_reserved,
            reorder_pt, reorder_qty,
            random.randint(500, 2000),
            random.choice(locations),
            last_counted.strftime("%Y-%m-%d %H:%M:%S")
        ))
    conn.commit()
    logger.info(f"[SEED] Inventory initialised for {n_products} products.")


def seed_purchase_orders(conn, n_products):
    """Generate 300 purchase orders across different statuses."""
    cur      = conn.cursor()
    statuses = ["Delivered","Delivered","Delivered","Pending","In Transit","Cancelled"]
    orders   = []

    for i in range(1, 301):
        product_id  = random.randint(1, n_products)
        supplier_id = random.randint(1, 30)
        order_date  = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))
        expected    = order_date + timedelta(days=random.randint(7, 30))
        status      = random.choice(statuses)
        received    = (expected + timedelta(days=random.randint(-2, 5))).strftime("%Y-%m-%d") \
                      if status == "Delivered" else None

        cur.execute("""
            INSERT INTO purchase_orders
            (po_number, supplier_id, product_id, quantity_ordered,
             unit_cost_ghs, order_date, expected_date, received_date, status)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            f"PO-{str(i).zfill(5)}",
            supplier_id, product_id,
            random.randint(50, 500),
            round(random.uniform(20, 2000), 2),
            order_date.strftime("%Y-%m-%d"),
            expected.strftime("%Y-%m-%d"),
            received, status
        ))
    conn.commit()
    logger.info(f"[SEED] 300 purchase orders inserted.")


def seed_stock_movements(conn, n_products):
    """Generate 1,000 stock movement records as audit trail."""
    cur   = conn.cursor()
    types = ["RECEIPT","RECEIPT","ISSUE","ISSUE","ADJUSTMENT","RETURN","TRANSFER"]

    for i in range(1, 1001):
        product_id  = random.randint(1, n_products)
        move_type   = random.choice(types)
        quantity    = random.randint(1, 200)
        qty_before  = random.randint(50, 800)
        qty_after   = qty_before + quantity if move_type in ["RECEIPT","RETURN"] \
                      else max(0, qty_before - quantity)
        moved_at    = datetime(2024, 1, 1) + timedelta(
                        days=random.randint(0, 365),
                        hours=random.randint(7, 18)
                      )

        cur.execute("""
            INSERT INTO stock_movements
            (product_id, movement_type, quantity, quantity_before,
             quantity_after, reference_id, reference_type, notes, created_at)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            product_id, move_type, quantity, qty_before, qty_after,
            f"REF-{str(i).zfill(6)}",
            random.choice(["PO","SO","ADJ","RET"]),
            f"{move_type} of {quantity} units",
            moved_at.strftime("%Y-%m-%d %H:%M:%S")
        ))
    conn.commit()
    logger.info(f"[SEED] 1,000 stock movement records inserted.")


# ─────────────────────────────────────────────
#  STEP 3 — BUSINESS QUERIES & REPORTS
# ─────────────────────────────────────────────
def run_reports(conn):
    """Run all management reporting queries and export to CSV."""
    logger.info("[REPORTS] Running warehouse management reports...")

    # ── Report 1: Full Inventory Status
    inventory_status = pd.read_sql_query("""
        SELECT
            p.product_code,
            p.product_name,
            p.category,
            s.supplier_name,
            i.warehouse_location,
            i.quantity_on_hand,
            i.quantity_reserved,
            i.quantity_available,
            i.reorder_point,
            i.reorder_quantity,
            i.max_stock_level,
            ROUND(i.quantity_on_hand * p.unit_cost_ghs, 2) AS stock_value_ghs,
            CASE
                WHEN i.quantity_on_hand = 0          THEN 'OUT OF STOCK'
                WHEN i.quantity_on_hand < i.reorder_point THEN 'REORDER NOW'
                WHEN i.quantity_on_hand > i.max_stock_level THEN 'OVERSTOCKED'
                ELSE 'OK'
            END AS stock_status
        FROM inventory i
        JOIN products p ON i.product_id = p.product_id
        JOIN suppliers s ON p.supplier_id = s.supplier_id
        ORDER BY stock_value_ghs DESC
    """, conn)

    # ── Report 2: Items Needing Reorder
    reorder_needed = inventory_status[
        inventory_status["stock_status"].isin(["REORDER NOW","OUT OF STOCK"])
    ].copy()

    # ── Report 3: Stock Value by Category
    category_value = pd.read_sql_query("""
        SELECT
            p.category,
            COUNT(p.product_id)                                AS total_products,
            SUM(i.quantity_on_hand)                            AS total_units,
            ROUND(SUM(i.quantity_on_hand * p.unit_cost_ghs),2) AS total_value_ghs,
            ROUND(AVG(i.quantity_on_hand),1)                   AS avg_stock_per_product
        FROM inventory i
        JOIN products p ON i.product_id = p.product_id
        GROUP BY p.category
        ORDER BY total_value_ghs DESC
    """, conn)

    # ── Report 4: Supplier Performance
    supplier_perf = pd.read_sql_query("""
        SELECT
            s.supplier_name,
            s.city,
            COUNT(po.po_id)                        AS total_orders,
            SUM(CASE WHEN po.status='Delivered'
                THEN 1 ELSE 0 END)                 AS delivered_orders,
            ROUND(SUM(po.quantity_ordered *
                po.unit_cost_ghs),2)               AS total_spend_ghs,
            ROUND(AVG(julianday(po.received_date) -
                julianday(po.order_date)),1)        AS avg_lead_days
        FROM suppliers s
        LEFT JOIN purchase_orders po ON s.supplier_id = po.supplier_id
        GROUP BY s.supplier_id
        ORDER BY total_spend_ghs DESC
    """, conn)

    # ── Report 5: Stock Movement Summary
    movement_summary = pd.read_sql_query("""
        SELECT
            movement_type,
            COUNT(movement_id)    AS total_movements,
            SUM(quantity)         AS total_units_moved,
            ROUND(AVG(quantity),1) AS avg_quantity
        FROM stock_movements
        GROUP BY movement_type
        ORDER BY total_units_moved DESC
    """, conn)

    # ── Report 6: Top 10 Highest Value Products
    top_value = inventory_status.head(10)[
        ["product_code","product_name","category",
         "quantity_on_hand","stock_value_ghs","stock_status"]
    ]

    return inventory_status, reorder_needed, category_value, \
           supplier_perf, movement_summary, top_value


# ─────────────────────────────────────────────
#  STEP 4 — PRINT SUMMARY + EXPORT
# ─────────────────────────────────────────────
def print_summary_and_export(inventory_status, reorder_needed,
                              category_value, supplier_perf,
                              movement_summary, top_value):

    total_value    = inventory_status["stock_value_ghs"].sum()
    out_of_stock   = (inventory_status["stock_status"] == "OUT OF STOCK").sum()
    reorder_count  = (inventory_status["stock_status"] == "REORDER NOW").sum()
    overstocked    = (inventory_status["stock_status"] == "OVERSTOCKED").sum()
    ok_count       = (inventory_status["stock_status"] == "OK").sum()

    print("\n" + "="*68)
    print("   WAREHOUSE DATA MANAGEMENT SYSTEM — RUN SUMMARY")
    print("="*68)
    print(f"  Total Products Tracked   : {len(inventory_status):,}")
    print(f"  Total Inventory Value    : GHS {total_value:,.2f}")
    print(f"  Total Units In Stock     : {inventory_status['quantity_on_hand'].sum():,}")
    print(f"  Stock Status — OK        : {ok_count:,} products")
    print(f"  Stock Status — Reorder   : {reorder_count:,} products")
    print(f"  Stock Status — Critical  : {out_of_stock:,} products (OUT OF STOCK)")
    print(f"  Stock Status — Overstock : {overstocked:,} products")

    print("\n" + "-"*68)
    print("  INVENTORY VALUE BY CATEGORY")
    print("-"*68)
    for _, row in category_value.iterrows():
        pct = row["total_value_ghs"] / total_value * 100
        print(f"  {row['category']:<25} : "
              f"GHS {row['total_value_ghs']:>12,.2f}  "
              f"({pct:.1f}%)  "
              f"| {int(row['total_products'])} products")

    print("\n" + "-"*68)
    print(f"  PRODUCTS REQUIRING REORDER ({len(reorder_needed)} items)")
    print("-"*68)
    for _, row in reorder_needed.head(10).iterrows():
        print(f"  [{row['stock_status']:<13}] "
              f"{row['product_name']:<30} "
              f"On Hand: {int(row['quantity_on_hand']):>4}  "
              f"Reorder Qty: {int(row['reorder_quantity']):>4}")

    print("\n" + "-"*68)
    print("  TOP 5 SUPPLIERS BY SPEND")
    print("-"*68)
    for _, row in supplier_perf.head(5).iterrows():
        delivery_rate = (row["delivered_orders"] / row["total_orders"] * 100
                         if row["total_orders"] > 0 else 0)
        print(f"  {row['supplier_name']:<30} "
              f"GHS {row['total_spend_ghs']:>10,.2f}  "
              f"| Orders: {int(row['total_orders']):>3}  "
              f"| Delivery Rate: {delivery_rate:.0f}%")

    print("\n" + "-"*68)
    print("  STOCK MOVEMENT AUDIT SUMMARY")
    print("-"*68)
    for _, row in movement_summary.iterrows():
        print(f"  {row['movement_type']:<15} : "
              f"{int(row['total_movements']):>4} movements  |  "
              f"{int(row['total_units_moved']):>6,} units moved  |  "
              f"Avg: {row['avg_quantity']:.1f} units each")

    print("\n" + "-"*68)
    print("  TOP 10 PRODUCTS BY STOCK VALUE")
    print("-"*68)
    for rank, (_, row) in enumerate(top_value.iterrows(), 1):
        print(f"  {rank:>2}. [{row['product_code']}] "
              f"{row['product_name']:<30} "
              f"GHS {row['stock_value_ghs']:>10,.2f}  "
              f"[{row['stock_status']}]")

    print("\n" + "-"*68)
    print("  EXPORTING REPORTS TO CSV")
    print("-"*68)

    exports = {
        "inventory_status.csv":    inventory_status,
        "reorder_alerts.csv":      reorder_needed,
        "category_value.csv":      category_value,
        "supplier_performance.csv": supplier_perf,
        "stock_movements_summary.csv": movement_summary,
        "top_value_products.csv":  top_value,
    }
    for filename, df in exports.items():
        path = REPORTS_PATH / filename
        df.to_csv(path, index=False)
        print(f"  Saved: {path}  ({len(df):,} rows)")

    print("="*68 + "\n")


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
def run_warehouse_system():
    logger.info("="*62)
    logger.info("  WAREHOUSE DATA MANAGEMENT SYSTEM — STARTED")
    logger.info("="*62)
    start = datetime.now()

    # Connect to SQLite
    conn = sqlite3.connect(DB_PATH)
    logger.info(f"[DB] Connected to: {DB_PATH}")

    # Step 1 — Schema
    logger.info("STEP 1/4 — Creating database schema...")
    create_schema(conn)

    # Step 2 — Seed data
    logger.info("STEP 2/4 — Seeding warehouse data...")
    seed_suppliers(conn)
    n_products = seed_products(conn)
    seed_inventory(conn, n_products)
    seed_purchase_orders(conn, n_products)
    seed_stock_movements(conn, n_products)

    # Step 3 — Reports
    logger.info("STEP 3/4 — Running management reports...")
    inventory_status, reorder_needed, category_value, \
    supplier_perf, movement_summary, top_value = run_reports(conn)

    # Step 4 — Print + Export
    logger.info("STEP 4/4 — Printing summary and exporting CSVs...")
    print_summary_and_export(
        inventory_status, reorder_needed, category_value,
        supplier_perf, movement_summary, top_value
    )

    conn.close()

    duration = (datetime.now() - start).total_seconds()
    logger.info(f"WAREHOUSE SYSTEM COMPLETED in {duration:.2f} seconds")


if __name__ == "__main__":
    run_warehouse_system()