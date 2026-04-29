# 🏭 Warehouse Data Management System

![Python](https://img.shields.io/badge/Python-3.11-blue?style=flat-square&logo=python)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-336791?style=flat-square&logo=postgresql)
![dbt](https://img.shields.io/badge/dbt-1.11-FF694B?style=flat-square&logo=dbt)
![PowerBI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811?style=flat-square&logo=powerbi)
![Tests](https://img.shields.io/badge/Tests-28%2F28%20Passing-brightgreen?style=flat-square)
![Status](https://img.shields.io/badge/Status-Production%20Ready-brightgreen?style=flat-square)

A production-grade warehouse management system built on **PostgreSQL** with a fully normalised 5-table schema, automated inventory reorder tracking, purchase order management, and stock movement logging — with a full **dbt analytical layer**, **Power BI dashboard**, **Airflow DAG**, and **Kafka stream simulator**.

---

## 🏗️ System Architecture

```
[Warehouse Data Source]
           │
           ▼
     ┌───────────┐
     │  GENERATE │  ← Creates 30 suppliers, 200 products, 1,000 movements, 150 POs
     └───────────┘
           │
           ▼
     ┌───────────┐
     │   LOAD    │  ← PostgreSQL warehouse (5 normalised tables)
     └───────────┘
           │
           ▼
     ┌───────────┐
     │    dbt    │  ← Analytical layer: 1 staging view + 4 mart tables
     └───────────┘
           │
           ▼
     ┌───────────┐
     │  Power BI │  ← 4-page inventory and procurement dashboard
     └───────────┘
           │
           ▼
     ┌───────────┐
     │   Kafka   │  ← Real-time stock movement stream simulator
     └───────────┘
```

---

## 🗄️ Database Schema — 5 Normalised Tables

```sql
warehouse_dw.suppliers        -- 30 Ghana suppliers
warehouse_dw.products         -- 200 warehouse products
warehouse_dw.inventory        -- Current stock levels + reorder logic
warehouse_dw.stock_movements  -- 1,000 in/out stock transactions
warehouse_dw.purchase_orders  -- 150 supplier purchase orders
```

Key design features:
- `inventory.quantity_available` is a **generated column** (on_hand - reserved)
- `inventory.needs_reorder` auto-flags when stock falls below reorder_point
- Foreign key relationships across all 5 tables
- Referential integrity enforced at database level

---

## ✅ What The System Does

### Data Generation
- 30 Ghana suppliers across 5 regions and 6 product categories
- 200 warehouse products with cost, price, reorder points and quantities
- 200 inventory records with locations, stock levels and reorder flags
- 1,000 stock movements (Inbound, Outbound, Adjustment, Return, Damage)
- 150 purchase orders across all status stages

### Inventory Management
- Automatic reorder flag when stock < reorder_point
- Stock value calculated per product and category
- Profit margin calculated from cost vs price
- Stock status: Out of Stock / Low Stock / Adequate / Well Stocked

---

## 🔁 dbt Analytical Layer

5 models built on top of PostgreSQL:

| Model | Type | Description |
|---|---|---|
| stg_inventory | View | Joined inventory + products + suppliers |
| mart_inventory_status | Table | Stock KPIs by category and status |
| mart_stock_movements | Table | Movement analysis by type and category |
| mart_supplier_performance | Table | Fulfillment rates for 30 suppliers |
| mart_purchase_orders | Table | Full PO details with delivery status |

```bash
cd dbt
dbt run --profiles-dir .    # Run all 5 models
dbt test --profiles-dir .   # Run 4 data quality tests
```

---

## 📊 Power BI Dashboard — 4 Pages

Connected live to PostgreSQL via dbt mart tables:

| Page | Key Metrics |
|---|---|
| Executive Summary | 188 products, GHS 6.67M stock value, 47K units, 22 reorder alerts |
| Inventory Analysis | Groceries leads stock, Electronics highest margin |
| Supplier & Procurement | GHS 4.92M procurement value, 48 orders received |
| Stock Movements | GHS 6.95M total movement value, Outbound leads count |

---

## 🌊 Kafka Stream Simulator

Real-time stock movement streaming:

```bash
python kafka_warehouse_simulator.py
```

```
Topic          : warehouse.stock.movements
Partitions     : 3
Producer Rate  : 8 movements/sec
Duration       : 60 seconds

StockProducer   → generates live stock movement events
ReorderConsumer → alerts on low stock and damage events (partition 0)
MetricsConsumer → aggregates real-time inventory KPIs (partition 1)
AuditConsumer   → logs all movements to JSONL file (partition 2)

Final Results:
  Total Movements        : 477
  Inbound Movements      : 56
  Outbound Movements     : 67
  Damage Events          : 5
  Reorder Alerts         : 15
  Total Movement Value   : GHS 1,066,521.36
  Total Damage Value     : GHS 57,910.78
  Low Stock Alerts       : 14
  Top Category           : Groceries
```

---

## 🧪 Unit Tests — 28/28 Passing

```bash
pytest test_warehouse_system.py -v
# 28 passed in 2.13s
```

| Test Class | Tests | Coverage |
|---|---|---|
| TestSuppliers | 5 | Count, columns, uniqueness, rating range, regions |
| TestProducts | 6 | Count, columns, price > cost, reorder values |
| TestInventory | 5 | Count, columns, quantities, reserved logic |
| TestStockMovements | 6 | Count, movement types, value calculation |
| TestPurchaseOrders | 6 | Count, status values, received dates |

---

## 📋 Airflow DAG

Scheduled pipeline at `dags/warehouse_pipeline_dag.py`:
- Runs **every day at 01:00 AM UTC** (overnight inventory refresh)
- 5 tasks: generate, load, dbt refresh, check reorders, notify operations
- XCom passes reorder count and product totals to operations team
- Email alerts on failure with 2 retries

---

## 📊 Sample System Output

```
====================================================================
   WAREHOUSE DATA MANAGEMENT SYSTEM — RUN SUMMARY
====================================================================
  Suppliers Loaded        : 30
  Products Loaded         : 200
  Inventory Records       : 200
  Stock Movements         : 1,000
  Purchase Orders         : 150
  Products Needing Reorder: 23
  Total Stock Value       : GHS 6,898,207.01
--------------------------------------------------------------------
  INVENTORY BY CATEGORY:
    Groceries            : 11,090 units
    Pharmaceuticals      :  9,890 units
    Hardware             :  9,800 units
    Automotive           :  7,316 units
    Electronics          :  6,965 units
    Clothing             :  5,950 units
--------------------------------------------------------------------
  PURCHASE ORDERS BY STATUS:
    RECEIVED             : 48
    APPROVED             : 36
    ORDERED              : 32
    PENDING              : 26
    CANCELLED            :  8
====================================================================
```

---

## 🚀 How To Run

```bash
# 1. Clone the repo
git clone https://github.com/lawrykoomson/Warehouse-Data-Management-System.git
cd Warehouse-Data-Management-System

# 2. Create virtual environment with Python 3.11
py -3.11 -m venv venv
venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Create PostgreSQL database
psql -U postgres -c "CREATE DATABASE warehouse_db;"

# 5. Configure environment
copy .env.example .env
# Edit .env with your PostgreSQL credentials

# 6. Run the warehouse system
python warehouse_system.py

# 7. Run unit tests
pytest test_warehouse_system.py -v

# 8. Run dbt models
cd dbt
set DBT_POSTGRES_PASSWORD=your_password
dbt run --profiles-dir .
dbt test --profiles-dir .

# 9. Run Kafka stream simulator
cd ..
python kafka_warehouse_simulator.py
```

---

## 📦 Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.11 | Core system language |
| Pandas | Data generation and transformation |
| NumPy | Numerical operations |
| psycopg2 | PostgreSQL database connector |
| dbt-postgres | Analytical transformation layer |
| Apache Airflow | Pipeline orchestration DAG |
| Power BI | Inventory and procurement dashboard |
| pytest | Unit testing framework |
| python-dotenv | Environment variable management |

---

## 🔮 Roadmap

- [x] 5-table normalised PostgreSQL schema
- [x] 28 unit tests — all passing
- [x] dbt analytical layer — 5 models, 4 tests passing
- [x] Apache Airflow DAG — daily scheduled runs
- [x] Power BI dashboard — 4 pages live
- [x] Kafka stream simulator — 3 consumer groups
- [ ] Docker containerisation
- [ ] REST API for inventory queries

---

## 👨‍💻 Author

**Lawrence Koomson**
BSc. Information Technology — Data Engineering | University of Cape Coast, Ghana
🔗 [LinkedIn](https://linkedin.com/in/lawrykoomson) | [GitHub](https://github.com/lawrykoomson)
