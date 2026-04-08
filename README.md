# 🏭 Warehouse Data Management System

![Python](https://img.shields.io/badge/Python-3.14-blue?style=flat-square&logo=python)
![SQLite](https://img.shields.io/badge/SQLite-Database-003B57?style=flat-square&logo=sqlite)
![Pandas](https://img.shields.io/badge/Pandas-3.0.2-150458?style=flat-square&logo=pandas)
![Status](https://img.shields.io/badge/Status-Active-brightgreen?style=flat-square)

A production-grade warehouse data management system built on SQLite with a fully normalised relational database schema, automated inventory tracking, purchase order management, stock movement audit trail, and management reporting.

---

## 🏗️ System Architecture

```
┌─────────────────────────────────────────────┐
│  DATABASE SCHEMA (SQLite — 5 tables)        │
│  suppliers → products → inventory           │
│  purchase_orders → stock_movements          │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│  DATA SEEDING                               │
│  30 suppliers · 200 products · inventory    │
│  300 purchase orders · 1,000 movements      │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│  MANAGEMENT REPORTS (6 reports)             │
│  Inventory status · Reorder alerts          │
│  Category value · Supplier performance      │
│  Movement audit · Top value products        │
└─────────────────────────────────────────────┘
```

---

## 🗄️ Database Schema

```sql
suppliers (
    supplier_id, supplier_name, contact_person,
    phone, email, city, region, payment_terms, is_active
)

products (
    product_id, product_code, product_name, category,
    supplier_id, unit_cost_ghs, unit_price_ghs,
    unit_of_measure, weight_kg, is_active
)

inventory (
    inventory_id, product_id, quantity_on_hand,
    quantity_reserved, quantity_available (VIRTUAL),
    reorder_point, reorder_quantity, max_stock_level,
    warehouse_location, last_counted_at
)

purchase_orders (
    po_id, po_number, supplier_id, product_id,
    quantity_ordered, unit_cost_ghs,
    total_cost_ghs (VIRTUAL), order_date,
    expected_date, received_date, status
)

stock_movements (
    movement_id, product_id, movement_type,
    quantity, quantity_before, quantity_after,
    reference_id, reference_type, notes, created_at
)
```

---

## ✅ Features

**Schema Design**
- 5 fully normalised tables with foreign key constraints
- Virtual computed columns (quantity_available, total_cost_ghs)
- Full referential integrity with PRAGMA foreign_keys = ON

**Inventory Management**
- Real-time stock status: OK / REORDER NOW / OUT OF STOCK / OVERSTOCKED
- Automated reorder point detection
- Stock value calculation per product and category

**Purchase Order Management**
- 300 orders across Pending / In Transit / Delivered / Cancelled statuses
- Supplier lead time tracking
- Delivery rate performance per supplier

**Stock Movement Audit Trail**
- 1,000 movement records: RECEIPT / ISSUE / ADJUSTMENT / RETURN / TRANSFER
- Before and after quantity tracking on every movement
- Full reference linking to source document

**Management Reports (6 CSV exports)**
- Complete inventory status report
- Reorder alerts with urgency flagging
- Stock value breakdown by category
- Supplier performance ranking
- Stock movement audit summary
- Top 10 highest value products

---

## 📊 Sample Output

```
====================================================================
   WAREHOUSE DATA MANAGEMENT SYSTEM — RUN SUMMARY
====================================================================
  Total Products Tracked   : 200
  Total Inventory Value    : GHS X,XXX,XXX.XX
  Stock Status — OK        : XXX products
  Stock Status — Reorder   : XX products
  Stock Status — Critical  : XX products (OUT OF STOCK)
====================================================================
```

---

## 🚀 How To Run

```bash
git clone https://github.com/lawrykoomson/Warehouse-Data-Management-System.git
cd Warehouse-Data-Management-System
pip install -r requirements.txt
python warehouse_system.py
```

---

## 📦 Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.14 | Core language |
| SQLite | Embedded relational database |
| Pandas | SQL query results and CSV export |
| NumPy | Numerical operations |
| psycopg2 | PostgreSQL connector (future migration) |

---

## 🔮 Future Improvements
- [ ] Migrate schema to PostgreSQL for multi-user access
- [ ] Stored procedures for automated reorder triggers
- [ ] Power BI dashboard for live inventory monitoring
- [ ] REST API for warehouse mobile scanning integration
- [ ] Barcode scanning input support

---

## 👨‍💻 Author

**Lawrence Koomson**
BSc. Information Technology — Data Engineering | University of Cape Coast, Ghana
🔗 [LinkedIn](https://linkedin.com/in/lawrykoomson) | [GitHub](https://github.com/lawrykoomson)