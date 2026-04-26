"""
Apache Airflow DAG — Warehouse Data Management System
======================================================
Schedules the warehouse system to run every
day at 01:00 AM UTC (overnight inventory refresh).

Tasks:
    1. generate_data     — generate warehouse data
    2. load_to_postgres  — load into PostgreSQL
    3. refresh_dbt       — rebuild analytical layer
    4. check_reorders    — flag products needing reorder
    5. notify_operations — log daily warehouse summary

Author: Lawrence Koomson
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logger = logging.getLogger(__name__)

default_args = {
    "owner":            "lawrence_koomson",
    "depends_on_past":  False,
    "email":            ["koomsonlawrence64@gmail.com"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

dag = DAG(
    dag_id="warehouse_data_management_system",
    default_args=default_args,
    description="Daily warehouse inventory and procurement data pipeline",
    schedule_interval="0 1 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["warehouse","inventory","procurement","ghana","data-engineering"],
)


def task_generate(**context):
    from warehouse_system import (
        generate_suppliers, generate_products,
        generate_inventory, generate_stock_movements,
        generate_purchase_orders
    )
    suppliers = generate_suppliers(30)
    products  = generate_products(suppliers, 200)
    inventory = generate_inventory(products)
    movements = generate_stock_movements(products, 1000)
    orders    = generate_purchase_orders(suppliers, products, 150)

    reorder_count = int(inventory["needs_reorder"].sum())
    context["ti"].xcom_push(key="reorder_count",    value=reorder_count)
    context["ti"].xcom_push(key="total_products",   value=len(products))
    context["ti"].xcom_push(key="total_movements",  value=len(movements))

    logger.info(f"Generated: {len(suppliers)} suppliers, {len(products)} products, "
                f"{len(movements)} movements, {len(orders)} orders")
    return len(products)


def task_load(**context):
    import psycopg2
    from warehouse_system import (
        generate_suppliers, generate_products,
        generate_inventory, generate_stock_movements,
        generate_purchase_orders, create_schema, load_all, DB_CONFIG
    )
    suppliers = generate_suppliers(30)
    products  = generate_products(suppliers, 200)
    inventory = generate_inventory(products)
    movements = generate_stock_movements(products, 1000)
    orders    = generate_purchase_orders(suppliers, products, 150)

    conn = psycopg2.connect(**DB_CONFIG)
    create_schema(conn)
    load_all(conn, suppliers, products, inventory, movements, orders)
    conn.close()
    logger.info("Warehouse data loaded to PostgreSQL")
    return "success"


def task_check_reorders(**context):
    reorder_count = context["ti"].xcom_pull(
        task_ids="generate_data", key="reorder_count"
    )
    logger.info(f"REORDER ALERT: {reorder_count} products need restocking")
    if reorder_count > 20:
        logger.warning(f"HIGH REORDER COUNT: {reorder_count} products below reorder point")
    return reorder_count


def task_notify(**context):
    run_date     = context["ds"]
    reorder      = context["ti"].xcom_pull(task_ids="generate_data", key="reorder_count")
    total_prod   = context["ti"].xcom_pull(task_ids="generate_data", key="total_products")
    total_moves  = context["ti"].xcom_pull(task_ids="generate_data", key="total_movements")

    logger.info("=" * 60)
    logger.info("  WAREHOUSE SYSTEM — DAILY OPERATIONS SUMMARY")
    logger.info("=" * 60)
    logger.info(f"  Run Date            : {run_date}")
    logger.info(f"  Products Tracked    : {total_prod:,}")
    logger.info(f"  Stock Movements     : {total_moves:,}")
    logger.info(f"  Reorder Alerts      : {reorder:,}")
    logger.info("=" * 60)
    return "notified"


start         = EmptyOperator(task_id="pipeline_start",    dag=dag)
generate_task = PythonOperator(task_id="generate_data",    python_callable=task_generate, dag=dag)
load_task     = PythonOperator(task_id="load_to_postgres", python_callable=task_load,     dag=dag)
reorder_task  = PythonOperator(task_id="check_reorders",   python_callable=task_check_reorders, dag=dag)
notify_task   = PythonOperator(task_id="notify_operations",python_callable=task_notify,   dag=dag)
end           = EmptyOperator(task_id="pipeline_end",      dag=dag)

start >> generate_task >> load_task >> reorder_task >> notify_task >> end