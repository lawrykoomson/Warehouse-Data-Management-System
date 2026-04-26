"""
Unit Tests — Warehouse Data Management System
===============================================
Run with: pytest test_warehouse_system.py -v

Author: Lawrence Koomson
"""

import pytest
import pandas as pd
import numpy as np
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from warehouse_system import (
    generate_suppliers, generate_products,
    generate_inventory, generate_stock_movements,
    generate_purchase_orders
)


@pytest.fixture(scope="module")
def suppliers():
    return generate_suppliers(30)

@pytest.fixture(scope="module")
def products(suppliers):
    return generate_products(suppliers, 200)

@pytest.fixture(scope="module")
def inventory(products):
    return generate_inventory(products)

@pytest.fixture(scope="module")
def movements(products):
    return generate_stock_movements(products, 1000)

@pytest.fixture(scope="module")
def orders(suppliers, products):
    return generate_purchase_orders(suppliers, products, 150)


class TestSuppliers:

    def test_correct_count(self, suppliers):
        assert len(suppliers) == 30

    def test_required_columns(self, suppliers):
        for col in ["supplier_id","supplier_name","region","category","rating","is_active"]:
            assert col in suppliers.columns

    def test_unique_supplier_ids(self, suppliers):
        assert suppliers["supplier_id"].nunique() == 30

    def test_rating_range(self, suppliers):
        assert suppliers["rating"].between(3.0, 5.0).all()

    def test_regions_valid(self, suppliers):
        valid = {"Greater Accra","Ashanti","Western","Eastern","Northern"}
        assert set(suppliers["region"].unique()).issubset(valid)


class TestProducts:

    def test_correct_count(self, products):
        assert len(products) == 200

    def test_required_columns(self, products):
        for col in ["product_id","product_name","category","supplier_id",
                    "unit_cost_ghs","unit_price_ghs","reorder_point","reorder_qty"]:
            assert col in products.columns

    def test_unique_product_ids(self, products):
        assert products["product_id"].nunique() == 200

    def test_price_above_cost(self, products):
        assert (products["unit_price_ghs"] >= products["unit_cost_ghs"]).all()

    def test_reorder_values_positive(self, products):
        assert (products["reorder_point"] > 0).all()
        assert (products["reorder_qty"] > 0).all()

    def test_categories_valid(self, products):
        valid = {"Electronics","Groceries","Clothing","Hardware","Pharmaceuticals","Automotive"}
        assert set(products["category"].unique()).issubset(valid)


class TestInventory:

    def test_correct_count(self, inventory):
        assert len(inventory) == 200

    def test_required_columns(self, inventory):
        for col in ["product_id","quantity_on_hand","quantity_reserved",
                    "warehouse_location","needs_reorder"]:
            assert col in inventory.columns

    def test_quantities_non_negative(self, inventory):
        assert (inventory["quantity_on_hand"] >= 0).all()
        assert (inventory["quantity_reserved"] >= 0).all()

    def test_reserved_not_exceed_on_hand(self, inventory):
        assert (inventory["quantity_reserved"] <= inventory["quantity_on_hand"]).all()

    def test_needs_reorder_is_boolean(self, inventory):
        assert inventory["needs_reorder"].dtype == bool


class TestStockMovements:

    def test_correct_count(self, movements):
        assert len(movements) == 1000

    def test_required_columns(self, movements):
        for col in ["product_id","movement_type","quantity",
                    "unit_cost_ghs","total_value_ghs","reference_no"]:
            assert col in movements.columns

    def test_movement_types_valid(self, movements):
        valid = {"INBOUND","OUTBOUND","ADJUSTMENT","RETURN","DAMAGE"}
        assert set(movements["movement_type"].unique()).issubset(valid)

    def test_quantities_positive(self, movements):
        assert (movements["quantity"] > 0).all()

    def test_total_value_correct(self, movements):
        calculated = (movements["quantity"] * movements["unit_cost_ghs"]).round(2)
        assert (abs(calculated - movements["total_value_ghs"]) < 0.02).all()

    def test_unique_reference_numbers(self, movements):
        assert movements["reference_no"].nunique() == len(movements)


class TestPurchaseOrders:

    def test_correct_count(self, orders):
        assert len(orders) == 150

    def test_required_columns(self, orders):
        for col in ["po_id","supplier_id","product_id","quantity_ordered",
                    "total_cost_ghs","status","order_date"]:
            assert col in orders.columns

    def test_unique_po_ids(self, orders):
        assert orders["po_id"].nunique() == 150

    def test_status_values_valid(self, orders):
        valid = {"PENDING","APPROVED","ORDERED","RECEIVED","CANCELLED"}
        assert set(orders["status"].unique()).issubset(valid)

    def test_total_cost_positive(self, orders):
        assert (orders["total_cost_ghs"] > 0).all()

    def test_received_orders_have_dates(self, orders):
        received = orders[orders["status"] == "RECEIVED"]
        assert received["received_date"].notna().all()