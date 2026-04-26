"""
Real-Time Warehouse Stock Movement Stream Simulator
=====================================================
Simulates Apache Kafka-style real-time streaming of
warehouse stock movements for inventory management.

Architecture:
    Producer          → generates live stock movement events
    ReorderConsumer   → alerts on low stock and reorder triggers
    MetricsConsumer   → aggregates real-time inventory KPIs
    AuditConsumer     → logs all movements to JSONL

Author: Lawrence Koomson
GitHub: github.com/lawrykoomson
"""

import queue
import threading
import time
import random
import json
import logging
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler("kafka_warehouse.log"),
        logging.StreamHandler()
    ]
)

TOPIC_NAME         = "warehouse.stock.movements"
PARTITION_COUNT    = 3
PRODUCER_RATE_HZ   = 8
SIMULATION_SECONDS = 60

CATEGORIES     = ["Electronics","Groceries","Clothing","Hardware","Pharmaceuticals","Automotive"]
MOVEMENT_TYPES = ["INBOUND","OUTBOUND","ADJUSTMENT","RETURN","DAMAGE"]
LOCATIONS      = [f"{r}{n:02d}-{s}" for r in "ABCD" for n in range(1,10) for s in range(1,5)]

REPORTS_PATH = Path("data/reports/")
REPORTS_PATH.mkdir(parents=True, exist_ok=True)


class WarehouseTopic:
    def __init__(self, name, partitions=3):
        self.name       = name
        self.partitions = [queue.Queue() for _ in range(partitions)]
        self.counter    = 0
        self.lock       = threading.Lock()

    def produce(self, msg):
        with self.lock:
            pid = self.counter % len(self.partitions)
            self.partitions[pid].put(msg)
            self.counter += 1

    def consume(self, pid, timeout=0.1):
        try:
            return self.partitions[pid].get(timeout=timeout)
        except queue.Empty:
            return None


class StockMovementProducer(threading.Thread):
    def __init__(self, topic, rate_hz, duration_secs):
        super().__init__(name="StockProducer", daemon=True)
        self.topic    = topic
        self.rate_hz  = rate_hz
        self.duration = duration_secs
        self.produced = 0
        self.running  = True
        self.logger   = logging.getLogger("StockProducer")
        self._counter = 1

    def generate_movement(self):
        category   = random.choice(CATEGORIES)
        movement   = random.choices(MOVEMENT_TYPES, weights=[40,40,10,7,3])[0]
        quantity   = random.randint(1, 100)
        unit_cost  = abs(random.lognormvariate(4.5, 1.0))
        total_val  = round(quantity * unit_cost, 2)
        qty_after  = random.randint(0, 500)
        reorder_pt = random.randint(10, 100)

        return {
            "event_id":         f"WH-EVT-{str(self._counter).zfill(8)}",
            "product_id":       f"PROD{str(random.randint(1,200)).zfill(5)}",
            "timestamp":        datetime.now().isoformat(),
            "category":         category,
            "movement_type":    movement,
            "quantity":         quantity,
            "unit_cost_ghs":    round(unit_cost, 2),
            "total_value_ghs":  total_val,
            "location":         random.choice(LOCATIONS[:20]),
            "qty_after_move":   qty_after,
            "reorder_point":    reorder_pt,
            "needs_reorder":    qty_after < reorder_pt,
            "is_damage":        movement == "DAMAGE",
            "reference_no":     f"REF{str(self._counter).zfill(8)}",
        }

    def run(self):
        self.logger.info(f"Producer started on topic '{self.topic.name}' at {self.rate_hz} movements/sec")
        end_time   = time.time() + self.duration
        sleep_time = 1.0 / self.rate_hz
        while self.running and time.time() < end_time:
            self.topic.produce(self.generate_movement())
            self.produced  += 1
            self._counter  += 1
            time.sleep(sleep_time)
        self.running = False
        self.logger.info(f"Producer finished — published {self.produced:,} stock movements")


class ReorderConsumer(threading.Thread):
    def __init__(self, topic):
        super().__init__(name="ReorderConsumer", daemon=True)
        self.topic    = topic
        self.running  = True
        self.alerts   = []
        self.logger   = logging.getLogger("ReorderConsumer")

    def run(self):
        self.logger.info("Consumer started — monitoring reorder triggers on partition 0")
        while self.running:
            msg = self.topic.consume(0)
            if msg is None:
                continue
            if msg["needs_reorder"]:
                self.alerts.append(msg)
                self.logger.warning(
                    f"REORDER ALERT | {msg['product_id']} | "
                    f"Qty: {msg['qty_after_move']} < Reorder Point: {msg['reorder_point']} | "
                    f"{msg['category']} | {msg['location']}"
                )
            if msg["is_damage"]:
                self.logger.warning(
                    f"DAMAGE RECORDED | {msg['product_id']} | "
                    f"Qty: {msg['quantity']} | Value: GHS {msg['total_value_ghs']:,.2f}"
                )


class MetricsConsumer(threading.Thread):
    def __init__(self, topic):
        super().__init__(name="MetricsConsumer", daemon=True)
        self.topic   = topic
        self.running = True
        self.logger  = logging.getLogger("MetricsConsumer")
        self.m = {
            "total": 0, "inbound": 0, "outbound": 0,
            "damage": 0, "reorder_alerts": 0,
            "total_value": 0.0, "damage_value": 0.0,
            "by_category": {}, "by_type": {}
        }

    def run(self):
        self.logger.info("Consumer started — aggregating inventory metrics on partition 1")
        while self.running:
            msg = self.topic.consume(1)
            if msg is None:
                continue
            m = self.m
            m["total"]       += 1
            m["total_value"] += msg["total_value_ghs"]
            if msg["movement_type"] == "INBOUND":  m["inbound"]  += 1
            if msg["movement_type"] == "OUTBOUND": m["outbound"] += 1
            if msg["is_damage"]:
                m["damage"]       += 1
                m["damage_value"] += msg["total_value_ghs"]
            if msg["needs_reorder"]:
                m["reorder_alerts"] += 1
            m["by_category"][msg["category"]] = \
                m["by_category"].get(msg["category"], 0) + msg["total_value_ghs"]
            m["by_type"][msg["movement_type"]] = \
                m["by_type"].get(msg["movement_type"], 0) + 1

    def snapshot(self):
        m = self.m
        return {
            "total":          m["total"],
            "inbound":        m["inbound"],
            "outbound":       m["outbound"],
            "damage":         m["damage"],
            "reorder_alerts": m["reorder_alerts"],
            "total_value":    round(m["total_value"], 2),
            "damage_value":   round(m["damage_value"], 2),
            "top_category":   max(m["by_category"], key=m["by_category"].get, default="N/A"),
        }


class AuditConsumer(threading.Thread):
    def __init__(self, topic):
        super().__init__(name="AuditConsumer", daemon=True)
        self.topic    = topic
        self.running  = True
        self.consumed = 0
        self.logger   = logging.getLogger("AuditConsumer")
        self.log_file = REPORTS_PATH / "warehouse_movements_live.jsonl"

    def run(self):
        self.logger.info(f"Consumer started — logging movements to {self.log_file}")
        with open(self.log_file, "w") as f:
            while self.running:
                msg = self.topic.consume(2)
                if msg is None:
                    continue
                self.consumed += 1
                f.write(json.dumps(msg) + "\n")
                f.flush()


def print_live_metrics(producer, metrics, reorder, audit, interval=10):
    start = time.time()
    while producer.running:
        time.sleep(interval)
        elapsed = int(time.time() - start)
        snap    = metrics.snapshot()
        print("\n" + "="*65)
        print(f"  WAREHOUSE STREAM — LIVE METRICS  [{elapsed}s elapsed]")
        print("="*65)
        print(f"  Movements Produced   : {producer.produced:,}")
        print(f"  Total Processed      : {snap['total']:,}")
        print(f"  Inbound Movements    : {snap['inbound']:,}")
        print(f"  Outbound Movements   : {snap['outbound']:,}")
        print(f"  Damage Events        : {snap['damage']:,}")
        print(f"  Reorder Alerts       : {snap['reorder_alerts']:,}")
        print(f"  Total Movement Value : GHS {snap['total_value']:,.2f}")
        print(f"  Damage Value         : GHS {snap['damage_value']:,.2f}")
        print(f"  Top Category         : {snap['top_category']}")
        print(f"  Low Stock Alerts     : {len(reorder.alerts):,}")
        print(f"  Events Logged        : {audit.consumed:,}")
        print("="*65)


def run_kafka_warehouse_simulator():
    print("\n" + "="*65)
    print("  WAREHOUSE — STOCK MOVEMENT KAFKA STREAM SIMULATOR")
    print("  Architecture: Producer → Topic → 3 Consumer Groups")
    print("="*65)
    print(f"  Topic          : {TOPIC_NAME}")
    print(f"  Partitions     : {PARTITION_COUNT}")
    print(f"  Producer Rate  : {PRODUCER_RATE_HZ} movements/sec")
    print(f"  Duration       : {SIMULATION_SECONDS} seconds")
    print(f"  Expected       : ~{PRODUCER_RATE_HZ * SIMULATION_SECONDS:,} movements")
    print("="*65 + "\n")

    topic   = WarehouseTopic(TOPIC_NAME, PARTITION_COUNT)
    producer = StockMovementProducer(topic, PRODUCER_RATE_HZ, SIMULATION_SECONDS)
    reorder  = ReorderConsumer(topic)
    metrics  = MetricsConsumer(topic)
    audit    = AuditConsumer(topic)

    for t in [producer, reorder, metrics, audit]:
        t.start()

    m_thread = threading.Thread(
        target=print_live_metrics,
        args=(producer, metrics, reorder, audit, 10),
        daemon=True
    )
    m_thread.start()
    producer.join()
    time.sleep(3)
    for t in [reorder, metrics, audit]:
        t.running = False

    final = metrics.snapshot()
    print("\n" + "="*65)
    print("  WAREHOUSE KAFKA SIMULATION — FINAL SUMMARY")
    print("="*65)
    print(f"  Total Movements        : {producer.produced:,}")
    print(f"  Inbound Movements      : {final['inbound']:,}")
    print(f"  Outbound Movements     : {final['outbound']:,}")
    print(f"  Damage Events          : {final['damage']:,}")
    print(f"  Reorder Alerts         : {final['reorder_alerts']:,}")
    print(f"  Total Movement Value   : GHS {final['total_value']:,.2f}")
    print(f"  Total Damage Value     : GHS {final['damage_value']:,.2f}")
    print(f"  Low Stock Alerts       : {len(reorder.alerts):,}")
    print(f"  Top Category           : {final['top_category']}")
    print(f"  Events Logged          : {audit.consumed:,}")
    print("="*65 + "\n")

    if reorder.alerts:
        import csv
        alerts_path = REPORTS_PATH / "reorder_alerts.csv"
        with open(alerts_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=reorder.alerts[0].keys())
            writer.writeheader()
            writer.writerows(reorder.alerts)
        print(f"  Reorder alerts saved: {alerts_path}")


if __name__ == "__main__":
    run_kafka_warehouse_simulator()