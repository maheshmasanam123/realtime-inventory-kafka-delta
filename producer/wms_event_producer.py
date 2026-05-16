"""Warehouse-management-system event simulator.

Emits inventory-change events (RECEIPT, PICK, ADJUST, TRANSFER) to a Kafka
topic at a configurable rate. Each event carries a SKU, warehouse, quantity
delta, and an event_time so downstream stream processors can compute current
stock state.
"""
from __future__ import annotations

import argparse
import json
import random
import signal
import time
from datetime import datetime, timezone

from confluent_kafka import Producer


EVENT_TYPES = ["RECEIPT", "PICK", "ADJUST", "TRANSFER"]
WAREHOUSES = [f"WH-{i:02d}" for i in range(1, 9)]
SKUS = [f"SKU-{i:05d}" for i in range(1, 501)]

_running = True


def _stop(*_: object) -> None:
    global _running
    _running = False


def make_event() -> dict:
    event_type = random.choices(EVENT_TYPES, weights=[3, 6, 1, 1])[0]
    qty = random.randint(1, 200)
    if event_type == "PICK":
        qty = -qty
    elif event_type == "ADJUST":
        qty = random.choice([-1, 1]) * random.randint(1, 20)
    return {
        "event_id": f"{int(time.time()*1000)}-{random.randint(0,9999):04d}",
        "event_type": event_type,
        "warehouse_id": random.choice(WAREHOUSES),
        "sku": random.choice(SKUS),
        "qty_delta": qty,
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="inventory.events")
    p.add_argument("--rate", type=int, default=20, help="events per second")
    args = p.parse_args()

    producer = Producer({"bootstrap.servers": args.bootstrap, "linger.ms": 20})
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    sleep_for = 1.0 / max(args.rate, 1)
    sent = 0
    while _running:
        ev = make_event()
        producer.produce(args.topic, key=ev["sku"], value=json.dumps(ev).encode("utf-8"))
        sent += 1
        if sent % 100 == 0:
            producer.poll(0)
            print(f"sent={sent}")
        time.sleep(sleep_for)

    producer.flush(10)
    print(f"flushed. total sent={sent}")


if __name__ == "__main__":
    main()
