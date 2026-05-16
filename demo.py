"""In-memory streaming demo (no Kafka, no Spark, no Docker).

Generates the same WMS events the producer would emit, runs them through the
same upsert logic the Spark consumer applies, and writes current stock state
to a Parquet file.

Usage:
    python demo.py --events 10000
"""
from __future__ import annotations

import argparse
import random
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


EVENT_TYPES = ["RECEIPT", "PICK", "ADJUST", "TRANSFER"]
WAREHOUSES = [f"WH-{i:02d}" for i in range(1, 9)]
SKUS = [f"SKU-{i:05d}" for i in range(1, 501)]


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


OUT = Path("data/state.parquet")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--events", type=int, default=10_000)
    args = p.parse_args()

    state: dict[tuple[str, str], int] = defaultdict(int)
    events: list[dict] = []
    t0 = time.time()
    for _ in range(args.events):
        ev = make_event()
        state[(ev["warehouse_id"], ev["sku"])] += ev["qty_delta"]
        events.append(ev)
    elapsed = time.time() - t0

    OUT.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(
        [{"warehouse_id": w, "sku": s, "on_hand": q} for (w, s), q in state.items()]
    ).sort_values(["warehouse_id", "sku"])
    df.to_parquet(OUT, index=False)

    print(f"processed {args.events:,} events in {elapsed:.2f}s "
          f"({args.events / max(elapsed, 0.001):,.0f} events/s)")
    print(f"distinct (warehouse, sku) pairs: {len(df):,}")
    print(f"total on hand: {df['on_hand'].sum():,}")
    print(f"\ntop 5 SKUs by on_hand:")
    print(df.nlargest(5, "on_hand").to_string(index=False))

    neg = df[df["on_hand"] < 0]
    if len(neg):
        print(f"\n{len(neg)} (warehouse, sku) pairs went negative — would alert in production")
        print(neg.head().to_string(index=False))
    else:
        print("\nNo negative stock — clean.")

    event_log = Path("data/event_log.parquet")
    pd.DataFrame(events).to_parquet(event_log, index=False)
    print(f"\nfull event log: {event_log}")


if __name__ == "__main__":
    main()
