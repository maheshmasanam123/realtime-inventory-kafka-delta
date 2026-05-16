# Real-Time Inventory: Kafka → Spark Structured Streaming → Delta

End-to-end streaming pipeline that simulates a warehouse management system
emitting inventory-change events, ingests them through Kafka, lands them in a
Delta Lake table via Spark Structured Streaming, and visualizes live stock
levels in a Streamlit dashboard.

The whole stack runs on `docker compose up` — no cloud account needed.

## Architecture

```
+-------------+     +-------+     +-----------------+     +------------+     +-----------+
| WMS event   | --> | Kafka | --> | Spark Streaming | --> | Delta Lake | --> | Streamlit |
| simulator   |     | topic |     | (exactly-once)  |     |  (MinIO)   |     | dashboard |
+-------------+     +-------+     +-----------------+     +------------+     +-----------+
```

## Stack

- Apache Kafka (Bitnami image, KRaft mode — no Zookeeper)
- Spark 3.5 Structured Streaming with the kafka-sql connector
- Delta Lake 3.2 on MinIO (S3-compatible)
- Streamlit for the live dashboard
- Python producer using `confluent-kafka` with Avro serialization

## Quick start

**Zero-infra demo (pure Python, no Kafka):**
```bash
pip install pandas pyarrow
python demo.py --events 10000
```

Runs the same upsert logic the Spark consumer applies, in memory. Expected
output: distinct (warehouse, sku) pairs, total on-hand, top-5 SKUs, and any
negative-stock alerts written to `data/state.parquet`.

**Full Kafka + Spark + Streamlit flavor (Docker):**
```bash
docker compose -f docker/docker-compose.yml up -d
python producer/wms_event_producer.py --rate 50      # 50 events/sec
python consumer/streaming_to_delta.py                # in another terminal
streamlit run dashboard/app.py                       # http://localhost:8501
```

## What this demonstrates

- Kafka producer/consumer with schema-validated Avro payloads
- Spark Structured Streaming with checkpointing and exactly-once semantics
- Delta Lake `MERGE INTO` for upserts on stock-level state
- Watermarking and late-arrival handling
- Backpressure tuning via `maxOffsetsPerTrigger`
- Streamlit dashboard auto-refreshing from the Delta table

## Repo layout

```
producer/        Event generator (configurable rate, multiple SKUs/warehouses)
consumer/        Spark Structured Streaming sink to Delta
dashboard/       Streamlit live dashboard
docker/          docker-compose: Kafka, MinIO, Spark
tests/           pytest unit tests for producer/consumer logic
.github/         CI: build, lint, integration test on every push
```
