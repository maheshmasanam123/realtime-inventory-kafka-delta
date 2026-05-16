"""Spark Structured Streaming: Kafka -> Delta.

Two outputs:
  1. inventory_events  - append-only raw event log (audit)
  2. inventory_state   - MERGE-upserted current stock per (warehouse, sku)
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sum as _sum, window
from pyspark.sql.types import (IntegerType, StringType, StructField, StructType,
                               TimestampType)
from delta.tables import DeltaTable


SCHEMA = StructType([
    StructField("event_id",     StringType()),
    StructField("event_type",   StringType()),
    StructField("warehouse_id", StringType()),
    StructField("sku",          StringType()),
    StructField("qty_delta",    IntegerType()),
    StructField("event_time",   TimestampType()),
])


def spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("inventory-streaming")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def upsert_state(batch_df, _batch_id: int) -> None:
    spark = batch_df.sparkSession
    deltas = (
        batch_df.groupBy("warehouse_id", "sku")
        .agg(_sum("qty_delta").alias("qty_delta"))
    )
    target_path = "s3a://inventory/state"
    if not DeltaTable.isDeltaTable(spark, target_path):
        deltas.withColumnRenamed("qty_delta", "on_hand").write.format("delta").save(target_path)
        return
    tgt = DeltaTable.forPath(spark, target_path)
    (
        tgt.alias("t")
        .merge(deltas.alias("s"), "t.warehouse_id = s.warehouse_id AND t.sku = s.sku")
        .whenMatchedUpdate(set={"on_hand": "t.on_hand + s.qty_delta"})
        .whenNotMatchedInsert(values={
            "warehouse_id": "s.warehouse_id",
            "sku":          "s.sku",
            "on_hand":      "s.qty_delta",
        })
        .execute()
    )


def main(bootstrap: str = "localhost:9092", topic: str = "inventory.events") -> None:
    spark = spark_session()
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", "5000")
        .load()
    )
    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json")
        .select(from_json(col("json"), SCHEMA).alias("e"))
        .select("e.*")
        .withWatermark("event_time", "10 minutes")
    )

    (
        parsed.writeStream.format("delta")
        .option("checkpointLocation", "s3a://inventory/_chk/events")
        .outputMode("append")
        .start("s3a://inventory/events")
    )

    (
        parsed.writeStream.foreachBatch(upsert_state)
        .option("checkpointLocation", "s3a://inventory/_chk/state")
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
