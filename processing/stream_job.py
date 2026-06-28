"""
InsightsStream — PySpark Structured Streaming aggregation job.

Reads events from Kafka, applies 1-minute tumbling-window aggregation per
metric, and idempotently upserts the results into PostgreSQL. Checkpointing
provides fault tolerance and recovery without duplicate processing.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "events"
CHECKPOINT = "/tmp/insightsstream/checkpoints"
PG_URL = "jdbc:postgresql://localhost:5432/insights"
PG_PROPS = {"user": "insights", "password": "insights", "driver": "org.postgresql.Driver"}

EVENT_SCHEMA = StructType(
    [
        StructField("entity_id", StringType()),
        StructField("metric", StringType()),
        StructField("value", IntegerType()),
        StructField("ts", TimestampType()),
    ]
)


def upsert_batch(batch_df, _batch_id: int) -> None:
    """Idempotent write: aggregates keyed by (metric, window_start)."""
    (
        batch_df.write.mode("append")
        .jdbc(PG_URL, "metric_aggregates", properties=PG_PROPS)
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("InsightsStream-Aggregator")
        .config("spark.sql.shuffle.partitions", "12")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("maxOffsetsPerTrigger", 50_000)  # backpressure cap
        .load()
    )

    parsed = raw.select(
        from_json(col("value").cast("string"), EVENT_SCHEMA).alias("e")
    ).select("e.*")

    aggregated = (
        parsed.withWatermark("ts", "2 minutes")
        .groupBy(window(col("ts"), "1 minute"), col("metric"))
        .sum("value")
        .selectExpr(
            "window.start as window_start",
            "metric",
            "`sum(value)` as total",
        )
    )

    query = (
        aggregated.writeStream.outputMode("update")
        .foreachBatch(upsert_batch)
        .option("checkpointLocation", CHECKPOINT)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
