"""Unified batch and streaming Delta Lake pipeline on local MinIO."""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import boto3
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StringType, StructField, StructType

from insightsstream.pipeline import PipelineRun

HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"
EVENT_SCHEMA = StructType(
    [
        StructField("entity_id", StringType(), True),
        StructField("metric", StringType(), True),
        StructField("value", StringType(), True),
        StructField("ts", StringType(), True),
    ]
)


@dataclass(frozen=True)
class DeltaSettings:
    endpoint: str = "http://minio:9000"
    access_key: str = "insights"
    secret_key: str = "insights-local"
    bucket: str = "insightsstream"
    kafka_bootstrap_servers: str = "kafka:29092"

    @classmethod
    def from_env(cls) -> "DeltaSettings":
        return cls(
            endpoint=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
            access_key=os.getenv("MINIO_ACCESS_KEY", "insights"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "insights-local"),
            bucket=os.getenv("MINIO_BUCKET", "insightsstream"),
            kafka_bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"
            ),
        )

    def uri(self, prefix: str) -> str:
        return f"s3a://{self.bucket}/{prefix.strip('/')}"


def create_delta_spark_session(
    app_name: str = "InsightsStream-Delta",
    settings: DeltaSettings | None = None,
) -> SparkSession:
    resolved = settings or DeltaSettings.from_env()
    builder = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.fs.s3a.endpoint", resolved.endpoint)
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.access.key", resolved.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", resolved.secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config(
            "spark.delta.logStore.class",
            "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
        )
    )
    return configure_spark_with_delta_pip(
        builder,
        extra_packages=[HADOOP_AWS_PACKAGE],
    ).getOrCreate()


def create_minio_client(settings: DeltaSettings | None = None):
    resolved = settings or DeltaSettings.from_env()
    return boto3.client(
        "s3",
        endpoint_url=resolved.endpoint,
        aws_access_key_id=resolved.access_key,
        aws_secret_access_key=resolved.secret_key,
        region_name="us-east-1",
    )


def ensure_bucket(settings: DeltaSettings | None = None) -> None:
    resolved = settings or DeltaSettings.from_env()
    client = create_minio_client(resolved)
    buckets = {item["Name"] for item in client.list_buckets().get("Buckets", [])}
    if resolved.bucket not in buckets:
        client.create_bucket(Bucket=resolved.bucket)


def upload_source(
    source_path: str | Path,
    settings: DeltaSettings | None = None,
) -> str:
    resolved = settings or DeltaSettings.from_env()
    path = Path(source_path)
    key = f"source/{path.name}"
    ensure_bucket(resolved)
    create_minio_client(resolved).upload_file(str(path), resolved.bucket, key)
    return resolved.uri(key)


def reset_lake(settings: DeltaSettings | None = None) -> int:
    """Delete project objects while retaining the MinIO bucket."""
    resolved = settings or DeltaSettings.from_env()
    ensure_bucket(resolved)
    client = create_minio_client(resolved)
    paginator = client.get_paginator("list_objects_v2")
    deleted = 0
    for page in paginator.paginate(Bucket=resolved.bucket):
        objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        if objects:
            client.delete_objects(
                Bucket=resolved.bucket,
                Delete={"Objects": objects, "Quiet": True},
            )
            deleted += len(objects)
    return deleted


def list_lake_objects(settings: DeltaSettings | None = None) -> list[str]:
    resolved = settings or DeltaSettings.from_env()
    ensure_bucket(resolved)
    client = create_minio_client(resolved)
    paginator = client.get_paginator("list_objects_v2")
    return sorted(
        item["Key"]
        for page in paginator.paginate(Bucket=resolved.bucket)
        for item in page.get("Contents", [])
    )


def _add_bronze_metadata(
    frame: DataFrame,
    source_type: str,
    source_name: str,
    batch_id: str,
) -> DataFrame:
    return (
        frame.select(*EVENT_SCHEMA.fieldNames())
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("ingestion_date", F.current_date())
        .withColumn("source_type", F.lit(source_type))
        .withColumn("source_name", F.lit(source_name))
        .withColumn("batch_id", F.lit(batch_id))
    )


def ingest_batch_to_bronze(
    spark: SparkSession,
    source_uri: str,
    settings: DeltaSettings | None = None,
    batch_id: str | None = None,
) -> PipelineRun:
    resolved = settings or DeltaSettings.from_env()
    now = datetime.now(timezone.utc)
    resolved_batch_id = batch_id or f"batch-{now:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"
    source = spark.read.option("header", True).schema(EVENT_SCHEMA).csv(source_uri)
    bronze = (
        _add_bronze_metadata(
            source,
            source_type="batch",
            source_name=source_uri,
            batch_id=resolved_batch_id,
        )
        .withColumn("kafka_topic", F.lit(None).cast("string"))
        .withColumn("kafka_partition", F.lit(None).cast("int"))
        .withColumn("kafka_offset", F.lit(None).cast("long"))
        .cache()
    )
    bronze_rows = bronze.count()
    bronze_path = resolved.uri("bronze/events")
    (
        bronze.write.format("delta")
        .mode("append")
        .partitionBy("ingestion_date")
        .save(bronze_path)
    )
    bronze.unpersist()
    return PipelineRun(
        batch_id=resolved_batch_id,
        source_name=source_uri,
        started_at=now.isoformat(),
        bronze_key=bronze_path,
        bronze_rows=bronze_rows,
    )


def start_kafka_to_bronze(
    spark: SparkSession,
    settings: DeltaSettings | None = None,
    available_now: bool = False,
) -> StreamingQuery:
    resolved = settings or DeltaSettings.from_env()
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", resolved.kafka_bootstrap_servers)
        .option("subscribe", "events")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    ).select("event.*", "kafka_topic", "kafka_partition", "kafka_offset")
    bronze = (
        parsed.withColumn("ingested_at", F.current_timestamp())
        .withColumn("ingestion_date", F.current_date())
        .withColumn("source_type", F.lit("stream"))
        .withColumn("source_name", F.lit("kafka://events"))
        .withColumn(
            "batch_id",
            F.concat_ws(
                "-",
                F.lit("kafka"),
                F.col("kafka_partition"),
                F.col("kafka_offset"),
            ),
        )
    )
    writer = (
        bronze.writeStream.format("delta")
        .outputMode("append")
        .partitionBy("ingestion_date")
        .option("mergeSchema", True)
        .option(
            "checkpointLocation",
            resolved.uri("checkpoints/kafka-to-bronze"),
        )
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime="10 seconds")
    return writer.start(resolved.uri("bronze/events"))


def build_silver(
    spark: SparkSession,
    settings: DeltaSettings | None = None,
) -> tuple[int, int]:
    resolved = settings or DeltaSettings.from_env()
    bronze = spark.read.format("delta").load(resolved.uri("bronze/events"))
    bronze.createOrReplaceTempView("bronze_events")
    typed = spark.sql(
        """
        SELECT
            trim(CAST(entity_id AS STRING)) AS entity_id,
            lower(trim(CAST(metric AS STRING))) AS metric,
            CAST(value AS DOUBLE) AS value,
            to_timestamp(ts) AS event_ts,
            ingested_at,
            ingestion_date,
            source_type,
            source_name,
            batch_id,
            kafka_topic,
            kafka_partition,
            kafka_offset
        FROM bronze_events
        """
    )
    typed.createOrReplaceTempView("typed_events")
    rejected = spark.sql(
        """
        SELECT *, 'missing_or_invalid_required_field' AS rejection_reason
        FROM typed_events
        WHERE entity_id IS NULL OR entity_id = ''
           OR metric IS NULL OR metric = ''
           OR value IS NULL OR event_ts IS NULL
        """
    ).cache()
    silver = spark.sql(
        """
        WITH ranked AS (
            SELECT *,
                row_number() OVER (
                    PARTITION BY entity_id, metric, event_ts
                    ORDER BY ingested_at DESC, batch_id DESC
                ) AS duplicate_rank
            FROM typed_events
            WHERE entity_id IS NOT NULL AND entity_id <> ''
              AND metric IS NOT NULL AND metric <> ''
              AND value IS NOT NULL AND event_ts IS NOT NULL
        )
        SELECT
            entity_id,
            metric,
            value,
            event_ts,
            to_date(event_ts) AS event_date,
            hour(event_ts) AS event_hour,
            ingested_at,
            source_type,
            source_name,
            batch_id,
            kafka_topic,
            kafka_partition,
            kafka_offset
        FROM ranked
        WHERE duplicate_rank = 1
        """
    ).cache()
    (
        silver.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .partitionBy("event_date")
        .save(resolved.uri("silver/events"))
    )
    (
        rejected.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .save(resolved.uri("quarantine/events"))
    )
    counts = (silver.count(), rejected.count())
    silver.unpersist()
    rejected.unpersist()
    return counts


def build_gold_and_processed(
    spark: SparkSession,
    settings: DeltaSettings | None = None,
) -> int:
    resolved = settings or DeltaSettings.from_env()
    silver = spark.read.format("delta").load(resolved.uri("silver/events"))
    silver.createOrReplaceTempView("silver_events")
    gold = spark.sql(
        """
        SELECT
            event_date,
            metric,
            COUNT(*) AS event_count,
            ROUND(SUM(value), 2) AS total_value,
            COUNT(DISTINCT entity_id) AS unique_entities,
            ROUND(AVG(value), 2) AS average_value
        FROM silver_events
        GROUP BY event_date, metric
        ORDER BY event_date, metric
        """
    ).cache()
    (
        gold.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .save(resolved.uri("gold/daily_metrics"))
    )
    (
        silver.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .partitionBy("event_date")
        .save(resolved.uri("processed/events"))
    )
    gold_rows = gold.count()
    gold.unpersist()
    return gold_rows


def write_latest_manifest(
    run: PipelineRun,
    settings: DeltaSettings | None = None,
) -> None:
    resolved = settings or DeltaSettings.from_env()
    create_minio_client(resolved).put_object(
        Bucket=resolved.bucket,
        Key="metadata/latest.json",
        Body=json.dumps(asdict(run), indent=2).encode("utf-8"),
        ContentType="application/json",
    )


def delta_history(
    spark: SparkSession,
    prefix: str,
    settings: DeltaSettings | None = None,
) -> DataFrame:
    resolved = settings or DeltaSettings.from_env()
    return DeltaTable.forPath(spark, resolved.uri(prefix)).history()
