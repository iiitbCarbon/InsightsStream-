"""PySpark implementation of the InsightsStream medallion pipeline."""

from __future__ import annotations

import csv
import io
import json
import shutil
import tempfile
import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
)

from insightsstream.pipeline import LATEST_STATE_KEY, PipelineRun
from insightsstream.storage import ObjectStore, create_store

REQUIRED_COLUMNS = ("entity_id", "metric", "value", "ts")
EVENT_SCHEMA = StructType(
    [
        StructField("entity_id", StringType(), True),
        StructField("metric", StringType(), True),
        StructField("value", StringType(), True),
        StructField("ts", StringType(), True),
    ]
)


def create_spark_session(app_name: str = "InsightsStream-Lakehouse") -> SparkSession:
    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def _write_json(store: ObjectStore, key: str, payload: dict) -> None:
    store.put_bytes(
        key,
        json.dumps(payload, indent=2).encode("utf-8"),
        "application/json",
    )


def _save_run(store: ObjectStore, run: PipelineRun) -> None:
    _write_json(store, f"metadata/runs/{run.batch_id}.json", asdict(run))
    _write_json(
        store,
        LATEST_STATE_KEY,
        {
            "batch_id": run.batch_id,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "completed": run.completed_at is not None,
        },
    )


def load_run(store: ObjectStore, batch_id: str | None = None) -> PipelineRun:
    if batch_id is None:
        if not store.exists(LATEST_STATE_KEY):
            raise FileNotFoundError("No pipeline run found. Run bronze ingestion first.")
        batch_id = json.loads(store.get_bytes(LATEST_STATE_KEY))["batch_id"]

    key = f"metadata/runs/{batch_id}.json"
    if not store.exists(key):
        raise FileNotFoundError(f"Pipeline metadata not found for batch '{batch_id}'")
    return PipelineRun(**json.loads(store.get_bytes(key)))


def _arrow_type(data_type):
    if isinstance(data_type, StringType):
        return pa.string()
    if isinstance(data_type, DoubleType):
        return pa.float64()
    if isinstance(data_type, IntegerType):
        return pa.int32()
    if isinstance(data_type, LongType):
        return pa.int64()
    if isinstance(data_type, DateType):
        return pa.date32()
    if isinstance(data_type, (TimestampType, TimestampNTZType)):
        return pa.timestamp("us")
    raise TypeError(f"Unsupported Spark type for object export: {data_type}")


def write_parquet_object(frame: DataFrame, store: ObjectStore, key: str) -> None:
    schema = pa.schema(
        [
            pa.field(field.name, _arrow_type(field.dataType), field.nullable)
            for field in frame.schema.fields
        ]
    )
    records = [row.asDict(recursive=True) for row in frame.toLocalIterator()]
    table = pa.Table.from_pylist(records, schema=schema)
    output = io.BytesIO()
    pq.write_table(table, output)
    store.put_bytes(
        key,
        output.getvalue(),
        "application/vnd.apache.parquet",
    )


def read_parquet_object(
    spark: SparkSession, store: ObjectStore, key: str
) -> DataFrame:
    temp_dir = Path(tempfile.mkdtemp(prefix="insightsstream-spark-"))
    source = temp_dir / "data.parquet"
    source.write_bytes(store.get_bytes(key))
    frame = spark.read.parquet(str(source)).cache()
    frame.count()
    shutil.rmtree(temp_dir)
    return frame


def write_csv_object(frame: DataFrame, store: ObjectStore, key: str) -> None:
    output = io.StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow(frame.columns)
    for row in frame.toLocalIterator():
        writer.writerow(row)
    store.put_bytes(key, output.getvalue().encode("utf-8"), "text/csv")


def read_source(spark: SparkSession, source_path: str | Path) -> DataFrame:
    path = Path(source_path)
    if not path.is_file():
        raise FileNotFoundError(f"Source file does not exist: {path}")

    suffix = path.suffix.lower()
    if suffix == ".csv":
        frame = spark.read.option("header", True).schema(EVENT_SCHEMA).csv(str(path))
    elif suffix in {".json", ".jsonl", ".ndjson"}:
        frame = spark.read.schema(EVENT_SCHEMA).json(str(path))
    elif suffix == ".parquet":
        frame = spark.read.parquet(str(path))
    else:
        raise ValueError(
            f"Unsupported source format '{suffix}'. Use CSV, JSON, JSONL, or Parquet."
        )

    frame = frame.select(
        *[F.col(column).alias(column.strip().lower()) for column in frame.columns]
    )
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError("Source is missing required columns: " + ", ".join(missing))
    return frame


def ingest_dataframe_as_bronze(
    frame: DataFrame,
    source_name: str,
    store: ObjectStore | None = None,
    batch_id: str | None = None,
) -> PipelineRun:
    object_store = store or create_store()
    now = datetime.now(timezone.utc)
    resolved_batch_id = batch_id or f"{now:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"
    bronze = (
        frame.select(*REQUIRED_COLUMNS)
        .withColumn("ingested_at", F.lit(now).cast(TimestampType()))
        .withColumn("source_file", F.lit(source_name))
        .withColumn("batch_id", F.lit(resolved_batch_id))
        .cache()
    )
    row_count = bronze.count()
    bronze_key = (
        f"bronze/events/ingestion_date={now:%Y-%m-%d}/"
        f"batch_id={resolved_batch_id}/events.parquet"
    )
    write_parquet_object(bronze, object_store, bronze_key)

    run = PipelineRun(
        batch_id=resolved_batch_id,
        source_name=source_name,
        started_at=now.isoformat(),
        bronze_key=bronze_key,
        bronze_rows=row_count,
    )
    _save_run(object_store, run)
    bronze.unpersist()
    return run


def ingest_source(
    source_path: str | Path,
    spark: SparkSession | None = None,
    store: ObjectStore | None = None,
    batch_id: str | None = None,
) -> PipelineRun:
    resolved_spark = spark or create_spark_session("InsightsStream-Bronze")
    frame = read_source(resolved_spark, source_path)
    return ingest_dataframe_as_bronze(
        frame,
        source_name=Path(source_path).name,
        store=store,
        batch_id=batch_id,
    )


def transform_silver(
    batch_id: str | None = None,
    spark: SparkSession | None = None,
    store: ObjectStore | None = None,
) -> PipelineRun:
    resolved_spark = spark or create_spark_session("InsightsStream-Silver")
    object_store = store or create_store()
    run = load_run(object_store, batch_id)
    if not run.bronze_key:
        raise ValueError(f"Batch '{run.batch_id}' has no bronze object")

    bronze = read_parquet_object(resolved_spark, object_store, run.bronze_key)
    bronze.createOrReplaceTempView("bronze_events")

    typed = resolved_spark.sql(
        """
        SELECT
            trim(CAST(entity_id AS STRING)) AS entity_id,
            lower(trim(CAST(metric AS STRING))) AS metric,
            CAST(value AS DOUBLE) AS value,
            to_timestamp(ts) AS event_ts,
            ingested_at,
            source_file,
            batch_id
        FROM bronze_events
        """
    ).cache()
    typed.createOrReplaceTempView("typed_events")

    rejected = resolved_spark.sql(
        """
        SELECT *, 'missing_or_invalid_required_field' AS rejection_reason
        FROM typed_events
        WHERE entity_id IS NULL OR entity_id = ''
           OR metric IS NULL OR metric = ''
           OR value IS NULL OR event_ts IS NULL
        """
    ).cache()
    valid = resolved_spark.sql(
        """
        WITH ranked AS (
            SELECT *,
                row_number() OVER (
                    PARTITION BY entity_id, metric, event_ts
                    ORDER BY ingested_at DESC
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
            source_file,
            batch_id
        FROM ranked
        WHERE duplicate_rank = 1
        """
    ).cache()

    silver_key = f"silver/events/batch_id={run.batch_id}/events_clean.parquet"
    write_parquet_object(valid, object_store, silver_key)

    rejected_rows = rejected.count()
    quarantine_key = None
    if rejected_rows:
        quarantine_key = (
            f"quarantine/events/batch_id={run.batch_id}/rejected.parquet"
        )
        write_parquet_object(rejected, object_store, quarantine_key)

    run.silver_key = silver_key
    run.quarantine_key = quarantine_key
    run.silver_rows = valid.count()
    run.rejected_rows = rejected_rows
    _save_run(object_store, run)
    bronze.unpersist()
    typed.unpersist()
    valid.unpersist()
    rejected.unpersist()
    return run


def build_gold(
    batch_id: str | None = None,
    spark: SparkSession | None = None,
    store: ObjectStore | None = None,
) -> PipelineRun:
    resolved_spark = spark or create_spark_session("InsightsStream-Gold")
    object_store = store or create_store()
    run = load_run(object_store, batch_id)
    if not run.silver_key:
        raise ValueError(f"Batch '{run.batch_id}' has not completed silver processing")

    silver = read_parquet_object(resolved_spark, object_store, run.silver_key)
    silver.createOrReplaceTempView("silver_events")
    gold = resolved_spark.sql(
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

    gold_key = (
        f"gold/daily_metrics/batch_id={run.batch_id}/daily_metrics.parquet"
    )
    export_key = f"processed/batch_id={run.batch_id}/events_processed.csv"
    write_parquet_object(gold, object_store, gold_key)
    write_csv_object(silver.orderBy("event_ts"), object_store, export_key)

    run.gold_key = gold_key
    run.export_key = export_key
    run.gold_rows = gold.count()
    run.completed_at = datetime.now(timezone.utc).isoformat()
    _save_run(object_store, run)
    silver.unpersist()
    gold.unpersist()
    return run


def load_latest_gold(
    spark: SparkSession | None = None,
    store: ObjectStore | None = None,
) -> tuple[DataFrame, PipelineRun]:
    resolved_spark = spark or create_spark_session("InsightsStream-Gold-Explore")
    object_store = store or create_store()
    run = load_run(object_store)
    if not run.gold_key:
        raise ValueError("The latest run has no gold output")
    return read_parquet_object(resolved_spark, object_store, run.gold_key), run


def run_pipeline(
    source_path: str | Path,
    spark: SparkSession | None = None,
    store: ObjectStore | None = None,
    batch_id: str | None = None,
) -> PipelineRun:
    resolved_spark = spark or create_spark_session()
    object_store = store or create_store()
    run = ingest_source(source_path, resolved_spark, object_store, batch_id)
    transform_silver(run.batch_id, resolved_spark, object_store)
    return build_gold(run.batch_id, resolved_spark, object_store)
