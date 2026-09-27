"""Batch medallion pipeline used by the CLI and Jupyter notebooks."""

from __future__ import annotations

import io
import json
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from insightsstream.storage import ObjectStore, create_store

REQUIRED_COLUMNS = ("entity_id", "metric", "value", "ts")
LATEST_STATE_KEY = "metadata/latest.json"


@dataclass
class PipelineRun:
    batch_id: str
    source_name: str
    started_at: str
    bronze_key: str | None = None
    silver_key: str | None = None
    quarantine_key: str | None = None
    gold_key: str | None = None
    export_key: str | None = None
    bronze_rows: int = 0
    silver_rows: int = 0
    rejected_rows: int = 0
    gold_rows: int = 0
    completed_at: str | None = None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _dataframe_to_parquet(frame: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    frame.to_parquet(output, index=False, engine="pyarrow")
    return output.getvalue()


def _parquet_to_dataframe(data: bytes) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(data), engine="pyarrow")


def _write_json(store: ObjectStore, key: str, payload: dict[str, Any]) -> None:
    store.put_bytes(
        key,
        json.dumps(payload, indent=2).encode("utf-8"),
        "application/json",
    )


def _read_source(source_path: Path) -> pd.DataFrame:
    suffix = source_path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(source_path)
    if suffix in {".jsonl", ".ndjson"}:
        return pd.read_json(source_path, lines=True)
    if suffix == ".json":
        return pd.read_json(source_path)
    if suffix == ".parquet":
        return pd.read_parquet(source_path, engine="pyarrow")
    raise ValueError(
        f"Unsupported source format '{suffix}'. Use CSV, JSON, JSONL, or Parquet."
    )


def _load_run(store: ObjectStore, batch_id: str | None = None) -> PipelineRun:
    if batch_id is None:
        if not store.exists(LATEST_STATE_KEY):
            raise FileNotFoundError("No pipeline run found. Run source ingestion first.")
        latest = json.loads(store.get_bytes(LATEST_STATE_KEY))
        batch_id = latest["batch_id"]

    key = f"metadata/runs/{batch_id}.json"
    if not store.exists(key):
        raise FileNotFoundError(f"Pipeline metadata not found for batch '{batch_id}'")
    return PipelineRun(**json.loads(store.get_bytes(key)))


def _save_run(store: ObjectStore, run: PipelineRun) -> None:
    payload = asdict(run)
    _write_json(store, f"metadata/runs/{run.batch_id}.json", payload)
    _write_json(
        store,
        LATEST_STATE_KEY,
        {
            "batch_id": run.batch_id,
            "updated_at": _utc_now().isoformat(),
            "completed": run.completed_at is not None,
        },
    )


def ingest_source(
    source_path: str | Path,
    store: ObjectStore | None = None,
    batch_id: str | None = None,
) -> PipelineRun:
    """Land source data in bronze without changing business fields."""
    object_store = store or create_store()
    path = Path(source_path)
    if not path.is_file():
        raise FileNotFoundError(f"Source file does not exist: {path}")

    frame = _read_source(path)
    normalized_columns = {column: str(column).strip().lower() for column in frame.columns}
    frame = frame.rename(columns=normalized_columns)
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError("Source is missing required columns: " + ", ".join(missing))

    now = _utc_now()
    resolved_batch_id = batch_id or f"{now:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"
    frame["ingested_at"] = now
    frame["source_file"] = path.name
    frame["batch_id"] = resolved_batch_id
    bronze_key = (
        f"bronze/events/ingestion_date={now:%Y-%m-%d}/"
        f"batch_id={resolved_batch_id}/events.parquet"
    )
    object_store.put_bytes(
        bronze_key,
        _dataframe_to_parquet(frame),
        "application/vnd.apache.parquet",
    )

    run = PipelineRun(
        batch_id=resolved_batch_id,
        source_name=path.name,
        started_at=now.isoformat(),
        bronze_key=bronze_key,
        bronze_rows=len(frame),
    )
    _save_run(object_store, run)
    return run


def transform_silver(
    batch_id: str | None = None,
    store: ObjectStore | None = None,
) -> PipelineRun:
    """Validate, type, deduplicate, and quarantine bronze events."""
    object_store = store or create_store()
    run = _load_run(object_store, batch_id)
    if not run.bronze_key:
        raise ValueError(f"Batch '{run.batch_id}' has no bronze object")

    frame = _parquet_to_dataframe(object_store.get_bytes(run.bronze_key))
    frame["entity_id"] = frame["entity_id"].astype("string").str.strip()
    frame["metric"] = frame["metric"].astype("string").str.strip().str.lower()
    frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
    frame["event_ts"] = pd.to_datetime(frame["ts"], errors="coerce", utc=True)

    invalid = (
        frame["entity_id"].isna()
        | frame["entity_id"].eq("")
        | frame["metric"].isna()
        | frame["metric"].eq("")
        | frame["value"].isna()
        | frame["event_ts"].isna()
    )
    rejected = frame.loc[invalid].copy()
    rejected["rejection_reason"] = "missing_or_invalid_required_field"

    valid = frame.loc[~invalid].copy()
    valid = valid.drop_duplicates(
        subset=["entity_id", "metric", "event_ts"], keep="last"
    )
    valid["event_date"] = valid["event_ts"].dt.date
    valid["event_hour"] = valid["event_ts"].dt.hour.astype("int16")
    valid = valid.drop(columns=["ts"]).sort_values("event_ts").reset_index(drop=True)

    silver_key = (
        f"silver/events/batch_id={run.batch_id}/events_clean.parquet"
    )
    object_store.put_bytes(
        silver_key,
        _dataframe_to_parquet(valid),
        "application/vnd.apache.parquet",
    )

    quarantine_key = None
    if not rejected.empty:
        quarantine_key = (
            f"quarantine/events/batch_id={run.batch_id}/rejected.parquet"
        )
        object_store.put_bytes(
            quarantine_key,
            _dataframe_to_parquet(rejected),
            "application/vnd.apache.parquet",
        )

    run.silver_key = silver_key
    run.quarantine_key = quarantine_key
    run.silver_rows = len(valid)
    run.rejected_rows = len(rejected)
    _save_run(object_store, run)
    return run


def build_gold(
    batch_id: str | None = None,
    store: ObjectStore | None = None,
) -> PipelineRun:
    """Aggregate silver events and publish processed data for consumers."""
    object_store = store or create_store()
    run = _load_run(object_store, batch_id)
    if not run.silver_key:
        raise ValueError(f"Batch '{run.batch_id}' has not completed silver processing")

    frame = _parquet_to_dataframe(object_store.get_bytes(run.silver_key))
    gold = (
        frame.groupby(["event_date", "metric"], as_index=False, dropna=False)
        .agg(
            event_count=("entity_id", "size"),
            total_value=("value", "sum"),
            unique_entities=("entity_id", "nunique"),
            average_value=("value", "mean"),
        )
        .sort_values(["event_date", "metric"])
        .reset_index(drop=True)
    )
    gold["average_value"] = gold["average_value"].round(2)

    gold_key = (
        f"gold/daily_metrics/batch_id={run.batch_id}/daily_metrics.parquet"
    )
    object_store.put_bytes(
        gold_key,
        _dataframe_to_parquet(gold),
        "application/vnd.apache.parquet",
    )

    export_key = f"processed/batch_id={run.batch_id}/events_processed.csv"
    object_store.put_bytes(
        export_key,
        frame.to_csv(index=False).encode("utf-8"),
        "text/csv",
    )

    run.gold_key = gold_key
    run.export_key = export_key
    run.gold_rows = len(gold)
    run.completed_at = _utc_now().isoformat()
    _save_run(object_store, run)
    return run


def run_pipeline(
    source_path: str | Path,
    store: ObjectStore | None = None,
    batch_id: str | None = None,
) -> PipelineRun:
    """Execute bronze, silver, gold, and processed export stages."""
    object_store = store or create_store()
    run = ingest_source(source_path, object_store, batch_id)
    transform_silver(run.batch_id, object_store)
    return build_gold(run.batch_id, object_store)


def load_latest_gold(store: ObjectStore | None = None) -> tuple[pd.DataFrame, PipelineRun]:
    object_store = store or create_store()
    run = _load_run(object_store)
    if not run.gold_key:
        raise ValueError("The latest run has no gold output")
    return _parquet_to_dataframe(object_store.get_bytes(run.gold_key)), run
