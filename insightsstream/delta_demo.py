"""Generate a source file and run the local MinIO Delta Lake batch path."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import datetime, timezone

from insightsstream.config import PROJECT_ROOT
from insightsstream.delta_pipeline import (
    DeltaSettings,
    build_gold_and_processed,
    build_silver,
    create_delta_spark_session,
    ingest_batch_to_bronze,
    list_lake_objects,
    reset_lake,
    upload_source,
    write_latest_manifest,
)
from producer.generate_source import generate_events


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate data and run the local MinIO Delta Lake pipeline"
    )
    parser.add_argument("--rows", type=int, default=1_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--invalid-rate", type=float, default=0.01)
    parser.add_argument("--duplicate-rate", type=float, default=0.02)
    parser.add_argument(
        "--keep-existing",
        action="store_true",
        help="Append to the existing bronze table instead of resetting the demo lake",
    )
    args = parser.parse_args()

    settings = DeltaSettings.from_env()
    if not args.keep_existing:
        print(f"Reset local lake: removed {reset_lake(settings)} existing objects")

    source_path = PROJECT_ROOT / "data" / "source" / "generated_events.csv"
    generate_events(
        source_path,
        row_count=args.rows,
        seed=args.seed,
        days=args.days,
        invalid_rate=args.invalid_rate,
        duplicate_rate=args.duplicate_rate,
    )
    source_uri = upload_source(source_path, settings)
    print(f"Source CSV: {source_path}")
    print(f"Source object: {source_uri}")

    spark = create_delta_spark_session("InsightsStream-Local-Delta-Demo", settings)
    try:
        run = ingest_batch_to_bronze(spark, source_uri, settings)
        run.silver_rows, run.rejected_rows = build_silver(spark, settings)
        run.gold_rows = build_gold_and_processed(spark, settings)
        run.silver_key = settings.uri("silver/events")
        run.quarantine_key = settings.uri("quarantine/events")
        run.gold_key = settings.uri("gold/daily_metrics")
        run.export_key = settings.uri("processed/events")
        run.completed_at = datetime.now(timezone.utc).isoformat()
        write_latest_manifest(run, settings)
    finally:
        spark.stop()

    print(json.dumps(asdict(run), indent=2))
    print("\nVisible MinIO prefixes:")
    for key in list_lake_objects(settings):
        if key.endswith("_last_checkpoint") or "_delta_log/" in key:
            continue
        print(f"  {key}")


if __name__ == "__main__":
    main()
