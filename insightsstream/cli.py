"""Command-line entry point for the medallion pipeline."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict

from insightsstream.pipeline import run_pipeline as run_pandas_pipeline
from insightsstream.spark_pipeline import run_pipeline as run_spark_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the InsightsStream ETL pipeline")
    parser.add_argument("source", help="CSV, JSON, JSONL, or Parquet source file")
    parser.add_argument("--batch-id", help="Optional deterministic batch identifier")
    parser.add_argument(
        "--engine",
        choices=("spark", "pandas"),
        default="spark",
        help="Execution engine (default: spark)",
    )
    args = parser.parse_args()

    pipeline = run_spark_pipeline if args.engine == "spark" else run_pandas_pipeline
    run = pipeline(args.source, batch_id=args.batch_id)
    print(json.dumps(asdict(run), indent=2))


if __name__ == "__main__":
    main()
