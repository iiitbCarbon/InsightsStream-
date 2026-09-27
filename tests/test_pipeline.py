from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from insightsstream.config import PROJECT_ROOT, Settings
from insightsstream.pipeline import run_pipeline
from insightsstream.storage import LocalObjectStore


def test_pipeline_builds_medallion_layers_and_processed_export(tmp_path: Path) -> None:
    source = tmp_path / "events.csv"
    pd.DataFrame(
        [
            {
                "entity_id": "user-1",
                "metric": "Clicks",
                "value": 1,
                "ts": "2026-09-27T10:00:00Z",
            },
            {
                "entity_id": "user-1",
                "metric": "Clicks",
                "value": 1,
                "ts": "2026-09-27T10:00:00Z",
            },
            {
                "entity_id": "",
                "metric": "clicks",
                "value": "invalid",
                "ts": "bad-date",
            },
        ]
    ).to_csv(source, index=False)
    store = LocalObjectStore(tmp_path / "lake")

    run = run_pipeline(source, store=store, batch_id="test-batch")

    assert run.bronze_rows == 3
    assert run.silver_rows == 1
    assert run.rejected_rows == 1
    assert run.gold_rows == 1
    assert run.completed_at is not None
    assert all(
        store.exists(key)
        for key in [
            run.bronze_key,
            run.silver_key,
            run.quarantine_key,
            run.gold_key,
            run.export_key,
        ]
        if key is not None
    )

    latest = json.loads(store.get_bytes("metadata/latest.json"))
    assert latest["batch_id"] == "test-batch"
    assert latest["completed"] is True

    processed = pd.read_csv(
        Path(tmp_path / "lake" / run.export_key)
    )
    assert processed.loc[0, "metric"] == "clicks"


def test_default_lake_path_is_independent_of_working_directory(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("INSIGHTS_LOCAL_LAKE_PATH", raising=False)
    monkeypatch.setenv("INSIGHTS_STORAGE_BACKEND", "local")

    settings = Settings.from_env()

    assert settings.local_lake_path == PROJECT_ROOT / "data" / "lakehouse"
