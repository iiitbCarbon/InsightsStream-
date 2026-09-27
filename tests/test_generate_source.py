from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path

import pytest

from producer.generate_source import generate_events


def test_generate_events_creates_requested_rows_deterministically(
    tmp_path: Path,
) -> None:
    first = generate_events(
        tmp_path / "first.csv",
        row_count=25,
        seed=7,
        invalid_rate=0,
        duplicate_rate=0,
        end_time=datetime(2026, 9, 27, tzinfo=timezone.utc),
    )
    second = generate_events(
        tmp_path / "second.csv",
        row_count=25,
        seed=7,
        invalid_rate=0,
        duplicate_rate=0,
        end_time=datetime(2026, 9, 27, tzinfo=timezone.utc),
    )

    with first.open(encoding="utf-8") as source_file:
        rows = list(csv.DictReader(source_file))

    assert len(rows) == 25
    assert rows[0].keys() == {"entity_id", "metric", "value", "ts"}
    assert first.read_text(encoding="utf-8") == second.read_text(encoding="utf-8")


@pytest.mark.parametrize(
    ("argument", "value"),
    [
        ("row_count", 0),
        ("days", 0),
        ("invalid_rate", -0.1),
        ("duplicate_rate", 1.1),
    ],
)
def test_generate_events_rejects_invalid_configuration(
    tmp_path: Path, argument: str, value: int | float
) -> None:
    arguments = {argument: value}

    with pytest.raises(ValueError):
        generate_events(tmp_path / "events.csv", **arguments)
