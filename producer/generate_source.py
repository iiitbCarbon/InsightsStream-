"""Generate realistic local source events for the batch ETL demo."""

from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

METRICS = ("page_views", "clicks", "purchases", "signups")
METRIC_WEIGHTS = (0.55, 0.30, 0.05, 0.10)


def generate_events(
    output_path: str | Path,
    row_count: int = 1_000,
    seed: int = 42,
    days: int = 7,
    invalid_rate: float = 0.01,
    duplicate_rate: float = 0.02,
    end_time: datetime | None = None,
) -> Path:
    """Create a deterministic CSV source with optional bad and duplicate rows."""
    if row_count < 1:
        raise ValueError("row_count must be at least 1")
    if days < 1:
        raise ValueError("days must be at least 1")
    if not 0 <= invalid_rate <= 1:
        raise ValueError("invalid_rate must be between 0 and 1")
    if not 0 <= duplicate_rate <= 1:
        raise ValueError("duplicate_rate must be between 0 and 1")

    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    randomizer = random.Random(seed)
    now = end_time or datetime.now(timezone.utc)
    window_seconds = days * 24 * 60 * 60
    events: list[dict[str, str | int | float]] = []

    for _ in range(row_count):
        if events and randomizer.random() < duplicate_rate:
            events.append(events[randomizer.randrange(len(events))].copy())
            continue

        metric = randomizer.choices(METRICS, weights=METRIC_WEIGHTS, k=1)[0]
        event_time = now - timedelta(
            seconds=randomizer.randint(0, window_seconds)
        )
        value: int | float = (
            round(randomizer.uniform(5, 250), 2) if metric == "purchases" else 1
        )
        event: dict[str, str | int | float] = {
            "entity_id": f"user_{randomizer.randint(1, 500):04d}",
            "metric": metric,
            "value": value,
            "ts": event_time.isoformat(),
        }
        if randomizer.random() < invalid_rate:
            event[randomizer.choice(("entity_id", "metric", "value", "ts"))] = ""
        events.append(event)

    with destination.open("w", newline="", encoding="utf-8") as source_file:
        writer = csv.DictWriter(
            source_file,
            fieldnames=["entity_id", "metric", "value", "ts"],
        )
        writer.writeheader()
        writer.writerows(events)

    return destination
