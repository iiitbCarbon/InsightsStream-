"""Print the local MinIO lake layout for an interview walkthrough."""

from __future__ import annotations

from collections import defaultdict

from insightsstream.delta_pipeline import DeltaSettings, list_lake_objects


def main() -> None:
    settings = DeltaSettings.from_env()
    grouped: dict[str, list[str]] = defaultdict(list)
    for key in list_lake_objects(settings):
        grouped[key.split("/", 1)[0]].append(key)

    print(f"MinIO bucket: {settings.bucket}")
    for layer in (
        "source",
        "bronze",
        "silver",
        "quarantine",
        "gold",
        "processed",
        "metadata",
        "checkpoints",
    ):
        objects = grouped.get(layer, [])
        print(f"\n{layer.upper()} ({len(objects)} objects)")
        for key in objects[:5]:
            print(f"  {key}")
        if len(objects) > 5:
            print(f"  ... {len(objects) - 5} more")


if __name__ == "__main__":
    main()
