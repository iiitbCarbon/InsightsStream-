"""
InsightsStream — Kafka event producer.

Simulates high-volume event ingestion (clickstream / IoT telemetry).
Events are keyed by `entity_id` so Kafka guarantees per-entity ordering
and even partition distribution for parallel downstream consumption.
"""
import argparse
import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "events"
METRICS = ["page_views", "clicks", "purchases", "signups"]


def build_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "linger.ms": 10,          # small batching window for throughput
            "compression.type": "lz4",
            "acks": "1",
        }
    )


def make_event() -> dict:
    return {
        "entity_id": f"user_{random.randint(1, 10_000)}",
        "metric": random.choice(METRICS),
        "value": 1,
        "ts": datetime.now(timezone.utc).isoformat(),
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")


def main(events_per_second: int = 50, duration: float | None = None) -> None:
    if events_per_second < 1:
        raise ValueError("events_per_second must be at least 1")
    if duration is not None and duration <= 0:
        raise ValueError("duration must be greater than 0")

    producer = build_producer()
    interval = 1.0 / events_per_second
    deadline = time.monotonic() + duration if duration is not None else None
    print(f"Producing ~{events_per_second} events/s to '{TOPIC}' ...")
    try:
        while deadline is None or time.monotonic() < deadline:
            event = make_event()
            producer.produce(
                TOPIC,
                key=event["entity_id"],          # keyed partitioning
                value=json.dumps(event).encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("Stopping producer ...")
    finally:
        producer.flush()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Produce dummy events to Kafka")
    parser.add_argument("--events-per-second", type=int, default=50)
    parser.add_argument(
        "--duration",
        type=float,
        help="Stop after this many seconds; omit to run until Ctrl+C",
    )
    arguments = parser.parse_args()
    main(arguments.events_per_second, arguments.duration)
