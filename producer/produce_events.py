"""
InsightsStream — Kafka event producer.

Simulates high-volume event ingestion (clickstream / IoT telemetry).
Events are keyed by `entity_id` so Kafka guarantees per-entity ordering
and even partition distribution for parallel downstream consumption.
"""
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


def main(events_per_second: int = 50) -> None:
    producer = build_producer()
    interval = 1.0 / events_per_second
    print(f"Producing ~{events_per_second} events/s to '{TOPIC}' ...")
    try:
        while True:
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
    main()
