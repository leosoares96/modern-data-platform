"""Publish synthetic flight events to a Kafka topic.

Configuration comes from environment variables so the same image works in Compose and in CI.
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import time

from confluent_kafka import KafkaException, Producer

from flight_events import FaultConfig, FlightEventGenerator

log = logging.getLogger("producer")


def build_generator() -> FlightEventGenerator:
    seed = os.getenv("PRODUCER_SEED")
    return FlightEventGenerator(
        fleet_size=int(os.getenv("EVENTS_FLEET_SIZE", "20")),
        seed=int(seed) if seed else None,
        tick_seconds=float(os.getenv("EVENTS_TICK_SECONDS", "1.0")),
        faults=FaultConfig(
            duplicate_rate=float(os.getenv("DUPLICATE_RATE", "0.02")),
            late_rate=float(os.getenv("LATE_RATE", "0.05")),
            malformed_rate=float(os.getenv("MALFORMED_RATE", "0.02")),
        ),
    )


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    topic = os.getenv("KAFKA_TOPIC", "flights.raw")
    max_events = int(os.getenv("PRODUCER_MAX_EVENTS", "0"))  # 0 means run until stopped
    tick_seconds = float(os.getenv("EVENTS_TICK_SECONDS", "1.0"))

    producer = Producer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092"),
            "client.id": "flight-producer",
            "linger.ms": 50,
            "enable.idempotence": True,
        }
    )
    generator = build_generator()
    stats = {"sent": 0, "delivered": 0, "failed": 0}
    stopping = False

    def request_stop(signum, frame) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    def on_delivery(err, msg) -> None:
        if err is not None:
            stats["failed"] += 1
            log.error("delivery failed: %s", err)
        else:
            stats["delivered"] += 1

    log.info("publishing to %s (max_events=%s)", topic, max_events or "unbounded")
    while not stopping and (max_events == 0 or stats["sent"] < max_events):
        for payload in generator.tick_payloads():
            if max_events and stats["sent"] >= max_events:
                break
            try:
                producer.produce(
                    topic,
                    value=payload.value.encode("utf-8"),
                    key=payload.key.encode("utf-8") if payload.key else None,
                    on_delivery=on_delivery,
                )
            except BufferError:
                producer.poll(1.0)  # local queue is full: let deliveries drain, then move on
                continue
            except KafkaException as exc:
                log.error("produce failed: %s", exc)
                stats["failed"] += 1
                continue
            stats["sent"] += 1
        producer.poll(0)
        if tick_seconds > 0:
            time.sleep(tick_seconds)

    remaining = producer.flush(30)
    log.info("done: %s (undelivered=%s)", stats, remaining)
    return 1 if stats["failed"] or remaining else 0


if __name__ == "__main__":
    sys.exit(main())
