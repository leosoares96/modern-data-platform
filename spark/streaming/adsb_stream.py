"""Bronze streaming job: Kafka -> Iceberg.

Reads flight events from Kafka and appends them to two tables:
  lake.bronze.flights_raw          parsed events
  lake.bronze.flights_dead_letter  payloads that could not be parsed, with the reason

Delivery is at least once. If a micro-batch is retried after a failure it can append the same rows
again, and silver removes the duplicates by event_id (see docs/decisions/0006).

  --once   process everything currently in Kafka, then stop (used by CI and for backfills)
"""

from __future__ import annotations

import argparse
import os

from lakehouse.ddl import ensure_lakehouse
from lakehouse.session import get_spark
from lakehouse.transforms import parse_kafka_batch

RAW_TABLE = "lake.bronze.flights_raw"
DEAD_LETTER_TABLE = "lake.bronze.flights_dead_letter"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--once", action="store_true", help="drain Kafka and stop")
    parser.add_argument("--trigger-seconds", type=int, default=10)
    parser.add_argument("--starting-offsets", default="earliest", choices=["earliest", "latest"])
    parser.add_argument("--max-offsets-per-trigger", type=int, default=5000)
    parser.add_argument(
        "--checkpoint",
        default=os.getenv("BRONZE_CHECKPOINT", "/opt/platform/checkpoints/bronze_flights"),
        help="local path: the image has no S3A connector, so checkpoints cannot live on S3",
    )
    return parser.parse_args()


def write_batch(batch, batch_id: int) -> None:
    batch.persist()
    try:
        valid, dead = parse_kafka_batch(batch)
        valid_count = valid.count()
        dead_count = dead.count()
        if valid_count:
            valid.writeTo(RAW_TABLE).append()
        if dead_count:
            dead.writeTo(DEAD_LETTER_TABLE).append()
        print(f"batch {batch_id}: valid={valid_count} dead_letter={dead_count}", flush=True)
    finally:
        batch.unpersist()


def main() -> None:
    args = parse_args()
    spark = get_spark("bronze-flights-stream")
    ensure_lakehouse(spark)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092"))
        .option("subscribe", os.getenv("KAFKA_TOPIC", "flights.raw"))
        .option("startingOffsets", args.starting_offsets)
        .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
        .option("failOnDataLoss", "false")
        .load()
    )

    writer = raw.writeStream.foreachBatch(write_batch).option("checkpointLocation", args.checkpoint)
    if args.once:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=f"{args.trigger_seconds} seconds")

    query = writer.start()
    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()
