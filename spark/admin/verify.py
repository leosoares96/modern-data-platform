"""Assertions on the lakehouse state. scripts/demo.sh uses them as the end-to-end test.

  verify.py bronze --events 400

Exits with a non-zero status on the first failed check and prints a GitHub Actions error annotation.
"""

from __future__ import annotations

import argparse
import sys

from lakehouse.session import get_spark


def check(condition: bool, message: str) -> None:
    if not condition:
        print(f"::error::FAILED: {message}", flush=True)
        sys.exit(1)
    print(f"ok: {message}", flush=True)


def count(spark, table: str) -> int:
    return spark.table(table).count()


def verify_bronze(spark, events: int) -> None:
    valid = count(spark, "lake.bronze.flights_raw")
    dead = count(spark, "lake.bronze.flights_dead_letter")
    print(f"bronze.flights_raw={valid} bronze.flights_dead_letter={dead} produced={events}")

    check(valid + dead == events, f"every message is in bronze ({valid} + {dead} = {events})")
    check(valid > 0, "bronze has valid events")
    check(dead > 0, "malformed payloads reached the dead-letter table")

    dead_rows = spark.table("lake.bronze.flights_dead_letter").select("reason").distinct()
    reasons = {row["reason"] for row in dead_rows.collect()}
    print(f"dead-letter reasons: {sorted(reasons)}")
    allowed = {
        "null_payload",
        "unparseable_json",
        "missing_or_invalid_fields",
        "unsupported_schema_version",
        "invalid_event_ts",
    }
    check(reasons <= allowed, "dead-letter reasons are all known")

    undeclared = spark.sql(
        "SELECT count(*) AS n FROM lake.bronze.flights_raw WHERE event_id IS NULL OR icao24 IS NULL"
    ).first()["n"]
    check(undeclared == 0, "bronze rows never miss event_id or icao24")


STAGES = {"bronze": verify_bronze}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("stage", choices=sorted(STAGES))
    parser.add_argument("--events", type=int, default=0, help="messages the producer sent")
    args = parser.parse_args()

    spark = get_spark(f"verify-{args.stage}")
    STAGES[args.stage](spark, args.events)
    spark.stop()


if __name__ == "__main__":
    main()
