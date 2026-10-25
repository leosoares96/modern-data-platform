"""Assertions on the lakehouse state. scripts/demo.sh uses them as the end-to-end test.

  verify.py bronze --events 400

Exits with a non-zero status on the first failed check and prints a GitHub Actions error annotation.
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import functions as F

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


def verify_silver(spark, events: int) -> None:
    bronze_rows = count(spark, "lake.bronze.flights_raw")
    bronze_ids = spark.table("lake.bronze.flights_raw").select("event_id").distinct().count()
    silver = count(spark, "lake.silver.flights")
    silver_ids = spark.table("lake.silver.flights").select("event_id").distinct().count()
    quarantine = count(spark, "lake.silver.flights_quarantine")
    print(
        f"bronze rows={bronze_rows} distinct ids={bronze_ids} | "
        f"silver={silver} quarantine={quarantine}"
    )

    check(bronze_rows > bronze_ids, "the producer injected duplicates into bronze")
    check(silver == silver_ids, "silver holds each event_id exactly once")
    check(
        silver + quarantine == bronze_ids,
        f"every distinct bronze event is in silver or quarantine ({silver} + {quarantine})",
    )
    check(quarantine > 0, "events that break a business rule were quarantined")

    late = spark.table("lake.silver.flights").filter("is_late").count()
    check(late > 0, f"late arrivals are flagged in silver ({late})")

    reasons = spark.table("lake.silver.flights_quarantine").select("reasons").distinct().collect()
    print(f"quarantine reasons: {sorted(row['reasons'] for row in reasons)}")

    checks = spark.table("lake.quality.check_results").filter("job = 'silver_flights'")
    check(checks.count() > 0, "quality results were persisted")
    failed = checks.filter("NOT passed AND severity = 'error'").count()
    check(failed == 0, "no error-severity quality check failed")


def verify_gold(spark, events: int) -> None:
    silver = count(spark, "lake.silver.flights")
    hourly = spark.table("lake.gold.flights_hourly")
    grid = spark.table("lake.gold.activity_grid")
    hourly_observations = hourly.agg(F.sum("observations")).first()[0]
    grid_observations = grid.agg(F.sum("observations")).first()[0]
    print(
        f"silver={silver} gold hourly rows={hourly.count()} observations={hourly_observations} "
        f"| grid rows={grid.count()} observations={grid_observations}"
    )

    check(hourly.count() > 0, "gold has hourly aggregates")
    check(hourly_observations == silver, "hourly observations add up to the silver rows")
    check(grid_observations == silver, "grid observations add up to the silver rows")
    check(
        hourly.filter("flights > observations").count() == 0, "no hour has more flights than rows"
    )
    check(
        hourly.filter("pct_on_ground < 0 OR pct_on_ground > 100").count() == 0,
        "percentage of aircraft on the ground stays within 0 to 100",
    )
    checks = spark.table("lake.quality.check_results").filter("job = 'gold_flights'")
    check(checks.count() > 0, "gold quality results were persisted")
    check(checks.filter("NOT passed AND severity = 'error'").count() == 0, "gold checks passed")


DATA_TABLES = [
    "lake.bronze.flights_raw",
    "lake.bronze.flights_dead_letter",
    "lake.silver.flights",
    "lake.silver.flights_quarantine",
    "lake.gold.flights_hourly",
    "lake.gold.activity_grid",
]


def print_counts(spark, events: int) -> None:
    """One `count <table>=<rows>` line per table, so the demo can compare two runs."""
    for table in DATA_TABLES:
        print(f"count {table}={count(spark, table)}", flush=True)


STAGES = {
    "bronze": verify_bronze,
    "silver": verify_silver,
    "gold": verify_gold,
    "counts": print_counts,
}


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
