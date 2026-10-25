"""Silver batch job: bronze -> silver.

Reads the bronze rows that arrived since the last run, applies the business rules and writes:
  lake.silver.flights             clean, deduplicated, enriched events (MERGE INTO, idempotent)
  lake.silver.flights_quarantine  events that broke a rule, with the reasons

The job is safe to re-run: MERGE INTO only inserts events whose event_id is not in silver yet, and
the read window overlaps the previous run by two minutes so a late bronze commit is never missed.
Quality checks run last and fail the job (so Airflow notices) when an error-severity check fails.
"""

from __future__ import annotations

import argparse
import uuid

from lakehouse.ddl import ensure_lakehouse
from lakehouse.quality import (
    Freshness,
    InRange,
    MinRows,
    NotNull,
    Unique,
    assert_no_errors,
    evaluate,
    persist_results,
)
from lakehouse.session import get_spark
from lakehouse.transforms import dedupe_events, split_by_rules, to_quarantine, to_silver

JOB = "silver_flights"
BRONZE = "lake.bronze.flights_raw"
SILVER = "lake.silver.flights"
QUARANTINE = "lake.silver.flights_quarantine"
WATERMARKS = "lake.ops.watermarks"
OVERLAP = "2 MINUTES"

SILVER_CHECKS = [
    NotNull("event_id"),
    NotNull("icao24"),
    NotNull("event_ts"),
    NotNull("airline"),
    Unique("event_id"),
    InRange("latitude", -90, 90),
    InRange("longitude", -180, 180),
    InRange("baro_altitude_m", -500, 20_000),
    InRange("velocity_ms", 0, 400),
    MinRows(1),
    Freshness("event_ts", max_age_minutes=60),
]


def read_new_bronze(spark, full_refresh: bool):
    if full_refresh:
        return spark.table(BRONZE)
    return spark.sql(
        f"""
        SELECT * FROM {BRONZE}
        WHERE ingested_at > COALESCE(
            (SELECT watermark FROM {WATERMARKS} WHERE job = '{JOB}'),
            TIMESTAMP '1970-01-01 00:00:00'
        ) - INTERVAL {OVERLAP}
        """
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--full-refresh", action="store_true", help="ignore the watermark")
    args = parser.parse_args()

    spark = get_spark("silver-flights")
    ensure_lakehouse(spark)
    run_id = str(uuid.uuid4())

    new_rows = read_new_bronze(spark, args.full_refresh).persist()
    read_count = new_rows.count()
    if read_count == 0:
        print("silver: nothing new in bronze", flush=True)
        spark.stop()
        return
    new_rows.createOrReplaceTempView("silver_new_rows")

    clean, rejected = split_by_rules(new_rows)
    updates = to_silver(dedupe_events(clean)).persist()
    quarantined = to_quarantine(dedupe_events(rejected)).persist()
    updates.createOrReplaceTempView("silver_updates")
    quarantined.createOrReplaceTempView("quarantine_updates")

    silver_before = spark.table(SILVER).count()
    quarantine_before = spark.table(QUARANTINE).count()

    spark.sql(
        f"""
        MERGE INTO {SILVER} AS t
        USING silver_updates AS s
        ON t.event_id = s.event_id AND t.event_ts = s.event_ts
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    spark.sql(
        f"""
        MERGE INTO {QUARANTINE} AS t
        USING quarantine_updates AS s
        ON t.event_id = s.event_id AND t.event_ts = s.event_ts
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    # Advance the watermark only after both merges succeeded.
    spark.sql(
        f"""
        MERGE INTO {WATERMARKS} AS t
        USING (
            SELECT '{JOB}' AS job, max(ingested_at) AS watermark, current_timestamp() AS updated_at
            FROM silver_new_rows
        ) AS s
        ON t.job = s.job
        WHEN MATCHED THEN UPDATE SET t.watermark = s.watermark, t.updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    silver_after = spark.table(SILVER).count()
    quarantine_after = spark.table(QUARANTINE).count()
    print(
        f"silver: read={read_count} inserted={silver_after - silver_before} "
        f"quarantined={quarantine_after - quarantine_before} "
        f"silver_total={silver_after} quarantine_total={quarantine_after}",
        flush=True,
    )

    results = evaluate(spark.table(SILVER), "silver.flights", SILVER_CHECKS)
    persist_results(spark, run_id, JOB, results)
    for result in results:
        status = "PASS" if result.passed else f"FAIL[{result.severity}]"
        print(f"quality {status} {result.check_name} observed={result.observed}", flush=True)
    assert_no_errors(results)
    spark.stop()


if __name__ == "__main__":
    main()
