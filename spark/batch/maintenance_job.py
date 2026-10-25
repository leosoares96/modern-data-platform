"""Iceberg table maintenance: compaction, snapshot expiry and orphan file removal.

Streaming and frequent merges create many small files and a snapshot per commit. This job keeps the
tables healthy:

  rewrite_data_files   merge small files into larger ones (compaction)
  expire_snapshots     forget snapshots older than the cutoff, always keeping the latest few
  remove_orphan_files  delete files no snapshot references (left behind by failed writes)

Expiring snapshots limits time travel to what is kept, so --older-than-hours is a retention policy.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from lakehouse.ddl import ensure_lakehouse
from lakehouse.session import get_spark

TABLES = [
    "bronze.flights_raw",
    "bronze.flights_dead_letter",
    "silver.flights",
    "silver.flights_quarantine",
    "gold.flights_hourly",
    "gold.activity_grid",
    "quality.check_results",
]


def show(label: str, table: str, rows) -> None:
    for row in rows:
        print(f"maintenance {label} {table}: {row.asDict()}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--retain-last", type=int, default=5, help="snapshots always kept")
    parser.add_argument("--older-than-hours", type=int, default=24, help="retention window")
    parser.add_argument("--min-input-files", type=int, default=2, help="compaction threshold")
    args = parser.parse_args()

    spark = get_spark("maintenance")
    ensure_lakehouse(spark)

    cutoff = datetime.now(timezone.utc) - timedelta(hours=args.older_than_hours)
    cutoff_literal = f"TIMESTAMP '{cutoff:%Y-%m-%d %H:%M:%S}'"

    for table in TABLES:
        rewritten = spark.sql(
            f"CALL lake.system.rewrite_data_files(table => '{table}', "
            f"options => map('min-input-files', '{args.min_input_files}'))"
        ).collect()
        show("compact", table, rewritten)

        expired = spark.sql(
            f"CALL lake.system.expire_snapshots(table => '{table}', "
            f"older_than => {cutoff_literal}, retain_last => {args.retain_last})"
        ).collect()
        show("expire", table, expired)

        orphans = spark.sql(
            f"CALL lake.system.remove_orphan_files("
            f"table => '{table}', older_than => {cutoff_literal})"
        ).collect()
        print(f"maintenance orphans {table}: removed={len(orphans)}", flush=True)

    print("MAINTENANCE OK", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
