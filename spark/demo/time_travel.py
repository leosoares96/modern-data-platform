"""Iceberg showcase: time travel, rollback, schema evolution and partition evolution.

Everything happens on a scratch table copied from silver, so the real tables are never touched.
Each step asserts what it claims, which makes the demo a test as well as a walkthrough.
"""

from __future__ import annotations

from lakehouse.ddl import ensure_lakehouse
from lakehouse.session import get_spark

TABLE = "lake.ops.time_travel_demo"


def count(spark, sql: str) -> int:
    return spark.sql(sql).first()[0]


def main() -> None:
    spark = get_spark("time-travel-demo")
    ensure_lakehouse(spark)
    spark.sql(f"DROP TABLE IF EXISTS {TABLE}")

    print("\n1. Copy 200 rows of silver into a scratch table")
    spark.sql(
        f"""
        CREATE TABLE {TABLE} USING iceberg
        TBLPROPERTIES ('format-version' = '2')
        AS SELECT event_id, icao24, airline, baro_altitude_m, event_ts
           FROM lake.silver.flights ORDER BY event_id LIMIT 200
        """
    )
    original = count(spark, f"SELECT count(*) FROM {TABLE}")
    first_snapshot = spark.sql(
        f"SELECT snapshot_id FROM {TABLE}.snapshots ORDER BY committed_at LIMIT 1"
    ).first()[0]
    print(f"   rows={original} first snapshot={first_snapshot}")
    assert original > 0

    print("\n2. A mistake: delete every GOL row")
    spark.sql(f"DELETE FROM {TABLE} WHERE airline = 'GOL'")
    after_delete = count(spark, f"SELECT count(*) FROM {TABLE}")
    print(f"   rows now={after_delete}")
    assert after_delete < original, "the delete removed nothing, the demo needs GOL rows"

    print("\n3. Time travel: the old data is still there, one snapshot back")
    travelled = count(spark, f"SELECT count(*) FROM {TABLE} VERSION AS OF {first_snapshot}")
    print(f"   rows at snapshot {first_snapshot}={travelled}")
    assert travelled == original

    print("\n4. Snapshot history")
    spark.sql(
        f"SELECT snapshot_id, operation, committed_at FROM {TABLE}.snapshots ORDER BY committed_at"
    ).show(truncate=False)

    print("5. Roll back to the first snapshot")
    spark.sql(f"CALL lake.system.rollback_to_snapshot('ops.time_travel_demo', {first_snapshot})")
    restored = count(spark, f"SELECT count(*) FROM {TABLE}")
    print(f"   rows after rollback={restored}")
    assert restored == original

    print("\n6. Schema evolution: add a column without rewriting any data")
    spark.sql(f"ALTER TABLE {TABLE} ADD COLUMN review_note string")
    nulls = count(spark, f"SELECT count(*) FROM {TABLE} WHERE review_note IS NULL")
    assert nulls == original, "existing rows should read the new column as null"
    print(f"   {nulls} existing rows read review_note as null")

    print("\n7. Partition evolution: start partitioning new data by airline")
    spark.sql(f"ALTER TABLE {TABLE} ADD PARTITION FIELD airline")
    spark.sql(
        f"INSERT INTO {TABLE} VALUES ('new-1', 'abc123', 'AZUL', 9000.0, current_timestamp(), 'x')"
    )
    total = count(spark, f"SELECT count(*) FROM {TABLE}")
    assert total == original + 1
    print(f"   old rows keep their layout, new rows are partitioned: total={total}")

    spark.sql(f"DROP TABLE {TABLE}")
    spark.stop()
    print("\nTIME TRAVEL DEMO OK", flush=True)


if __name__ == "__main__":
    main()
