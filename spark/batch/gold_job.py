"""Gold batch job: silver -> gold aggregates.

  lake.gold.flights_hourly  per airline and hour: distinct aircraft, observations, averages
  lake.gold.activity_grid   per hour and one-degree cell: observations and distinct aircraft

Only the hours touched by silver rows processed since the last run are recomputed, and they are
recomputed from scratch from silver, so a late event simply corrects the hour it belongs to. The
result is written with MERGE INTO and is idempotent.
"""

from __future__ import annotations

import uuid

from pyspark.sql import functions as F

from lakehouse.ddl import ensure_lakehouse
from lakehouse.quality import (
    InRange,
    MinRows,
    NotNull,
    Unique,
    assert_no_errors,
    evaluate,
    persist_results,
)
from lakehouse.session import get_spark

JOB = "gold_flights"
SILVER = "lake.silver.flights"
HOURLY = "lake.gold.flights_hourly"
GRID = "lake.gold.activity_grid"
WATERMARKS = "lake.ops.watermarks"
OVERLAP = "2 MINUTES"

HOURLY_SQL = f"""
MERGE INTO {HOURLY} AS t
USING (
    SELECT s.airline,
           date_trunc('hour', s.event_ts) AS hour_ts,
           count(DISTINCT s.icao24) AS flights,
           count(*) AS observations,
           avg(s.altitude_ft) AS avg_altitude_ft,
           avg(s.velocity_kt) AS avg_speed_kt,
           100.0 * avg(CASE WHEN s.on_ground THEN 1.0 ELSE 0.0 END) AS pct_on_ground,
           current_timestamp() AS computed_at
    FROM {SILVER} AS s
    JOIN gold_changed_hours AS h ON date_trunc('hour', s.event_ts) = h.hour_ts
    GROUP BY s.airline, date_trunc('hour', s.event_ts)
) AS u
ON t.airline = u.airline AND t.hour_ts = u.hour_ts
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""

GRID_SQL = f"""
MERGE INTO {GRID} AS t
USING (
    SELECT date_trunc('hour', s.event_ts) AS hour_ts,
           CAST(floor(s.latitude) AS INT) AS cell_lat,
           CAST(floor(s.longitude) AS INT) AS cell_lon,
           count(*) AS observations,
           count(DISTINCT s.icao24) AS aircraft,
           current_timestamp() AS computed_at
    FROM {SILVER} AS s
    JOIN gold_changed_hours AS h ON date_trunc('hour', s.event_ts) = h.hour_ts
    GROUP BY date_trunc('hour', s.event_ts), floor(s.latitude), floor(s.longitude)
) AS u
ON t.hour_ts = u.hour_ts AND t.cell_lat = u.cell_lat AND t.cell_lon = u.cell_lon
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""

WATERMARK_SQL = f"""
MERGE INTO {WATERMARKS} AS t
USING (
    SELECT '{JOB}' AS job, max(processed_at) AS watermark, current_timestamp() AS updated_at
    FROM gold_new_silver
) AS s
ON t.job = s.job
WHEN MATCHED THEN UPDATE SET t.watermark = s.watermark, t.updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT *
"""


def main() -> None:
    spark = get_spark("gold-flights")
    ensure_lakehouse(spark)
    run_id = str(uuid.uuid4())

    new_silver = spark.sql(
        f"""
        SELECT * FROM {SILVER}
        WHERE processed_at > COALESCE(
            (SELECT watermark FROM {WATERMARKS} WHERE job = '{JOB}'),
            TIMESTAMP '1970-01-01 00:00:00'
        ) - INTERVAL {OVERLAP}
        """
    ).persist()
    if new_silver.count() == 0:
        print("gold: nothing new in silver", flush=True)
        spark.stop()
        return
    new_silver.createOrReplaceTempView("gold_new_silver")

    changed = new_silver.select(F.date_trunc("hour", "event_ts").alias("hour_ts")).distinct()
    changed.persist().createOrReplaceTempView("gold_changed_hours")
    hours = changed.count()

    spark.sql(HOURLY_SQL)
    spark.sql(GRID_SQL)
    spark.sql(WATERMARK_SQL)

    hourly_rows = spark.table(HOURLY).count()
    grid_rows = spark.table(GRID).count()
    print(
        f"gold: hours_recomputed={hours} hourly_rows={hourly_rows} grid_rows={grid_rows}",
        flush=True,
    )

    hourly = spark.table(HOURLY).withColumn(
        "airline_hour", F.concat_ws("|", F.col("airline"), F.col("hour_ts").cast("string"))
    )
    results = evaluate(
        hourly,
        "gold.flights_hourly",
        [
            NotNull("airline"),
            NotNull("hour_ts"),
            Unique("airline_hour"),
            InRange("pct_on_ground", 0, 100),
            MinRows(1),
        ],
    )
    persist_results(spark, run_id, JOB, results)
    for result in results:
        status = "PASS" if result.passed else f"FAIL[{result.severity}]"
        print(f"quality {status} {result.check_name} observed={result.observed}", flush=True)
    assert_no_errors(results)
    spark.stop()


if __name__ == "__main__":
    main()
