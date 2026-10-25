"""Iceberg table definitions for the lakehouse. Creation is idempotent."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

NAMESPACES = ("bronze", "silver", "gold", "quality", "ops")

EVENT_COLUMNS = """
    event_id string,
    schema_version int,
    icao24 string,
    callsign string,
    airline string,
    latitude double,
    longitude double,
    baro_altitude_m double,
    velocity_ms double,
    true_track_deg double,
    on_ground boolean,
    event_ts timestamp"""

KAFKA_COLUMNS = """
    kafka_topic string,
    kafka_partition int,
    kafka_offset bigint,
    kafka_ts timestamp"""

BASE_PROPERTIES = {"format-version": "2"}

# Silver is upserted with MERGE INTO. Merge-on-read writes small delete files instead of rewriting
# whole data files, which suits frequent small merges. See ADR 0007.
MERGE_ON_READ = {
    "write.merge.mode": "merge-on-read",
    "write.update.mode": "merge-on-read",
    "write.delete.mode": "merge-on-read",
}


def _create_table(
    name: str,
    columns: str,
    partition: str | None = None,
    properties: dict[str, str] | None = None,
) -> str:
    props = {**BASE_PROPERTIES, **(properties or {})}
    rendered_props = ", ".join(f"'{key}' = '{value}'" for key, value in props.items())
    partition_clause = f"\nPARTITIONED BY ({partition})" if partition else ""
    return (
        f"CREATE TABLE IF NOT EXISTS lake.{name} ({columns}\n)\n"
        f"USING iceberg{partition_clause}\n"
        f"TBLPROPERTIES ({rendered_props})"
    )


TABLES: dict[str, str] = {
    # Parsed events exactly as received. Append only.
    "bronze.flights_raw": _create_table(
        "bronze.flights_raw",
        f"{EVENT_COLUMNS},{KAFKA_COLUMNS},\n    ingested_at timestamp",
        partition="days(event_ts)",
    ),
    # Payloads that could not be parsed, with the reason.
    "bronze.flights_dead_letter": _create_table(
        "bronze.flights_dead_letter",
        f"\n    payload string,\n    reason string,{KAFKA_COLUMNS},\n    ingested_at timestamp",
        partition="days(ingested_at)",
    ),
    # Deduplicated, validated and enriched events.
    "silver.flights": _create_table(
        "silver.flights",
        f"{EVENT_COLUMNS},\n    altitude_ft double,\n    velocity_kt double,\n"
        "    is_late boolean,\n    kafka_ts timestamp,\n    ingested_at timestamp,\n"
        "    processed_at timestamp",
        partition="days(event_ts)",
        properties=MERGE_ON_READ,
    ),
    # Events that parsed but broke a business rule.
    "silver.flights_quarantine": _create_table(
        "silver.flights_quarantine",
        f"{EVENT_COLUMNS},\n    reasons string,\n    kafka_ts timestamp,\n"
        "    ingested_at timestamp,\n    processed_at timestamp",
        partition="days(processed_at)",
    ),
    "gold.flights_hourly": _create_table(
        "gold.flights_hourly",
        "\n    airline string,\n    hour_ts timestamp,\n    flights bigint,\n"
        "    observations bigint,\n    avg_altitude_ft double,\n    avg_speed_kt double,\n"
        "    pct_on_ground double,\n    computed_at timestamp",
        partition="days(hour_ts)",
    ),
    "gold.activity_grid": _create_table(
        "gold.activity_grid",
        "\n    hour_ts timestamp,\n    cell_lat int,\n    cell_lon int,\n"
        "    observations bigint,\n    aircraft bigint,\n    computed_at timestamp",
        partition="days(hour_ts)",
    ),
    "quality.check_results": _create_table(
        "quality.check_results",
        "\n    run_id string,\n    job string,\n    table_name string,\n    check_name string,\n"
        "    severity string,\n    passed boolean,\n    observed double,\n    expected string,\n"
        "    checked_at timestamp",
        partition="days(checked_at)",
    ),
    # Last processed position of each incremental job.
    "ops.watermarks": _create_table(
        "ops.watermarks",
        "\n    job string,\n    watermark timestamp,\n    updated_at timestamp",
    ),
}


def ensure_lakehouse(spark: SparkSession) -> None:
    """Create every namespace and table that does not exist yet."""
    for namespace in NAMESPACES:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lake.{namespace}")
    for ddl in TABLES.values():
        spark.sql(ddl)
