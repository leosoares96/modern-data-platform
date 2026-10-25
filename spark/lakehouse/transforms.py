"""Pure DataFrame transformations. They hold no I/O so they can be tested on a local Spark."""

from __future__ import annotations

import operator
from functools import reduce

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from lakehouse.schemas import EVENT_SCHEMA, REQUIRED_FIELDS, SCHEMA_VERSION

EVENT_COLUMNS = [
    "event_id",
    "schema_version",
    "icao24",
    "callsign",
    "airline",
    "latitude",
    "longitude",
    "baro_altitude_m",
    "velocity_ms",
    "true_track_deg",
    "on_ground",
    "event_ts",
]
KAFKA_COLUMNS = ["kafka_topic", "kafka_partition", "kafka_offset", "kafka_ts"]

REASON_NULL_PAYLOAD = "null_payload"
REASON_UNPARSEABLE = "unparseable_json"
REASON_MISSING = "missing_or_invalid_fields"
REASON_VERSION = "unsupported_schema_version"
REASON_BAD_TIMESTAMP = "invalid_event_ts"

SILVER_COLUMNS = [
    *EVENT_COLUMNS,
    "altitude_ft",
    "velocity_kt",
    "is_late",
    "kafka_ts",
    "ingested_at",
    "processed_at",
]
QUARANTINE_COLUMNS = [*EVENT_COLUMNS, "reasons", "kafka_ts", "ingested_at", "processed_at"]

# Business rules applied in silver. Bounds are deliberately generous: they catch impossible values,
# not unusual ones.
LATITUDE_RANGE = (-90.0, 90.0)
LONGITUDE_RANGE = (-180.0, 180.0)
ALTITUDE_RANGE_M = (-500.0, 20_000.0)
VELOCITY_RANGE_MS = (0.0, 400.0)
TRACK_RANGE_DEG = (0.0, 360.0)
FUTURE_TOLERANCE_SECONDS = 300
LATE_THRESHOLD_SECONDS = 300

METERS_TO_FEET = 3.28084
MS_TO_KNOTS = 1.943844


def _all(columns: list[Column]) -> Column:
    return reduce(operator.and_, columns)


def _any(columns: list[Column]) -> Column:
    return reduce(operator.or_, columns)


def parse_kafka_batch(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split a batch read from Kafka into (valid events, dead-letter rows).

    `raw` has the columns Spark's Kafka source produces (value, topic, partition, offset,
    timestamp). Every input row lands in exactly one of the two outputs. Parsing happens here and
    not in silver so that a bad payload never blocks the stream and is never lost.
    """
    base = raw.select(
        F.col("value").cast("string").alias("payload"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
    )
    parsed = base.withColumn("parsed", F.from_json(F.col("payload"), EVENT_SCHEMA))
    parsed = parsed.withColumn("parsed_ts", F.to_timestamp(F.col("parsed.event_ts")))

    fields = [F.col(f"parsed.{name}") for name in REQUIRED_FIELDS]
    all_present = _all([field.isNotNull() for field in fields])
    any_present = _any([field.isNotNull() for field in fields])

    # The first matching condition wins; a row with no reason is a valid event.
    reason = (
        F.when(F.col("payload").isNull(), F.lit(REASON_NULL_PAYLOAD))
        .when(~any_present, F.lit(REASON_UNPARSEABLE))
        .when(~all_present, F.lit(REASON_MISSING))
        .when(F.col("parsed.schema_version") != SCHEMA_VERSION, F.lit(REASON_VERSION))
        .when(F.col("parsed_ts").isNull(), F.lit(REASON_BAD_TIMESTAMP))
    )
    tagged = parsed.withColumn("reason", reason).withColumn("ingested_at", F.current_timestamp())

    valid = tagged.filter(F.col("reason").isNull()).select(
        *[F.col(f"parsed.{name}") for name in EVENT_COLUMNS if name != "event_ts"],
        F.col("parsed_ts").alias("event_ts"),
        *KAFKA_COLUMNS,
        "ingested_at",
    )
    valid = valid.select(*EVENT_COLUMNS, *KAFKA_COLUMNS, "ingested_at")
    dead = tagged.filter(F.col("reason").isNotNull()).select(
        "payload", "reason", *KAFKA_COLUMNS, "ingested_at"
    )
    return valid, dead


def _outside(column: str, bounds: tuple[float, float], reason: str) -> Column:
    return F.when((F.col(column) < bounds[0]) | (F.col(column) > bounds[1]), F.lit(reason))


def apply_business_rules(df: DataFrame) -> DataFrame:
    """Add `reasons`: the broken rules as a comma-separated list, empty when the row is clean."""
    broken = [
        _outside("latitude", LATITUDE_RANGE, "latitude_out_of_range"),
        _outside("longitude", LONGITUDE_RANGE, "longitude_out_of_range"),
        _outside("baro_altitude_m", ALTITUDE_RANGE_M, "altitude_out_of_range"),
        _outside("velocity_ms", VELOCITY_RANGE_MS, "velocity_out_of_range"),
        _outside("true_track_deg", TRACK_RANGE_DEG, "track_out_of_range"),
        F.when(~F.col("icao24").rlike("^[0-9a-fA-F]{6}$"), F.lit("invalid_icao24")),
        F.when(
            F.unix_timestamp("event_ts")
            > F.unix_timestamp(F.current_timestamp()) + FUTURE_TOLERANCE_SECONDS,
            F.lit("event_in_the_future"),
        ),
    ]
    # concat_ws skips nulls, so a clean row ends up with an empty string.
    return df.withColumn("reasons", F.concat_ws(",", *broken))


def split_by_rules(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (clean rows, quarantined rows). Quarantined rows keep their `reasons`."""
    checked = apply_business_rules(df)
    return checked.filter(F.col("reasons") == "").drop("reasons"), checked.filter(
        F.col("reasons") != ""
    )


def dedupe_events(df: DataFrame, key: str = "event_id") -> DataFrame:
    """Keep one row per key: the one Kafka stored first. Duplicates carry the same event_id."""
    first_seen = Window.partitionBy(key).orderBy(
        F.col("kafka_ts").asc(), F.col("kafka_partition").asc(), F.col("kafka_offset").asc()
    )
    return df.withColumn("_rank", F.row_number().over(first_seen)).filter("_rank = 1").drop("_rank")


def to_silver(df: DataFrame) -> DataFrame:
    """Enrich clean events: imperial units, a late-arrival flag and the processing time."""
    lateness_s = F.unix_timestamp("kafka_ts") - F.unix_timestamp("event_ts")
    return df.select(
        *EVENT_COLUMNS,
        (F.col("baro_altitude_m") * METERS_TO_FEET).alias("altitude_ft"),
        (F.col("velocity_ms") * MS_TO_KNOTS).alias("velocity_kt"),
        (lateness_s > LATE_THRESHOLD_SECONDS).alias("is_late"),
        "kafka_ts",
        "ingested_at",
        F.current_timestamp().alias("processed_at"),
    )


def to_quarantine(df: DataFrame) -> DataFrame:
    return df.select(
        *EVENT_COLUMNS,
        "reasons",
        "kafka_ts",
        "ingested_at",
        F.current_timestamp().alias("processed_at"),
    )
