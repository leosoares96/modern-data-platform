from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import types as T  # noqa: E402

from lakehouse.transforms import (  # noqa: E402
    QUARANTINE_COLUMNS,
    SILVER_COLUMNS,
    dedupe_events,
    split_by_rules,
    to_quarantine,
    to_silver,
)

NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType()),
        T.StructField("schema_version", T.IntegerType()),
        T.StructField("icao24", T.StringType()),
        T.StructField("callsign", T.StringType()),
        T.StructField("airline", T.StringType()),
        T.StructField("latitude", T.DoubleType()),
        T.StructField("longitude", T.DoubleType()),
        T.StructField("baro_altitude_m", T.DoubleType()),
        T.StructField("velocity_ms", T.DoubleType()),
        T.StructField("true_track_deg", T.DoubleType()),
        T.StructField("on_ground", T.BooleanType()),
        T.StructField("event_ts", T.TimestampType()),
        T.StructField("kafka_topic", T.StringType()),
        T.StructField("kafka_partition", T.IntegerType()),
        T.StructField("kafka_offset", T.LongType()),
        T.StructField("kafka_ts", T.TimestampType()),
        T.StructField("ingested_at", T.TimestampType()),
    ]
)

BASE = {
    "event_id": "e1",
    "schema_version": 1,
    "icao24": "abc123",
    "callsign": "GLO1234",
    "airline": "GOL",
    "latitude": -23.5,
    "longitude": -46.6,
    "baro_altitude_m": 10_000.0,
    "velocity_ms": 230.0,
    "true_track_deg": 90.0,
    "on_ground": False,
    "event_ts": NOW,
    "kafka_topic": "flights.raw",
    "kafka_partition": 0,
    "kafka_offset": 0,
    "kafka_ts": NOW + timedelta(seconds=1),
    "ingested_at": NOW + timedelta(seconds=2),
}


def frame(spark, *overrides):
    rows = [
        tuple({**BASE, **change}[field.name] for field in SCHEMA.fields) for change in overrides
    ]
    return spark.createDataFrame(rows, SCHEMA)


def reasons_of(quarantined):
    return [row["reasons"] for row in quarantined.collect()]


def test_clean_rows_pass_and_lose_the_reasons_column(spark):
    clean, quarantined = split_by_rules(frame(spark, {}))
    assert clean.count() == 1
    assert quarantined.count() == 0
    assert "reasons" not in clean.columns


@pytest.mark.parametrize(
    ("override", "reason"),
    [
        ({"latitude": 123.4}, "latitude_out_of_range"),
        ({"longitude": -200.0}, "longitude_out_of_range"),
        ({"baro_altitude_m": 25_000.0}, "altitude_out_of_range"),
        ({"velocity_ms": -1.0}, "velocity_out_of_range"),
        ({"true_track_deg": 400.0}, "track_out_of_range"),
        ({"icao24": "zzzzzz"}, "invalid_icao24"),
        ({"event_ts": datetime.now(timezone.utc) + timedelta(days=1)}, "event_in_the_future"),
    ],
)
def test_each_business_rule_quarantines_the_row(spark, override, reason):
    clean, quarantined = split_by_rules(frame(spark, override))
    assert clean.count() == 0
    assert reasons_of(quarantined) == [reason]


def test_several_broken_rules_are_listed_together(spark):
    _, quarantined = split_by_rules(frame(spark, {"latitude": 123.4, "icao24": "nothex"}))
    assert set(reasons_of(quarantined)[0].split(",")) == {"latitude_out_of_range", "invalid_icao24"}


def test_values_exactly_on_the_boundary_are_accepted(spark):
    edge = {"latitude": 90.0, "longitude": -180.0, "velocity_ms": 400.0, "true_track_deg": 360.0}
    clean, _ = split_by_rules(frame(spark, edge))
    assert clean.count() == 1


def test_dedupe_keeps_the_copy_kafka_stored_first(spark):
    df = frame(
        spark,
        {"kafka_offset": 7, "kafka_ts": NOW + timedelta(seconds=5)},
        {"kafka_offset": 3, "kafka_ts": NOW + timedelta(seconds=1)},
        {"event_id": "e2", "kafka_offset": 9},
    )
    rows = {row["event_id"]: row["kafka_offset"] for row in dedupe_events(df).collect()}
    assert rows == {"e1": 3, "e2": 9}


def test_silver_converts_units(spark):
    row = to_silver(frame(spark, {"baro_altitude_m": 1_000.0, "velocity_ms": 100.0})).first()
    assert row["altitude_ft"] == pytest.approx(3280.84, rel=1e-4)
    assert row["velocity_kt"] == pytest.approx(194.3844, rel=1e-4)


def test_late_arrivals_are_flagged(spark):
    late = {"kafka_ts": NOW + timedelta(minutes=10)}
    on_time = {"event_id": "e2", "kafka_ts": NOW + timedelta(seconds=1)}
    flags = {
        row["event_id"]: row["is_late"] for row in to_silver(frame(spark, late, on_time)).collect()
    }
    assert flags == {"e1": True, "e2": False}


def test_output_columns_match_the_table_definitions(spark):
    clean, quarantined = split_by_rules(frame(spark, {}, {"event_id": "e2", "latitude": 99.0}))
    assert to_silver(clean).columns == SILVER_COLUMNS
    assert to_quarantine(quarantined).columns == QUARANTINE_COLUMNS
