import json
from datetime import datetime, timezone

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import types as T  # noqa: E402

from flight_events import FaultConfig, FlightEventGenerator, encode  # noqa: E402
from lakehouse.transforms import (  # noqa: E402
    EVENT_COLUMNS,
    KAFKA_COLUMNS,
    REASON_BAD_TIMESTAMP,
    REASON_NULL_PAYLOAD,
    REASON_UNPARSEABLE,
    REASON_VERSION,
    parse_kafka_batch,
)

KAFKA_SCHEMA = T.StructType(
    [
        T.StructField("key", T.BinaryType()),
        T.StructField("value", T.BinaryType()),
        T.StructField("topic", T.StringType()),
        T.StructField("partition", T.IntegerType()),
        T.StructField("offset", T.LongType()),
        T.StructField("timestamp", T.TimestampType()),
    ]
)
NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def kafka_df(spark, payloads):
    rows = [
        (None, None if p is None else p.encode(), "flights.raw", 0, offset, NOW)
        for offset, p in enumerate(payloads)
    ]
    return spark.createDataFrame(rows, KAFKA_SCHEMA)


def good_events(n=5):
    generator = FlightEventGenerator(fleet_size=n, seed=1, clock=lambda: NOW)
    return generator.tick()


def split(spark, payloads):
    valid, dead = parse_kafka_batch(kafka_df(spark, payloads))
    return valid, dead


def reasons(dead):
    return {row["reason"] for row in dead.collect()}


def test_valid_events_are_parsed_with_kafka_coordinates(spark):
    events = good_events(5)
    valid, dead = split(spark, [encode(e) for e in events])
    assert valid.count() == 5
    assert dead.count() == 0
    assert valid.columns == [*EVENT_COLUMNS, *KAFKA_COLUMNS, "ingested_at"]
    assert dict(valid.dtypes)["event_ts"] == "timestamp"
    assert sorted(r["kafka_offset"] for r in valid.collect()) == [0, 1, 2, 3, 4]


def test_event_timestamp_is_converted_to_utc(spark):
    valid, _ = split(spark, [encode(good_events(1)[0])])
    event_ts = valid.first()["event_ts"]
    assert event_ts.replace(tzinfo=timezone.utc) == NOW


def test_text_that_is_not_json_goes_to_dead_letter(spark):
    valid, dead = split(spark, ["{not-json:abc12345"])
    assert valid.count() == 0
    assert reasons(dead) == {REASON_UNPARSEABLE}


def test_missing_field_goes_to_dead_letter(spark):
    event = good_events(1)[0]
    del event["icao24"]
    valid, dead = split(spark, [json.dumps(event)])
    assert valid.count() == 0
    assert dead.count() == 1


def test_wrong_type_goes_to_dead_letter(spark):
    event = good_events(1)[0]
    event["baro_altitude_m"] = "high"
    valid, dead = split(spark, [json.dumps(event)])
    assert valid.count() == 0
    assert dead.count() == 1


def test_out_of_range_values_are_still_valid_json_and_pass_through(spark):
    event = good_events(1)[0]
    event["latitude"] = 123.4
    valid, dead = split(spark, [json.dumps(event)])
    assert valid.count() == 1
    assert dead.count() == 0


def test_unsupported_schema_version_goes_to_dead_letter(spark):
    event = good_events(1)[0]
    event["schema_version"] = 2
    _, dead = split(spark, [json.dumps(event)])
    assert reasons(dead) == {REASON_VERSION}


def test_null_payload_goes_to_dead_letter(spark):
    _, dead = split(spark, [None])
    assert reasons(dead) == {REASON_NULL_PAYLOAD}


def test_unparseable_event_timestamp_goes_to_dead_letter(spark):
    event = good_events(1)[0]
    event["event_ts"] = "yesterday"
    _, dead = split(spark, [json.dumps(event)])
    assert reasons(dead) == {REASON_BAD_TIMESTAMP}


def test_dead_letter_keeps_the_raw_payload_and_its_position(spark):
    _, dead = split(spark, ["garbage"])
    row = dead.first()
    assert row["payload"] == "garbage"
    assert row["kafka_topic"] == "flights.raw"
    assert row["kafka_offset"] == 0


def test_every_input_row_lands_in_exactly_one_output(spark):
    generator = FlightEventGenerator(
        fleet_size=30,
        seed=9,
        clock=lambda: NOW,
        faults=FaultConfig(duplicate_rate=0.2, late_rate=0.2, malformed_rate=0.4),
    )
    payloads = [p.value for _ in range(5) for p in generator.tick_payloads()]
    valid, dead = split(spark, payloads)
    assert valid.count() + dead.count() == len(payloads)
