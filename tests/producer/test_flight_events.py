from datetime import datetime, timezone

import pytest

from flight_events import LAT_RANGE, LON_RANGE, FlightEventGenerator, encode, validate_event

FIXED_NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def make(seed=7, fleet_size=10):
    return FlightEventGenerator(fleet_size=fleet_size, seed=seed, clock=lambda: FIXED_NOW)


def test_same_seed_gives_identical_events():
    assert make(seed=1).tick() == make(seed=1).tick()


def test_different_seed_gives_different_events():
    assert make(seed=1).tick() != make(seed=2).tick()


def test_one_event_per_aircraft_per_tick():
    assert len(make(fleet_size=13).tick()) == 13


def test_events_match_the_schema():
    generator = make()
    for _ in range(5):
        for event in generator.tick():
            assert validate_event(event) == []


def test_event_ids_are_unique():
    generator = make()
    ids = [event["event_id"] for _ in range(10) for event in generator.tick()]
    assert len(ids) == len(set(ids))


def test_positions_stay_inside_the_bounding_box():
    generator = make()
    for _ in range(200):
        for event in generator.tick():
            assert LAT_RANGE[0] <= event["latitude"] <= LAT_RANGE[1]
            assert LON_RANGE[0] <= event["longitude"] <= LON_RANGE[1]


def test_event_timestamp_is_utc_iso8601():
    event = make().tick()[0]
    assert event["event_ts"] == "2026-01-01T12:00:00.000Z"


def test_validate_event_reports_missing_and_mistyped_fields():
    event = make().tick()[0]
    del event["icao24"]
    event["baro_altitude_m"] = "high"
    event["on_ground"] = "no"
    problems = validate_event(event)
    assert "missing field: icao24" in problems
    assert any("baro_altitude_m" in problem for problem in problems)
    assert any("on_ground" in problem for problem in problems)


@pytest.mark.parametrize("field", ["latitude", "velocity_ms"])
def test_booleans_are_rejected_for_numeric_fields(field):
    event = make().tick()[0]
    event[field] = True
    assert any(field in problem for problem in validate_event(event))


def test_encode_is_compact_json():
    assert ", " not in encode({"a": 1, "b": [1, 2]})
