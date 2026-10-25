import json
from datetime import datetime, timezone

from flight_events import FaultConfig, FlightEventGenerator, validate_event

FIXED_NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def make(faults, seed=3, fleet_size=10):
    return FlightEventGenerator(
        fleet_size=fleet_size, seed=seed, clock=lambda: FIXED_NOW, faults=faults
    )


def parse(payload):
    try:
        return json.loads(payload.value)
    except json.JSONDecodeError:
        return None


def test_no_faults_gives_one_valid_payload_per_aircraft():
    payloads = make(FaultConfig()).tick_payloads()
    assert len(payloads) == 10
    assert all(validate_event(parse(p)) == [] for p in payloads)


def test_duplicates_repeat_the_same_payload():
    payloads = make(FaultConfig(duplicate_rate=1.0)).tick_payloads()
    assert len(payloads) == 20
    assert payloads[0] == payloads[1]


def test_late_events_are_shifted_into_the_past():
    payloads = make(FaultConfig(late_rate=1.0)).tick_payloads()
    for payload in payloads:
        observed = datetime.fromisoformat(parse(payload)["event_ts"].replace("Z", "+00:00"))
        assert (FIXED_NOW - observed).total_seconds() >= 300


def test_malformed_events_cover_every_kind_of_breakage():
    generator = make(FaultConfig(malformed_rate=1.0), fleet_size=40)
    seen = set()
    for _ in range(10):
        for payload in generator.tick_payloads():
            event = parse(payload)
            if event is None:
                seen.add("not_json")
            elif "icao24" not in event:
                seen.add("missing_field")
            elif isinstance(event["baro_altitude_m"], str):
                seen.add("wrong_type")
            elif event["latitude"] > 90:
                seen.add("out_of_range")
    assert seen == {"not_json", "missing_field", "wrong_type", "out_of_range"}


def test_fault_injection_is_deterministic():
    faults = FaultConfig(duplicate_rate=0.3, late_rate=0.3, malformed_rate=0.3)
    assert make(faults, seed=5).tick_payloads() == make(faults, seed=5).tick_payloads()
