"""Synthetic flight events modelled on ADS-B / OpenSky state vectors.

The generator is deterministic for a given seed and clock, which keeps tests and demos reproducible.
"""

from __future__ import annotations

import json
import math
import random
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import NamedTuple

SCHEMA_VERSION = 1

# (ICAO airline designator used in callsigns, airline name)
AIRLINES = (("GLO", "GOL"), ("TAM", "LATAM"), ("AZU", "AZUL"))

# Bounding box roughly covering Brazilian airspace.
LAT_RANGE = (-33.0, 5.0)
LON_RANGE = (-73.0, -35.0)

# Field name -> accepted Python type. Booleans are never accepted for numeric fields.
SCHEMA: dict[str, type] = {
    "schema_version": int,
    "event_id": str,
    "icao24": str,
    "callsign": str,
    "airline": str,
    "latitude": float,
    "longitude": float,
    "baro_altitude_m": float,
    "velocity_ms": float,
    "true_track_deg": float,
    "on_ground": bool,
    "event_ts": str,
}


MALFORMED_KINDS = ("not_json", "missing_field", "wrong_type", "out_of_range")


@dataclass(frozen=True)
class FaultConfig:
    """Probabilities (0 to 1) applied independently to every event."""

    duplicate_rate: float = 0.0
    late_rate: float = 0.0
    malformed_rate: float = 0.0
    max_lateness_s: int = 1800


class Payload(NamedTuple):
    """What goes on the wire: an optional Kafka key and the message value."""

    key: str | None
    value: str


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def validate_event(event: dict) -> list[str]:
    """Return the schema problems found in an event. An empty list means the event is valid."""
    problems: list[str] = []
    for name, expected in SCHEMA.items():
        if name not in event or event[name] is None:
            problems.append(f"missing field: {name}")
            continue
        value = event[name]
        if expected is float:
            ok = isinstance(value, (int, float)) and not isinstance(value, bool)
        elif expected is int:
            ok = isinstance(value, int) and not isinstance(value, bool)
        else:
            ok = isinstance(value, expected)
        if not ok:
            problems.append(f"wrong type for {name}: expected {expected.__name__}")
    return problems


def encode(event: dict) -> str:
    return json.dumps(event, separators=(",", ":"))


@dataclass
class Aircraft:
    icao24: str
    callsign: str
    airline: str
    latitude: float
    longitude: float
    altitude_m: float
    velocity_ms: float
    track_deg: float
    on_ground: bool = False

    def step(self, rng: random.Random, dt_s: float) -> None:
        """Advance the aircraft along its track for dt_s seconds."""
        if self.on_ground:
            return
        distance_m = self.velocity_ms * dt_s
        heading = math.radians(self.track_deg)
        d_lat = distance_m * math.cos(heading) / 111_320
        cos_lat = max(math.cos(math.radians(self.latitude)), 0.01)
        d_lon = distance_m * math.sin(heading) / (111_320 * cos_lat)
        self.latitude = _clamp(self.latitude + d_lat, *LAT_RANGE)
        self.longitude = _clamp(self.longitude + d_lon, *LON_RANGE)
        self.track_deg = (self.track_deg + rng.uniform(-2.0, 2.0)) % 360
        self.altitude_m = _clamp(self.altitude_m + rng.uniform(-30.0, 30.0), 500.0, 12_500.0)
        self.velocity_ms = _clamp(self.velocity_ms + rng.uniform(-3.0, 3.0), 120.0, 300.0)


class FlightEventGenerator:
    def __init__(
        self,
        fleet_size: int = 20,
        seed: int | None = None,
        tick_seconds: float = 1.0,
        clock: Callable[[], datetime] | None = None,
        faults: FaultConfig | None = None,
    ) -> None:
        self._faults = faults or FaultConfig()
        self._rng = random.Random(seed)
        self._tick_seconds = tick_seconds
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._fleet = [self._new_aircraft(i) for i in range(fleet_size)]

    def _new_aircraft(self, index: int) -> Aircraft:
        prefix, airline = AIRLINES[index % len(AIRLINES)]
        on_ground = self._rng.random() < 0.1
        return Aircraft(
            icao24=f"{self._rng.getrandbits(24):06x}",
            callsign=f"{prefix}{self._rng.randint(1000, 9999)}",
            airline=airline,
            latitude=self._rng.uniform(*LAT_RANGE),
            longitude=self._rng.uniform(*LON_RANGE),
            altitude_m=0.0 if on_ground else self._rng.uniform(3_000.0, 11_500.0),
            velocity_ms=(
                self._rng.uniform(0.0, 15.0) if on_ground else self._rng.uniform(180.0, 260.0)
            ),
            track_deg=self._rng.uniform(0.0, 360.0),
            on_ground=on_ground,
        )

    def _event_id(self) -> str:
        return str(uuid.UUID(int=self._rng.getrandbits(128), version=4))

    def _to_event(self, plane: Aircraft, now: datetime) -> dict:
        return {
            "schema_version": SCHEMA_VERSION,
            "event_id": self._event_id(),
            "icao24": plane.icao24,
            "callsign": plane.callsign,
            "airline": plane.airline,
            "latitude": round(plane.latitude, 5),
            "longitude": round(plane.longitude, 5),
            "baro_altitude_m": round(plane.altitude_m, 1),
            "velocity_ms": round(plane.velocity_ms, 1),
            "true_track_deg": round(plane.track_deg, 1),
            "on_ground": plane.on_ground,
            "event_ts": now.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        }

    def tick(self) -> list[dict]:
        """Advance every aircraft once and return one event per aircraft."""
        now = self._clock()
        events = []
        for plane in self._fleet:
            plane.step(self._rng, self._tick_seconds)
            events.append(self._to_event(plane, now))
        return events

    def _make_late(self, event: dict) -> dict:
        """Report an old observation: same event, event_ts shifted into the past."""
        shift = timedelta(seconds=self._rng.uniform(300.0, max(self._faults.max_lateness_s, 301)))
        observed = datetime.fromisoformat(event["event_ts"].replace("Z", "+00:00")) - shift
        late = dict(event)
        late["event_ts"] = observed.isoformat(timespec="milliseconds").replace("+00:00", "Z")
        return late

    def _corrupt(self, event: dict) -> Payload:
        """Break an event in one of four ways.

        not_json, missing_field and wrong_type fail schema parsing and end up in the bronze
        dead-letter table. out_of_range is valid JSON that breaks a business rule, so it is
        parsed into bronze and quarantined later in silver.
        """
        kind = self._rng.choice(MALFORMED_KINDS)
        key = event["icao24"]
        if kind == "not_json":
            return Payload(key, "{not-json:" + event["event_id"][:8])
        broken = dict(event)
        if kind == "missing_field":
            del broken["icao24"]
        elif kind == "wrong_type":
            broken["baro_altitude_m"] = "high"
        else:
            broken["latitude"] = 123.4
        return Payload(key, encode(broken))

    def tick_payloads(self) -> list[Payload]:
        """Like tick(), but serialised and with the configured faults applied."""
        faults = self._faults
        payloads: list[Payload] = []
        for event in self.tick():
            if self._rng.random() < faults.late_rate:
                event = self._make_late(event)
            if self._rng.random() < faults.malformed_rate:
                payload = self._corrupt(event)
            else:
                payload = Payload(event["icao24"], encode(event))
            payloads.append(payload)
            if self._rng.random() < faults.duplicate_rate:
                payloads.append(payload)
        return payloads
