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
from datetime import datetime, timezone

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
    ) -> None:
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
