"""Wire schema of a flight event, mirrored from producer/adsb/flight_events.py."""

from __future__ import annotations

from pyspark.sql import types as T

SCHEMA_VERSION = 1

# event_ts stays a string on the wire and is converted to a timestamp after validation.
EVENT_SCHEMA = T.StructType(
    [
        T.StructField("schema_version", T.IntegerType()),
        T.StructField("event_id", T.StringType()),
        T.StructField("icao24", T.StringType()),
        T.StructField("callsign", T.StringType()),
        T.StructField("airline", T.StringType()),
        T.StructField("latitude", T.DoubleType()),
        T.StructField("longitude", T.DoubleType()),
        T.StructField("baro_altitude_m", T.DoubleType()),
        T.StructField("velocity_ms", T.DoubleType()),
        T.StructField("true_track_deg", T.DoubleType()),
        T.StructField("on_ground", T.BooleanType()),
        T.StructField("event_ts", T.StringType()),
    ]
)

REQUIRED_FIELDS = [field.name for field in EVENT_SCHEMA.fields]
