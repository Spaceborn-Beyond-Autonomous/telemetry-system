"""
Data models for the raw-writer service.

Provides helper functions to convert Kafka JSON messages
(``IngestionEvent`` dicts) into ordered tuples matching the
``telemetry_raw`` table column order for efficient batch inserts.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any


# Column order must match db.COLUMNS exactly
COLUMN_ORDER = [
    "ingestion_id", "mission_id", "drone_profile", "schema_version",
    "exported_at", "ingested_at",
    "t",
    "px", "py", "pz",
    "roll", "pitch", "yaw",
    "gx", "gy", "gz",
    "accX", "accY", "accZ",       # JSON key names from IngestionEvent
    "vx", "vy", "vz",
    "m0", "m1", "m2", "m3",
    "rpm0", "rpm1", "rpm2", "rpm3",
    "batt", "curr", "batt_pct",
    "baro_raw", "baro_filtered",
    "wind_x", "wind_z",
    "dryden_x", "dryden_y", "dryden_z",
    "gps_lat", "gps_lon", "gps_fix", "gps_sat", "gps_eph", "gps_epv",
    "obs_fwd", "obs_right", "obs_back", "obs_left", "obs_up",
    "mode", "armed", "crashed", "grounded", "ground_y",
    "dmg0", "dmg1", "dmg2", "dmg3",
    "input_throttle", "input_pitch", "input_roll", "input_yaw",
    "pid_roll_err", "pid_roll_out",
    "pid_pitch_err", "pid_pitch_out",
    "pid_yaw_err", "pid_yaw_out",
    "pid_alt_err", "pid_alt_out",
]


def _parse_timestamp(value: str) -> datetime:
    """
    Parse an ISO 8601 timestamp string into a timezone-aware datetime.

    Handles both ``Z`` suffix and ``+00:00`` offset formats.
    """
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def event_to_record(event: dict[str, Any]) -> tuple[Any, ...]:
    """
    Convert a deserialized Kafka ``IngestionEvent`` JSON dict into an
    ordered tuple matching the ``telemetry_raw`` table columns.

    Handles:
    * ISO 8601 timestamp strings → ``datetime`` objects for asyncpg
    * JSON field name mapping (``accX`` → positional slot for ``acc_x``)
    * Type preservation for all numeric fields

    Parameters
    ----------
    event:
        Dict decoded from Kafka message value (JSON).

    Returns
    -------
    tuple
        Values in ``db.COLUMNS`` order, ready for ``executemany``.

    Raises
    ------
    KeyError
        If a required field is missing from the event.
    ValueError
        If a timestamp cannot be parsed.
    """
    return (
        # Provenance
        event["ingestion_id"],
        event["mission_id"],
        event["drone_profile"],
        event["schema_version"],
        _parse_timestamp(event["exported_at"]),
        _parse_timestamp(event["ingested_at"]),
        # Timing
        float(event["t"]),
        # Position
        float(event["px"]),
        float(event["py"]),
        float(event["pz"]),
        # Orientation
        float(event["roll"]),
        float(event["pitch"]),
        float(event["yaw"]),
        # Gyroscope
        float(event["gx"]),
        float(event["gy"]),
        float(event["gz"]),
        # Accelerometer
        float(event["accX"]),
        float(event["accY"]),
        float(event["accZ"]),
        # Velocity
        float(event["vx"]),
        float(event["vy"]),
        float(event["vz"]),
        # Motor commands
        float(event["m0"]),
        float(event["m1"]),
        float(event["m2"]),
        float(event["m3"]),
        # Motor RPM
        float(event["rpm0"]),
        float(event["rpm1"]),
        float(event["rpm2"]),
        float(event["rpm3"]),
        # Battery
        float(event["batt"]),
        float(event["curr"]),
        float(event["batt_pct"]),
        # Barometer
        float(event["baro_raw"]),
        float(event["baro_filtered"]),
        # Wind
        float(event["wind_x"]),
        float(event["wind_z"]),
        # Dryden turbulence
        float(event["dryden_x"]),
        float(event["dryden_y"]),
        float(event["dryden_z"]),
        # GPS
        int(event["gps_lat"]),
        int(event["gps_lon"]),
        int(event["gps_fix"]),
        int(event["gps_sat"]),
        int(event["gps_eph"]),
        int(event["gps_epv"]),
        # Obstacle sensors
        float(event["obs_fwd"]),
        float(event["obs_right"]),
        float(event["obs_back"]),
        float(event["obs_left"]),
        float(event["obs_up"]),
        # Flight controller state
        str(event["mode"]),
        bool(event["armed"]),
        int(event["crashed"]),
        int(event["grounded"]),
        float(event["ground_y"]),
        # Motor damage
        float(event["dmg0"]),
        float(event["dmg1"]),
        float(event["dmg2"]),
        float(event["dmg3"]),
        # Pilot inputs
        float(event["input_throttle"]),
        float(event["input_pitch"]),
        float(event["input_roll"]),
        float(event["input_yaw"]),
        # PID controller
        float(event["pid_roll_err"]),
        float(event["pid_roll_out"]),
        float(event["pid_pitch_err"]),
        float(event["pid_pitch_out"]),
        float(event["pid_yaw_err"]),
        float(event["pid_yaw_out"]),
        float(event["pid_alt_err"]),
        float(event["pid_alt_out"]),
    )
