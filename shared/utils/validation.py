"""
Generic validation helpers shared across telemetry-system services.

These are *stateless* utility functions that complement Pydantic model
validation with domain-specific checks reusable in multiple contexts
(API validation, Kafka consumer validation, etc.).

Validators are aligned with the simulation telemetry parameters defined
in the project README.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Sequence, Type


# ---------------------------------------------------------------------------
# GPS validation (×1e7 integer format from simulation)
# ---------------------------------------------------------------------------

def is_valid_gps_lat(value: int) -> bool:
    """
    Return ``True`` if *value* is a valid GPS latitude in ×1e7 format.

    Valid range: −900_000_000 … 900_000_000 (i.e. −90° … +90°).
    """
    return -900_000_000 <= value <= 900_000_000


def is_valid_gps_lon(value: int) -> bool:
    """
    Return ``True`` if *value* is a valid GPS longitude in ×1e7 format.

    Valid range: −1_800_000_000 … 1_800_000_000 (i.e. −180° … +180°).
    """
    return -1_800_000_000 <= value <= 1_800_000_000


# ---------------------------------------------------------------------------
# Legacy WGS-84 helpers (still useful for converted values)
# ---------------------------------------------------------------------------

def is_valid_latitude(value: float) -> bool:
    """Return ``True`` if *value* is a valid WGS-84 latitude (−90 … 90)."""
    return -90.0 <= value <= 90.0


def is_valid_longitude(value: float) -> bool:
    """Return ``True`` if *value* is a valid WGS-84 longitude (−180 … 180)."""
    return -180.0 <= value <= 180.0


def is_valid_altitude(value: float, *, max_altitude: float = 50_000.0) -> bool:
    """Return ``True`` if *value* is a plausible altitude in metres."""
    return -500.0 <= value <= max_altitude


# ---------------------------------------------------------------------------
# Motor & RPM validation
# ---------------------------------------------------------------------------

def is_valid_motor_command(value: float) -> bool:
    """Return ``True`` if a motor output command is in [0.0, 1.0]."""
    return 0.0 <= value <= 1.0


def is_valid_rpm(value: float) -> bool:
    """Return ``True`` if an RPM value is non-negative."""
    return value >= 0.0


def is_valid_motor_damage(value: float) -> bool:
    """Return ``True`` if motor damage percentage is in [0, 100]."""
    return 0.0 <= value <= 100.0


# ---------------------------------------------------------------------------
# Obstacle sensor validation
# ---------------------------------------------------------------------------

def is_valid_obstacle_distance(value: float) -> bool:
    """
    Return ``True`` if an obstacle sensor reading is non-negative.

    A value of 0 or very large value typically means no obstacle detected.
    """
    return value >= 0.0


# ---------------------------------------------------------------------------
# Battery validation
# ---------------------------------------------------------------------------

def is_valid_percentage(value: float) -> bool:
    """Return ``True`` if *value* is in [0, 100]."""
    return 0.0 <= value <= 100.0


def is_valid_battery_voltage(value: float) -> bool:
    """Return ``True`` if battery voltage is positive and plausible."""
    return 0.0 < value <= 100.0


def is_valid_current_draw(value: float) -> bool:
    """Return ``True`` if current draw is non-negative."""
    return value >= 0.0


# ---------------------------------------------------------------------------
# PID controller validation
# ---------------------------------------------------------------------------

def is_valid_pid_output(value: float, *, max_abs: float = 1000.0) -> bool:
    """Return ``True`` if a PID output is within a plausible range."""
    return -max_abs <= value <= max_abs


# ---------------------------------------------------------------------------
# Pilot input validation
# ---------------------------------------------------------------------------

def is_valid_pilot_input(value: float) -> bool:
    """Return ``True`` if a pilot control input is in [-1.0, 1.0]."""
    return -1.0 <= value <= 1.0


def is_valid_throttle_input(value: float) -> bool:
    """Return ``True`` if throttle input is in [0.0, 1.0]."""
    return 0.0 <= value <= 1.0


# ---------------------------------------------------------------------------
# General-purpose helpers
# ---------------------------------------------------------------------------

def in_range(
    value: float,
    *,
    low: float,
    high: float,
    inclusive: bool = True,
) -> bool:
    """
    Check whether *value* falls inside [*low*, *high*] (inclusive)
    or (*low*, *high*) (exclusive).
    """
    if inclusive:
        return low <= value <= high
    return low < value < high


def is_non_empty_string(value: Any) -> bool:
    """Return ``True`` if *value* is a non-empty, non-whitespace string."""
    return isinstance(value, str) and len(value.strip()) > 0


def is_valid_enum_value(value: Any, enum_class: Type[Enum]) -> bool:
    """Return ``True`` when *value* matches one of the enum members' values."""
    return value in {member.value for member in enum_class}


def validate_required_fields(
    data: dict[str, Any],
    required: Sequence[str],
) -> list[str]:
    """
    Return a list of field names from *required* that are missing or
    ``None`` in *data*.  An empty list means all fields are present.
    """
    return [
        field
        for field in required
        if field not in data or data[field] is None
    ]


def is_positive(value: float) -> bool:
    """Return ``True`` if *value* is strictly positive."""
    return value > 0.0


def is_valid_simulation_time(value: float) -> bool:
    """Return ``True`` if a simulation timestamp is non-negative."""
    return value >= 0.0
