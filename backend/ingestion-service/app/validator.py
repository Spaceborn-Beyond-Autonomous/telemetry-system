"""
Input validation layer for the ingestion service.

Operates *on top of* the Pydantic schema validation to enforce
business-level invariants that cannot be expressed in field constraints
alone (e.g. timestamp ordering, motor consistency, GPS sanity).

Aligned with the simulation telemetry parameters from the README.

Every public function either returns ``None`` on success or raises
a ``ValidationError`` describing the problem.
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

# Ensure the shared package is importable
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[3]))

from shared.schemas.telemetry_schema import TelemetryBatchPayload, TelemetryFrame
from shared.utils.validation import (
    is_valid_gps_lat,
    is_valid_gps_lon,
    is_valid_motor_damage,
    is_valid_obstacle_distance,
    is_valid_percentage,
    is_valid_simulation_time,
)

if TYPE_CHECKING:
    pass


class ValidationError(Exception):
    """Raised when a telemetry batch fails business-rule validation."""

    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        super().__init__(f"Validation failed: {'; '.join(errors)}")


# ---------------------------------------------------------------------------
# Public validation functions
# ---------------------------------------------------------------------------

def validate_batch(
    payload: TelemetryBatchPayload,
    *,
    max_frames: int = 10_000,
    enforce_timestamp_order: bool = True,
) -> None:
    """
    Run all business-rule validations on a ``TelemetryBatchPayload``.

    Parameters
    ----------
    payload:
        Already Pydantic-validated payload.
    max_frames:
        Upper limit on the number of frames per batch.
    enforce_timestamp_order:
        When *True*, reject batches whose simulation timestamps (``t``)
        are not strictly monotonically increasing.

    Raises
    ------
    ValidationError
        Contains a list of human-readable error strings.
    """
    errors: list[str] = []

    # --- batch-level checks ---
    if len(payload.frames) > max_frames:
        errors.append(
            f"Batch exceeds maximum frame count: "
            f"{len(payload.frames)} > {max_frames}"
        )

    # --- simulation timestamp ordering ---
    if enforce_timestamp_order:
        _check_timestamp_ordering(payload.frames, errors)

    # --- per-frame deep validation ---
    for idx, frame in enumerate(payload.frames):
        _validate_frame(frame, idx, errors)

    if errors:
        raise ValidationError(errors)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _check_timestamp_ordering(
    frames: list[TelemetryFrame],
    errors: list[str],
) -> None:
    """
    Verify that simulation timestamps (``t``) are strictly increasing.

    Duplicate or out-of-order timestamps indicate corrupt or
    mis-assembled batches.
    """
    for i in range(1, len(frames)):
        if frames[i].t <= frames[i - 1].t:
            errors.append(
                f"Simulation timestamp not increasing at index {i}: "
                f"t={frames[i].t} <= t={frames[i - 1].t}"
            )
            break  # Report only the first violation


def _validate_frame(
    frame: TelemetryFrame,
    index: int,
    errors: list[str],
) -> None:
    """Run deep validation on a single frame's sensor values."""
    prefix = f"Frame[{index}]"

    # --- Simulation time ---
    if not is_valid_simulation_time(frame.t):
        errors.append(f"{prefix}: Negative simulation time: t={frame.t}")

    # --- GPS sanity (×1e7 integer format) ---
    if not is_valid_gps_lat(frame.gps_lat):
        errors.append(
            f"{prefix}: GPS latitude out of range: {frame.gps_lat}"
        )
    if not is_valid_gps_lon(frame.gps_lon):
        errors.append(
            f"{prefix}: GPS longitude out of range: {frame.gps_lon}"
        )

    # --- Battery percentage ---
    if not is_valid_percentage(frame.batt_pct):
        errors.append(
            f"{prefix}: Battery percentage out of range: {frame.batt_pct}"
        )

    # --- Motor damage (should be 0-100%) ---
    for motor_idx, dmg in enumerate([frame.dmg0, frame.dmg1, frame.dmg2, frame.dmg3]):
        if not is_valid_motor_damage(dmg):
            errors.append(
                f"{prefix}: Motor {motor_idx} damage out of range: {dmg}"
            )

    # --- Obstacle sensors (should be non-negative) ---
    for sensor_name, value in [
        ("obs_fwd", frame.obs_fwd),
        ("obs_right", frame.obs_right),
        ("obs_back", frame.obs_back),
        ("obs_left", frame.obs_left),
        ("obs_up", frame.obs_up),
    ]:
        if not is_valid_obstacle_distance(value):
            errors.append(
                f"{prefix}: Obstacle sensor {sensor_name} negative: {value}"
            )

    # --- Crashed drone should not be armed ---
    if frame.crashed == 1 and frame.armed:
        errors.append(
            f"{prefix}: Inconsistency — drone is crashed but still armed"
        )

    # --- RPM consistency: if crashed, RPMs should be zero ---
    if frame.crashed == 1:
        for motor_idx, rpm in enumerate([frame.rpm0, frame.rpm1, frame.rpm2, frame.rpm3]):
            if rpm > 0:
                errors.append(
                    f"{prefix}: Motor {motor_idx} RPM > 0 while crashed: {rpm}"
                )
                break  # One warning is enough
