"""
Pydantic v2 models for drone telemetry data.

Defines the canonical schema for batch telemetry ingestion payloads
matching the live telemetry log format from the simulation tool.

Frame fields use the same short-form keys defined in the project README
(``t``, ``px``, ``py``, ``pz``, ``roll``, ``pitch``, ``yaw``, etc.)
to ensure zero-friction ingestion from the data source.
"""

from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class FlightMode(str, Enum):
    """
    Flight controller modes as defined in the simulation tool.

    Maps directly to the ``mode`` field in each telemetry frame.
    """
    STABILIZED = "stabilized"
    ANGLE = "angle"
    ACRO = "acro"
    ALTHOLD = "althold"
    GPSHOLD = "gpshold"
    RTH = "rth"                # Return-to-home


# ---------------------------------------------------------------------------
# Telemetry Frame — flat structure matching README spec
# ---------------------------------------------------------------------------

class TelemetryFrame(BaseModel):
    """
    A single timestamped telemetry snapshot from the drone simulation.

    Every field corresponds 1:1 to the keys documented in the project
    README under *Live Telemetry Logs (frames)*.
    """

    # --- Timing ---
    t: float = Field(
        ...,
        description="Shared simulation timestamp / clock time",
    )

    # --- 3-D Position ---
    px: float = Field(..., description="Position X coordinate")
    py: float = Field(..., description="Position Y coordinate")
    pz: float = Field(..., description="Position Z coordinate")

    # --- Orientation (Euler angles) ---
    roll: float = Field(..., description="Roll angle (degrees)")
    pitch: float = Field(..., description="Pitch angle (degrees)")
    yaw: float = Field(..., description="Yaw / heading angle (degrees)")

    # --- Gyroscope (angular velocity) ---
    gx: float = Field(..., description="Gyroscope angular velocity X (deg/s)")
    gy: float = Field(..., description="Gyroscope angular velocity Y (deg/s)")
    gz: float = Field(..., description="Gyroscope angular velocity Z (deg/s)")

    # --- Accelerometer (body-frame) ---
    accX: float = Field(..., description="Accelerometer X-axis (m/s²)")
    accY: float = Field(..., description="Accelerometer Y-axis (m/s²)")
    accZ: float = Field(..., description="Accelerometer Z-axis (m/s²)")

    # --- Velocity vector ---
    vx: float = Field(..., description="Velocity X (m/s)")
    vy: float = Field(..., description="Velocity Y (m/s)")
    vz: float = Field(..., description="Velocity Z (m/s)")

    # --- Motor commands (4 motors) ---
    m0: float = Field(..., description="Motor 0 output command")
    m1: float = Field(..., description="Motor 1 output command")
    m2: float = Field(..., description="Motor 2 output command")
    m3: float = Field(..., description="Motor 3 output command")

    # --- Motor RPM (4 motors) ---
    rpm0: float = Field(..., description="Motor 0 RPM")
    rpm1: float = Field(..., description="Motor 1 RPM")
    rpm2: float = Field(..., description="Motor 2 RPM")
    rpm3: float = Field(..., description="Motor 3 RPM")

    # --- Battery ---
    batt: float = Field(..., description="Battery voltage (V)")
    curr: float = Field(..., description="Electrical current draw (A)")
    batt_pct: float = Field(
        ..., ge=0.0, le=100.0,
        description="Battery remaining percentage (0-100)",
    )

    # --- Barometer ---
    baro_raw: float = Field(..., description="Raw barometric altitude reading")
    baro_filtered: float = Field(
        ..., description="Filtered barometric altitude estimate",
    )

    # --- Wind environment ---
    wind_x: float = Field(..., description="Environmental wind vector X")
    wind_z: float = Field(..., description="Environmental wind vector Z")

    # --- Dryden turbulence ---
    dryden_x: float = Field(..., description="Dryden turbulence gust X")
    dryden_y: float = Field(..., description="Dryden turbulence gust Y")
    dryden_z: float = Field(..., description="Dryden turbulence gust Z")

    # --- GPS ---
    gps_lat: int = Field(
        ...,
        description="Simulated GPS latitude (×1e7 integer)",
    )
    gps_lon: int = Field(
        ...,
        description="Simulated GPS longitude (×1e7 integer)",
    )
    gps_fix: int = Field(
        ..., ge=0,
        description="GPS fix type indicator",
    )
    gps_sat: int = Field(
        ..., ge=0,
        description="Count of visible GPS satellites",
    )
    gps_eph: int = Field(
        ..., ge=0,
        description="Estimated horizontal position error",
    )
    gps_epv: int = Field(
        ..., ge=0,
        description="Estimated vertical position error",
    )

    # --- Obstacle distance sensors ---
    obs_fwd: float = Field(..., description="Obstacle distance forward (m)")
    obs_right: float = Field(..., description="Obstacle distance right (m)")
    obs_back: float = Field(..., description="Obstacle distance back (m)")
    obs_left: float = Field(..., description="Obstacle distance left (m)")
    obs_up: float = Field(..., description="Obstacle distance up (m)")

    # --- Flight controller state ---
    mode: FlightMode = Field(
        ...,
        description="Flight controller mode",
    )
    armed: bool = Field(..., description="Drone arm state")
    crashed: int = Field(
        ..., ge=0, le=1,
        description="Crash flag (1=crashed, 0=normal)",
    )
    grounded: int = Field(
        ..., ge=0, le=1,
        description="Ground flag (1=on ground, 0=airborne)",
    )
    ground_y: float = Field(
        ...,
        description="Terrain altitude directly below the drone",
    )

    # --- Motor damage (4 motors: FR, FL, BL, BR) ---
    dmg0: float = Field(
        ..., ge=0.0, le=100.0,
        description="Motor 0 (FR) damage percentage",
    )
    dmg1: float = Field(
        ..., ge=0.0, le=100.0,
        description="Motor 1 (FL) damage percentage",
    )
    dmg2: float = Field(
        ..., ge=0.0, le=100.0,
        description="Motor 2 (BL) damage percentage",
    )
    dmg3: float = Field(
        ..., ge=0.0, le=100.0,
        description="Motor 3 (BR) damage percentage",
    )

    # --- Pilot inputs ---
    input_throttle: float = Field(
        ..., description="Pilot throttle control input",
    )
    input_pitch: float = Field(
        ..., description="Pilot pitch control stick position",
    )
    input_roll: float = Field(
        ..., description="Pilot roll control stick position",
    )
    input_yaw: float = Field(
        ..., description="Pilot yaw control stick position",
    )

    # --- PID controller ---
    pid_roll_err: float = Field(
        ..., description="Roll axis PID loop error",
    )
    pid_roll_out: float = Field(
        ..., description="Roll axis PID control output",
    )
    pid_pitch_err: float = Field(
        ..., description="Pitch axis PID loop error",
    )
    pid_pitch_out: float = Field(
        ..., description="Pitch axis PID control output",
    )
    pid_yaw_err: float = Field(
        ..., description="Yaw axis PID loop error",
    )
    pid_yaw_out: float = Field(
        ..., description="Yaw axis PID control output",
    )
    pid_alt_err: float = Field(
        ..., description="Altitude axis PID loop error",
    )
    pid_alt_out: float = Field(
        ..., description="Altitude axis PID control output",
    )


# ---------------------------------------------------------------------------
# Root Metadata Keys (meta) — matching README spec
# ---------------------------------------------------------------------------

class TelemetryMeta(BaseModel):
    """
    Root metadata block accompanying a telemetry batch.

    Matches the README's *Root Metadata Keys (meta)* section, plus
    the required ``mission_id`` for pipeline routing.
    """

    version: str = Field(
        ..., min_length=1, max_length=32,
        description="Telemetry schema version (e.g. '2.1')",
    )
    drone: str = Field(
        ..., min_length=1, max_length=256,
        description="Active drone profile / parameters configuration",
    )
    exported: str = Field(
        ...,
        description="ISO 8601 timestamp of when the file was generated",
    )
    mission_id: str = Field(
        ..., min_length=1, max_length=128,
        description="Unique mission identifier (attached to every frame)",
    )

    @field_validator("exported")
    @classmethod
    def _validate_exported_iso(cls, v: str) -> str:
        """Ensure the exported timestamp is parseable as ISO 8601."""
        from datetime import datetime
        try:
            datetime.fromisoformat(v)
        except (ValueError, TypeError) as exc:
            raise ValueError(
                f"Invalid ISO 8601 timestamp for 'exported': {v!r}"
            ) from exc
        return v

    @field_validator("mission_id", "drone")
    @classmethod
    def _no_whitespace_only(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Field must not be blank or whitespace-only")
        return v


class TelemetryBatchPayload(BaseModel):
    """
    Top-level request body for the ``POST /ingest`` endpoint.

    Consists of a *meta* block and an ordered list of telemetry *frames*.
    Matches the dataset structure described in the project README.
    """

    meta: TelemetryMeta
    frames: list[TelemetryFrame] = Field(
        ..., min_length=1,
        description="Ordered list of telemetry frames (≥ 1)",
    )
