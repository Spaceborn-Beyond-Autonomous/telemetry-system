"""
Pydantic v2 models for Kafka ingestion events.

Defines the envelope schema used when publishing flattened telemetry
frames to Kafka topics (e.g. ``telemetry.raw``).  Each event carries
the original simulation sensor data **plus** provenance metadata
injected by the ingestion service.

The flattened fields match 1:1 with the README's *Live Telemetry Logs*
specification.
"""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field


class IngestionEvent(BaseModel):
    """
    A single flattened telemetry record ready for Kafka publication.

    Produced by the ingestion service parser — combines the raw
    simulation telemetry values with mission / provenance context so
    downstream consumers have everything in one self-contained message.
    """

    # ---- Provenance / routing ----
    ingestion_id: str = Field(
        ..., description="Unique identifier for this ingestion event",
    )
    mission_id: str = Field(
        ..., description="Mission this frame belongs to",
    )
    drone_profile: str = Field(
        ..., description="Drone profile configuration from meta",
    )
    schema_version: str = Field(
        ..., description="Telemetry schema version from meta",
    )
    exported_at: str = Field(
        ..., description="ISO 8601 timestamp of when the batch was exported",
    )
    ingested_at: str = Field(
        ..., description="ISO 8601 timestamp of when the frame was ingested",
    )

    # ---- Timing ----
    t: float = Field(..., description="Simulation clock time")

    # ---- 3-D Position ----
    px: float
    py: float
    pz: float

    # ---- Orientation ----
    roll: float
    pitch: float
    yaw: float

    # ---- Gyroscope ----
    gx: float
    gy: float
    gz: float

    # ---- Accelerometer ----
    accX: float
    accY: float
    accZ: float

    # ---- Velocity ----
    vx: float
    vy: float
    vz: float

    # ---- Motor commands ----
    m0: float
    m1: float
    m2: float
    m3: float

    # ---- Motor RPM ----
    rpm0: float
    rpm1: float
    rpm2: float
    rpm3: float

    # ---- Battery ----
    batt: float
    curr: float
    batt_pct: float

    # ---- Barometer ----
    baro_raw: float
    baro_filtered: float

    # ---- Wind environment ----
    wind_x: float
    wind_z: float

    # ---- Dryden turbulence ----
    dryden_x: float
    dryden_y: float
    dryden_z: float

    # ---- GPS (×1e7 integers) ----
    gps_lat: int
    gps_lon: int
    gps_fix: int
    gps_sat: int
    gps_eph: int
    gps_epv: int

    # ---- Obstacle sensors ----
    obs_fwd: float
    obs_right: float
    obs_back: float
    obs_left: float
    obs_up: float

    # ---- Flight controller state ----
    mode: str
    armed: bool
    crashed: int
    grounded: int
    ground_y: float

    # ---- Motor damage (FR, FL, BL, BR) ----
    dmg0: float
    dmg1: float
    dmg2: float
    dmg3: float

    # ---- Pilot inputs ----
    input_throttle: float
    input_pitch: float
    input_roll: float
    input_yaw: float

    # ---- PID controller ----
    pid_roll_err: float
    pid_roll_out: float
    pid_pitch_err: float
    pid_pitch_out: float
    pid_yaw_err: float
    pid_yaw_out: float
    pid_alt_err: float
    pid_alt_out: float

    def to_kafka_value(self) -> dict[str, Any]:
        """Serialise the event to a dict suitable for Kafka JSON encoding."""
        return self.model_dump(mode="json", exclude_none=False)
