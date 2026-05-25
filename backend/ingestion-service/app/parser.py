"""
Telemetry frame parser for the ingestion service.

Transforms the ``TelemetryBatchPayload`` (meta + frames) into a list of
``IngestionEvent`` records ready for Kafka publication.

Since the simulation telemetry frames are already flat (matching the
README spec), the parser's main job is to:
1. Attach provenance metadata (mission_id, drone_profile, schema_version)
2. Generate unique ingestion IDs
3. Stamp the ingestion timestamp
"""

from __future__ import annotations

import sys
import uuid
from typing import TYPE_CHECKING

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[3]))

from shared.schemas.event_schema import IngestionEvent
from shared.schemas.telemetry_schema import TelemetryBatchPayload, TelemetryFrame
from shared.utils.time import utc_now_iso

if TYPE_CHECKING:
    pass


def generate_ingestion_id() -> str:
    """
    Generate a globally unique ingestion event identifier.

    Uses UUID-4 for guaranteed uniqueness without coordination.
    """
    return uuid.uuid4().hex


def flatten_frame(
    frame: TelemetryFrame,
    *,
    mission_id: str,
    drone_profile: str,
    schema_version: str,
    exported_at: str,
    ingested_at: str | None = None,
) -> IngestionEvent:
    """
    Convert a ``TelemetryFrame`` into an ``IngestionEvent`` by attaching
    provenance metadata.

    Since frames are already flat (matching the README simulation format),
    all sensor fields are directly transferred.  The parser adds:
    - ``ingestion_id`` — unique event identifier
    - ``mission_id`` — from batch meta
    - ``drone_profile`` — from batch meta
    - ``schema_version`` — from batch meta
    - ``exported_at`` — from batch meta
    - ``ingested_at`` — server-side ingestion timestamp
    """
    ts = ingested_at or utc_now_iso()

    return IngestionEvent(
        # Provenance
        ingestion_id=generate_ingestion_id(),
        mission_id=mission_id,
        drone_profile=drone_profile,
        schema_version=schema_version,
        exported_at=exported_at,
        ingested_at=ts,
        # Timing
        t=frame.t,
        # Position
        px=frame.px,
        py=frame.py,
        pz=frame.pz,
        # Orientation
        roll=frame.roll,
        pitch=frame.pitch,
        yaw=frame.yaw,
        # Gyroscope
        gx=frame.gx,
        gy=frame.gy,
        gz=frame.gz,
        # Accelerometer
        accX=frame.accX,
        accY=frame.accY,
        accZ=frame.accZ,
        # Velocity
        vx=frame.vx,
        vy=frame.vy,
        vz=frame.vz,
        # Motor commands
        m0=frame.m0,
        m1=frame.m1,
        m2=frame.m2,
        m3=frame.m3,
        # Motor RPM
        rpm0=frame.rpm0,
        rpm1=frame.rpm1,
        rpm2=frame.rpm2,
        rpm3=frame.rpm3,
        # Battery
        batt=frame.batt,
        curr=frame.curr,
        batt_pct=frame.batt_pct,
        # Barometer
        baro_raw=frame.baro_raw,
        baro_filtered=frame.baro_filtered,
        # Wind
        wind_x=frame.wind_x,
        wind_z=frame.wind_z,
        # Dryden turbulence
        dryden_x=frame.dryden_x,
        dryden_y=frame.dryden_y,
        dryden_z=frame.dryden_z,
        # GPS
        gps_lat=frame.gps_lat,
        gps_lon=frame.gps_lon,
        gps_fix=frame.gps_fix,
        gps_sat=frame.gps_sat,
        gps_eph=frame.gps_eph,
        gps_epv=frame.gps_epv,
        # Obstacle sensors
        obs_fwd=frame.obs_fwd,
        obs_right=frame.obs_right,
        obs_back=frame.obs_back,
        obs_left=frame.obs_left,
        obs_up=frame.obs_up,
        # Flight controller state
        mode=frame.mode.value,
        armed=frame.armed,
        crashed=frame.crashed,
        grounded=frame.grounded,
        ground_y=frame.ground_y,
        # Motor damage
        dmg0=frame.dmg0,
        dmg1=frame.dmg1,
        dmg2=frame.dmg2,
        dmg3=frame.dmg3,
        # Pilot inputs
        input_throttle=frame.input_throttle,
        input_pitch=frame.input_pitch,
        input_roll=frame.input_roll,
        input_yaw=frame.input_yaw,
        # PID controller
        pid_roll_err=frame.pid_roll_err,
        pid_roll_out=frame.pid_roll_out,
        pid_pitch_err=frame.pid_pitch_err,
        pid_pitch_out=frame.pid_pitch_out,
        pid_yaw_err=frame.pid_yaw_err,
        pid_yaw_out=frame.pid_yaw_out,
        pid_alt_err=frame.pid_alt_err,
        pid_alt_out=frame.pid_alt_out,
    )


def parse_batch(payload: TelemetryBatchPayload) -> list[IngestionEvent]:
    """
    Parse an entire ``TelemetryBatchPayload`` into a list of
    ``IngestionEvent`` records.

    Each frame is enriched with the mission-level metadata from the
    batch ``meta`` block.  A single ``ingested_at`` timestamp is shared
    across all frames in the batch for consistency.

    Returns
    -------
    list[IngestionEvent]
        One event per telemetry frame, in the same order as the input.
    """
    meta = payload.meta
    ingested_at = utc_now_iso()

    return [
        flatten_frame(
            frame,
            mission_id=meta.mission_id,
            drone_profile=meta.drone,
            schema_version=meta.version,
            exported_at=meta.exported,
            ingested_at=ingested_at,
        )
        for frame in payload.frames
    ]
