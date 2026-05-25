"""
Pydantic v2 models for mission-level information.

Used to associate telemetry batches with specific missions and to
validate mission metadata at ingestion boundaries.  Aligned with the
simulation tool's meta format.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class MissionInfo(BaseModel):
    """
    Describes a drone mission.

    Captures the identifiers, timing, and optional descriptive fields
    used throughout the telemetry pipeline to correlate frames with
    their originating mission context.
    """

    mission_id: str = Field(
        ..., min_length=1, max_length=128,
        description="Globally unique mission identifier",
    )
    drone_profile: str = Field(
        ..., min_length=1, max_length=256,
        description="Drone profile / parameters configuration used",
    )
    schema_version: str = Field(
        ..., min_length=1, max_length=32,
        description="Telemetry schema version (e.g. '2.1')",
    )
    exported: Optional[str] = Field(
        default=None,
        description="ISO 8601 timestamp of when the data was exported",
    )
    mission_name: Optional[str] = Field(
        default=None, max_length=256,
        description="Human-readable mission name / label",
    )
    start_time: Optional[str] = Field(
        default=None,
        description="Mission start time (ISO 8601)",
    )
    end_time: Optional[str] = Field(
        default=None,
        description="Mission end time (ISO 8601), null if still running",
    )
    region: Optional[str] = Field(
        default=None, max_length=256,
        description="Geographic region or zone label",
    )
    operator_id: Optional[str] = Field(
        default=None, max_length=128,
        description="Identifier of the human or system operator",
    )

    # --- validators ---

    @field_validator("exported", "start_time", "end_time", mode="before")
    @classmethod
    def _validate_iso_timestamps(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            try:
                datetime.fromisoformat(v)
            except (ValueError, TypeError) as exc:
                raise ValueError(
                    f"Invalid ISO 8601 timestamp: {v!r}"
                ) from exc
        return v

    @model_validator(mode="after")
    def _end_after_start(self) -> "MissionInfo":
        if self.start_time and self.end_time:
            start = datetime.fromisoformat(self.start_time)
            end = datetime.fromisoformat(self.end_time)
            if end < start:
                raise ValueError(
                    "end_time must be equal to or after start_time"
                )
        return self

    @field_validator("mission_id", "drone_profile")
    @classmethod
    def _no_whitespace_only(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Field must not be blank or whitespace-only")
        return v
