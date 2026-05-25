"""
FastAPI route definitions for the ingestion service.

Exposes:
* ``POST /ingest``  — accept batch telemetry JSON
* ``GET  /health``  — liveness / readiness probe
* ``GET  /ready``   — deep readiness (Kafka connectivity)
"""

from __future__ import annotations

import sys
import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field, ValidationError as PydanticValidationError

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[3]))

from shared.schemas.telemetry_schema import TelemetryBatchPayload
from shared.utils.logger import get_logger, get_correlation_id

from app.config import Settings, get_settings
from app.kafka_producer import TelemetryKafkaProducer
from app.parser import parse_batch
from app.validator import ValidationError, validate_batch

logger = get_logger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

class IngestionResponse(BaseModel):
    """Successful ingestion response."""
    status: str = "accepted"
    correlation_id: str
    mission_id: str
    drone_profile: str
    frames_received: int
    frames_published: int
    processing_time_ms: float


class HealthResponse(BaseModel):
    """Liveness probe response."""
    status: str = "healthy"
    service: str = "ingestion-service"
    uptime_seconds: float = 0.0


class ErrorDetail(BaseModel):
    """Structured error response."""
    error: str
    detail: str | list[str] | None = None
    correlation_id: str | None = None


# ---------------------------------------------------------------------------
# Dependency injection helpers
# ---------------------------------------------------------------------------

def _get_producer(request: Request) -> TelemetryKafkaProducer:
    """Extract the Kafka producer from app state."""
    producer: TelemetryKafkaProducer | None = getattr(
        request.app.state, "kafka_producer", None
    )
    if producer is None or not producer.is_running:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Kafka producer is not available",
        )
    return producer


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get(
    "/health",
    response_model=HealthResponse,
    tags=["observability"],
    summary="Liveness probe",
)
async def health(request: Request) -> HealthResponse:
    """Return service health status for container orchestrators."""
    start_time: float = getattr(request.app.state, "start_time", time.time())
    return HealthResponse(
        status="healthy",
        service="ingestion-service",
        uptime_seconds=round(time.time() - start_time, 2),
    )


@router.get(
    "/ready",
    response_model=HealthResponse,
    tags=["observability"],
    summary="Readiness probe",
)
async def readiness(
    request: Request,
    producer: TelemetryKafkaProducer = Depends(_get_producer),
) -> HealthResponse:
    """
    Deep readiness check — verifies Kafka producer connectivity.

    Returns 503 if the producer is not connected.
    """
    start_time: float = getattr(request.app.state, "start_time", time.time())
    return HealthResponse(
        status="ready",
        service="ingestion-service",
        uptime_seconds=round(time.time() - start_time, 2),
    )


@router.post(
    "/ingest",
    response_model=IngestionResponse,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["ingestion"],
    summary="Ingest batch telemetry data",
    responses={
        400: {"model": ErrorDetail, "description": "Invalid payload"},
        413: {"model": ErrorDetail, "description": "Payload too large"},
        422: {"model": ErrorDetail, "description": "Schema validation error"},
        503: {"model": ErrorDetail, "description": "Kafka unavailable"},
    },
)
async def ingest_telemetry(
    payload: TelemetryBatchPayload,
    request: Request,
    settings: Settings = Depends(get_settings),
    producer: TelemetryKafkaProducer = Depends(_get_producer),
) -> IngestionResponse:
    """
    Accept a batch of drone telemetry frames from the simulation tool.

    Pipeline:
    1. Pydantic schema validation (automatic via type annotation)
    2. Business-rule validation (timestamp ordering, sensor ranges, etc.)
    3. Attach provenance metadata (mission_id, ingestion_id, timestamps)
    4. Publish each event to Kafka ``telemetry.raw``
    5. Return summary response

    The request body must conform to ``TelemetryBatchPayload``
    with the format defined in the project README::

        {
          "meta": {
            "version": "2.1",
            "drone": "default_quad",
            "exported": "2026-05-25T03:00:00Z",
            "mission_id": "mission-2026-001"
          },
          "frames": [
            {
              "t": 0.0,
              "px": 0.0, "py": 10.0, "pz": 0.0,
              "roll": 0.0, "pitch": 0.0, "yaw": 0.0,
              ...
            }
          ]
        }
    """
    t_start = time.perf_counter()
    correlation_id = get_correlation_id()

    logger.info(
        "ingestion_started",
        mission_id=payload.meta.mission_id,
        drone_profile=payload.meta.drone,
        schema_version=payload.meta.version,
        frame_count=len(payload.frames),
        correlation_id=correlation_id,
    )

    # ------------------------------------------------------------------
    # 1. Business-rule validation
    # ------------------------------------------------------------------
    try:
        validate_batch(
            payload,
            max_frames=settings.max_batch_frames,
            enforce_timestamp_order=settings.enforce_timestamp_order,
        )
    except ValidationError as exc:
        logger.warning(
            "ingestion_validation_failed",
            mission_id=payload.meta.mission_id,
            errors=exc.errors,
            correlation_id=correlation_id,
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "Validation failed",
                "detail": exc.errors,
                "correlation_id": correlation_id,
            },
        )

    # ------------------------------------------------------------------
    # 2. Parse & attach provenance metadata
    # ------------------------------------------------------------------
    events = parse_batch(payload)

    logger.info(
        "frames_parsed",
        event_count=len(events),
        mission_id=payload.meta.mission_id,
        correlation_id=correlation_id,
    )

    # ------------------------------------------------------------------
    # 3. Publish to Kafka
    # ------------------------------------------------------------------
    try:
        published = await producer.publish_events(events)
    except Exception:
        logger.exception(
            "ingestion_kafka_error",
            mission_id=payload.meta.mission_id,
            correlation_id=correlation_id,
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": "Failed to publish to message broker",
                "correlation_id": correlation_id,
            },
        )

    # ------------------------------------------------------------------
    # 4. Response
    # ------------------------------------------------------------------
    elapsed_ms = round((time.perf_counter() - t_start) * 1000, 2)

    logger.info(
        "ingestion_completed",
        mission_id=payload.meta.mission_id,
        drone_profile=payload.meta.drone,
        frames_received=len(payload.frames),
        frames_published=published,
        processing_time_ms=elapsed_ms,
        correlation_id=correlation_id,
    )

    return IngestionResponse(
        status="accepted",
        correlation_id=correlation_id,
        mission_id=payload.meta.mission_id,
        drone_profile=payload.meta.drone,
        frames_received=len(payload.frames),
        frames_published=published,
        processing_time_ms=elapsed_ms,
    )
