"""
Application entry-point for the ingestion service.

Creates the FastAPI application, wires up the Kafka producer lifecycle,
configures middleware, and mounts the API router.

Run locally with::

    uvicorn app.main:app --host 0.0.0.0 --port 8001 --reload
"""

from __future__ import annotations

import sys
import time
import uuid
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import ValidationError as PydanticValidationError

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[3]))

from shared.utils.logger import configure_logging, get_logger, set_correlation_id

from app.api import router
from app.config import get_settings
from app.kafka_producer import TelemetryKafkaProducer


# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """
    Manage application startup and shutdown.

    * Initialises structured logging.
    * Starts the Kafka producer.
    * On shutdown, gracefully stops the producer.
    """
    settings = get_settings()

    # --- Logging ---
    configure_logging(
        service_name=settings.service_name,
        log_level=settings.log_level,
        json_output=settings.log_json,
    )
    logger = get_logger(__name__)
    logger.info(
        "service_starting",
        service=settings.service_name,
        port=settings.service_port,
    )

    # --- Kafka producer ---
    producer = TelemetryKafkaProducer(settings)
    try:
        await producer.start()
    except Exception:
        logger.exception("kafka_producer_start_failed")
        # Allow the service to start even if Kafka is temporarily down;
        # the /ready probe will report not-ready.
        producer = TelemetryKafkaProducer(settings)  # fresh un-started instance

    app.state.kafka_producer = producer
    app.state.start_time = time.time()

    logger.info("service_started", service=settings.service_name)

    yield  # ← application is running

    # --- Shutdown ---
    logger.info("service_stopping", service=settings.service_name)
    await producer.stop()
    logger.info("service_stopped", service=settings.service_name)


def create_app() -> FastAPI:
    """Build and return the configured FastAPI application."""
    settings = get_settings()

    app = FastAPI(
        title="Telemetry Ingestion Service",
        description=(
            "Production-grade ingestion endpoint for drone telemetry data. "
            "Validates, flattens, and publishes telemetry frames to Kafka."
        ),
        version="1.0.0",
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    # --- CORS ---
    origins = [o.strip() for o in settings.cors_origins.split(",") if o.strip()]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # --- Correlation-ID middleware ---
    @app.middleware("http")
    async def correlation_id_middleware(
        request: Request, call_next
    ) -> Response:
        """
        Inject a correlation ID into every request.

        If the client sends ``X-Correlation-ID``, it is reused; otherwise
        a new UUID is generated.  The ID is propagated through structured
        logs and returned in the response header.
        """
        cid = request.headers.get("X-Correlation-ID", uuid.uuid4().hex)
        set_correlation_id(cid)

        response: Response = await call_next(request)
        response.headers["X-Correlation-ID"] = cid
        return response

    # --- Request-size guard middleware ---
    @app.middleware("http")
    async def payload_size_middleware(
        request: Request, call_next
    ) -> Response:
        """Reject requests exceeding the configured maximum payload size."""
        content_length = request.headers.get("content-length")
        if content_length and int(content_length) > settings.max_payload_bytes:
            return JSONResponse(
                status_code=413,
                content={
                    "error": "Payload too large",
                    "detail": (
                        f"Request body ({int(content_length)} bytes) exceeds "
                        f"maximum allowed size ({settings.max_payload_bytes} bytes)"
                    ),
                },
            )
        return await call_next(request)

    # --- Global Pydantic validation error handler ---
    @app.exception_handler(PydanticValidationError)
    async def pydantic_validation_handler(
        request: Request, exc: PydanticValidationError
    ) -> JSONResponse:
        """Return a structured 422 response for Pydantic validation errors."""
        return JSONResponse(
            status_code=422,
            content={
                "error": "Schema validation error",
                "detail": exc.errors(),
            },
        )

    # --- Mount router ---
    app.include_router(router, prefix="/api/v1")

    return app


# Create the application instance for uvicorn
app = create_app()
