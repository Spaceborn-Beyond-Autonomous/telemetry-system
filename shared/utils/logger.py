"""
Structured logging utilities for the telemetry system.

Provides JSON-formatted structured logging with correlation ID propagation,
configurable log levels, and service-name tagging for microservice observability.
"""

from __future__ import annotations

import logging
import os
import sys
import uuid
from contextvars import ContextVar
from typing import Any

import structlog

# ---------------------------------------------------------------------------
# Context-local correlation ID — propagated across async tasks automatically
# ---------------------------------------------------------------------------
_correlation_id_ctx: ContextVar[str | None] = ContextVar(
    "correlation_id", default=None
)


def get_correlation_id() -> str:
    """Return the current correlation ID, generating one if absent."""
    cid = _correlation_id_ctx.get()
    if cid is None:
        cid = uuid.uuid4().hex
        _correlation_id_ctx.set(cid)
    return cid


def set_correlation_id(cid: str) -> None:
    """Explicitly set the correlation ID for the current context."""
    _correlation_id_ctx.set(cid)


# ---------------------------------------------------------------------------
# Structlog processor that injects the correlation ID
# ---------------------------------------------------------------------------
def _inject_correlation_id(
    logger: Any, method_name: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    """Structlog processor: attach correlation_id to every log entry."""
    event_dict.setdefault("correlation_id", get_correlation_id())
    return event_dict


# ---------------------------------------------------------------------------
# Public configuration entry-point
# ---------------------------------------------------------------------------
def configure_logging(
    *,
    service_name: str = "telemetry-service",
    log_level: str | None = None,
    json_output: bool | None = None,
) -> None:
    """
    Initialise structured logging for the calling service.

    Parameters
    ----------
    service_name:
        Added to every log line under the ``service`` key.
    log_level:
        Python log-level name (DEBUG, INFO, WARNING, …).
        Falls back to the ``LOG_LEVEL`` environment variable, then ``INFO``.
    json_output:
        When *True*, emit JSON lines (production).  When *False*, use
        coloured, human-readable console output (development).
        Falls back to ``LOG_JSON`` environment variable, then *True*.
    """
    level = (log_level or os.getenv("LOG_LEVEL", "INFO")).upper()
    numeric_level = getattr(logging, level, logging.INFO)

    if json_output is None:
        json_output = os.getenv("LOG_JSON", "true").lower() in ("1", "true", "yes")

    # Shared processors applied to every log entry
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        _inject_correlation_id,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]

    if json_output:
        renderer: structlog.types.Processor = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=True)

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
        foreign_pre_chain=shared_processors,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(numeric_level)

    # Silence noisy third-party loggers
    for noisy in ("aiokafka", "kafka", "uvicorn.access"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """
    Return a *bound* structured logger.

    Usage::

        logger = get_logger(__name__)
        logger.info("ingestion_started", mission_id="m-42", frames=120)
    """
    return structlog.get_logger(name)
