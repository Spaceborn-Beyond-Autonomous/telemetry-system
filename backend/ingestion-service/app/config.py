"""
Configuration management for the ingestion service.

Uses Pydantic ``BaseSettings`` to load configuration from environment
variables with sensible defaults.  Supports ``.env`` files for local
development via ``python-dotenv``.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """
    Ingestion-service configuration sourced from environment variables.

    Every field can be overridden by setting the corresponding
    upper-case environment variable (prefix ``INGESTION_``).
    """

    # --- Service ---
    service_name: str = Field(
        default="ingestion-service",
        description="Logical service name (used in logs & metrics)",
    )
    service_host: str = Field(
        default="0.0.0.0",
        description="Bind address for the HTTP server",
    )
    service_port: int = Field(
        default=8001,
        ge=1, le=65535,
        description="HTTP port to listen on",
    )
    debug: bool = Field(
        default=False,
        description="Enable debug mode (extra logging, reloading)",
    )

    # --- Kafka ---
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Comma-separated Kafka broker addresses",
    )
    kafka_topic: str = Field(
        default="telemetry.raw",
        description="Kafka topic to publish ingested frames to",
    )
    kafka_acks: str = Field(
        default="all",
        description="Kafka producer acks setting (0, 1, or 'all')",
    )
    kafka_retries: int = Field(
        default=3,
        ge=0,
        description="Number of send retries on transient failures",
    )
    kafka_retry_backoff_ms: int = Field(
        default=200,
        ge=0,
        description="Backoff between retries in milliseconds",
    )
    kafka_batch_size: int = Field(
        default=16384,
        ge=0,
        description="Kafka producer batch size in bytes",
    )
    kafka_linger_ms: int = Field(
        default=10,
        ge=0,
        description="Kafka producer linger time in milliseconds",
    )
    kafka_compression_type: Optional[str] = Field(
        default="gzip",
        description="Compression codec: gzip, snappy, lz4, zstd, or None",
    )
    kafka_max_request_size: int = Field(
        default=1_048_576,
        ge=1,
        description="Maximum size of a Kafka produce request in bytes",
    )
    kafka_request_timeout_ms: int = Field(
        default=30_000,
        ge=1000,
        description="Kafka produce request timeout in milliseconds",
    )

    # --- Logging ---
    log_level: str = Field(
        default="INFO",
        description="Python log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )
    log_json: bool = Field(
        default=True,
        description="Emit JSON-structured log lines when True",
    )

    # --- Validation ---
    max_batch_frames: int = Field(
        default=10_000,
        ge=1,
        description="Maximum number of frames accepted in a single batch",
    )
    max_payload_bytes: int = Field(
        default=10_485_760,  # 10 MiB
        ge=1,
        description="Maximum request body size in bytes",
    )
    enforce_timestamp_order: bool = Field(
        default=True,
        description="Reject batches whose frame timestamps are not monotonically increasing",
    )

    # --- CORS ---
    cors_origins: str = Field(
        default="*",
        description="Comma-separated allowed CORS origins",
    )

    @field_validator("log_level")
    @classmethod
    def _normalise_log_level(cls, v: str) -> str:
        return v.upper()

    @field_validator("kafka_acks")
    @classmethod
    def _validate_acks(cls, v: str) -> str:
        allowed = {"0", "1", "all"}
        if v not in allowed:
            raise ValueError(f"kafka_acks must be one of {allowed}, got {v!r}")
        return v

    model_config = {
        "env_prefix": "INGESTION_",
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": False,
    }


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Return the cached application settings singleton.

    The first call reads from environment variables / ``.env``; subsequent
    calls return the same instance.
    """
    return Settings()
