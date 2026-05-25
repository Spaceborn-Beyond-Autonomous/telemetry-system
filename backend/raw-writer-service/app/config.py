"""
Configuration management for the raw-writer service.

Uses Pydantic ``BaseSettings`` to load configuration from environment
variables with sensible defaults.  Supports ``.env`` files for local
development via ``python-dotenv``.

All environment variables use the ``RAW_WRITER_`` prefix.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """
    Raw-writer-service configuration sourced from environment variables.

    Every field can be overridden by setting the corresponding
    upper-case environment variable (prefix ``RAW_WRITER_``).
    """

    # --- Service ---
    service_name: str = Field(
        default="raw-writer-service",
        description="Logical service name (used in logs & metrics)",
    )
    debug: bool = Field(
        default=False,
        description="Enable debug mode (extra logging)",
    )

    # --- Kafka Consumer ---
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Comma-separated Kafka broker addresses",
    )
    kafka_topic: str = Field(
        default="telemetry.raw",
        description="Kafka topic to consume from",
    )
    kafka_group_id: str = Field(
        default="raw-writer-group",
        description="Consumer group ID for offset management",
    )
    kafka_auto_offset_reset: str = Field(
        default="earliest",
        description="Where to start if no committed offset exists (earliest/latest)",
    )
    kafka_enable_auto_commit: bool = Field(
        default=False,
        description="Disable auto-commit; we commit after successful DB write",
    )
    kafka_max_poll_records: int = Field(
        default=500,
        ge=1,
        description="Max records per poll batch (controls DB insert batch size)",
    )
    kafka_session_timeout_ms: int = Field(
        default=30_000,
        ge=6000,
        description="Session timeout for consumer group membership",
    )
    kafka_heartbeat_interval_ms: int = Field(
        default=10_000,
        ge=1000,
        description="Heartbeat interval for consumer group",
    )
    kafka_fetch_max_wait_ms: int = Field(
        default=500,
        ge=100,
        description="Maximum wait time for fetch requests in ms",
    )

    # --- TimescaleDB / PostgreSQL ---
    db_host: str = Field(
        default="localhost",
        description="TimescaleDB host",
    )
    db_port: int = Field(
        default=5432,
        ge=1, le=65535,
        description="TimescaleDB port",
    )
    db_name: str = Field(
        default="telemetry",
        description="Database name",
    )
    db_user: str = Field(
        default="telemetry",
        description="Database user",
    )
    db_password: str = Field(
        default="telemetry",
        description="Database password",
    )
    db_pool_min_size: int = Field(
        default=5,
        ge=1,
        description="Minimum number of connections in the pool",
    )
    db_pool_max_size: int = Field(
        default=20,
        ge=1,
        description="Maximum number of connections in the pool",
    )
    db_command_timeout: float = Field(
        default=60.0,
        gt=0,
        description="Default command timeout in seconds",
    )
    db_ssl: bool = Field(
        default=False,
        description="Enable SSL for database connections",
    )

    # --- Batch Processing ---
    batch_insert_size: int = Field(
        default=500,
        ge=1,
        description="Number of records to batch-insert per DB transaction",
    )
    consumer_poll_timeout_ms: int = Field(
        default=1000,
        ge=100,
        description="Timeout for Kafka consumer getmany() in ms",
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

    @field_validator("log_level")
    @classmethod
    def _normalise_log_level(cls, v: str) -> str:
        return v.upper()

    @field_validator("kafka_auto_offset_reset")
    @classmethod
    def _validate_offset_reset(cls, v: str) -> str:
        allowed = {"earliest", "latest", "none"}
        if v not in allowed:
            raise ValueError(
                f"kafka_auto_offset_reset must be one of {allowed}, got {v!r}"
            )
        return v

    @property
    def dsn(self) -> str:
        """Build a PostgreSQL DSN string from individual components."""
        scheme = "postgresql"
        return (
            f"{scheme}://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    model_config = {
        "env_prefix": "RAW_WRITER_",
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
