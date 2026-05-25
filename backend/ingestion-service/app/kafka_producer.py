"""
Async Kafka producer for the ingestion service.

Wraps ``aiokafka.AIOKafkaProducer`` with lifecycle management,
structured logging, and JSON serialisation via ``orjson``.

Usage (within a FastAPI lifespan)::

    producer = TelemetryKafkaProducer(settings)
    await producer.start()
    ...
    await producer.publish_events(events)
    ...
    await producer.stop()
"""

from __future__ import annotations

import asyncio
import sys
from typing import TYPE_CHECKING, Sequence

import orjson
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[3]))

from shared.schemas.event_schema import IngestionEvent
from shared.utils.logger import get_logger

if TYPE_CHECKING:
    from app.config import Settings

logger = get_logger(__name__)


def _serialise_value(value: dict) -> bytes:
    """Serialise a dict to compact JSON bytes using orjson."""
    return orjson.dumps(value)


def _serialise_key(key: str) -> bytes:
    """Encode a string key to UTF-8 bytes."""
    return key.encode("utf-8")


class TelemetryKafkaProducer:
    """
    High-level async Kafka producer for telemetry ingestion events.

    Attributes
    ----------
    _producer : AIOKafkaProducer | None
        The underlying aiokafka producer instance (``None`` until started).
    """

    def __init__(self, settings: "Settings") -> None:
        self._settings = settings
        self._producer: AIOKafkaProducer | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """
        Initialise and start the Kafka producer connection.

        Raises ``RuntimeError`` if the producer is already running.
        """
        if self._producer is not None:
            raise RuntimeError("Kafka producer is already running")

        logger.info(
            "kafka_producer_starting",
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            topic=self._settings.kafka_topic,
            acks=self._settings.kafka_acks,
        )

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            acks=self._settings.kafka_acks if self._settings.kafka_acks == "all" else int(self._settings.kafka_acks),
            retries=self._settings.kafka_retries,
            retry_backoff_ms=self._settings.kafka_retry_backoff_ms,
            batch_size=self._settings.kafka_batch_size,
            linger_ms=self._settings.kafka_linger_ms,
            compression_type=self._settings.kafka_compression_type,
            max_request_size=self._settings.kafka_max_request_size,
            request_timeout_ms=self._settings.kafka_request_timeout_ms,
            value_serializer=_serialise_value,
            key_serializer=_serialise_key,
        )

        await self._producer.start()
        logger.info("kafka_producer_started")

    async def stop(self) -> None:
        """Flush pending messages and close the producer connection."""
        if self._producer is not None:
            logger.info("kafka_producer_stopping")
            await self._producer.stop()
            self._producer = None
            logger.info("kafka_producer_stopped")

    @property
    def is_running(self) -> bool:
        """Return ``True`` if the producer is currently connected."""
        return self._producer is not None

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------

    async def publish_event(self, event: IngestionEvent) -> None:
        """
        Publish a single ``IngestionEvent`` to the configured Kafka topic.

        The Kafka message key is set to the ``mission_id`` so all frames
        from the same mission land in the same partition, preserving order.

        Raises
        ------
        RuntimeError
            If the producer has not been started.
        KafkaError
            On unrecoverable Kafka send failures.
        """
        if self._producer is None:
            raise RuntimeError("Kafka producer is not running — call start() first")

        topic = self._settings.kafka_topic
        key = event.mission_id
        value = event.to_kafka_value()

        try:
            record_metadata = await self._producer.send_and_wait(
                topic, value=value, key=key,
            )
            logger.debug(
                "kafka_message_sent",
                topic=record_metadata.topic,
                partition=record_metadata.partition,
                offset=record_metadata.offset,
                ingestion_id=event.ingestion_id,
            )
        except KafkaError:
            logger.exception(
                "kafka_send_failed",
                topic=topic,
                ingestion_id=event.ingestion_id,
            )
            raise

    async def publish_events(
        self,
        events: Sequence[IngestionEvent],
    ) -> int:
        """
        Publish multiple events concurrently using ``asyncio.gather``.

        Returns the count of successfully published events.

        Individual send failures are logged but do **not** abort the
        entire batch — partial success is possible.
        """
        if self._producer is None:
            raise RuntimeError("Kafka producer is not running — call start() first")

        topic = self._settings.kafka_topic
        published = 0

        # Fire all sends concurrently for maximum throughput
        tasks = []
        for event in events:
            tasks.append(
                self._producer.send_and_wait(
                    topic,
                    value=event.to_kafka_value(),
                    key=event.mission_id,
                )
            )

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, result in enumerate(results):
            if isinstance(result, BaseException):
                logger.error(
                    "kafka_send_failed",
                    ingestion_id=events[i].ingestion_id,
                    error=str(result),
                )
            else:
                published += 1
                logger.debug(
                    "kafka_message_sent",
                    topic=result.topic,
                    partition=result.partition,
                    offset=result.offset,
                    ingestion_id=events[i].ingestion_id,
                )

        logger.info(
            "kafka_batch_published",
            total=len(events),
            published=published,
            failed=len(events) - published,
        )

        return published
