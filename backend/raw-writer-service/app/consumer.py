"""
Async Kafka consumer for the raw-writer service.

Reads ``IngestionEvent`` JSON messages from the ``telemetry.raw`` topic,
converts them to DB records via :func:`models.event_to_record`, and
batch-inserts into TimescaleDB via :class:`db.DatabaseManager`.

Key design decisions:
* **Stateless** — no local state beyond the Kafka consumer offset.
* **Manual commit** — offsets are committed only after a successful DB write.
* **Batch processing** — messages are accumulated up to ``batch_insert_size``
  before flushing to the database in a single ``executemany`` call.
* **Reconnect logic** — retries on both Kafka and DB transient failures
  with exponential backoff.
* **Idempotency** — ``ON CONFLICT (ingestion_id) DO NOTHING`` in the DB
  layer ensures that reprocessed messages (after consumer restart) do not
  create duplicates.
"""

from __future__ import annotations

import asyncio
import sys
from typing import TYPE_CHECKING

import orjson
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[3]))

from shared.utils.logger import get_logger

from app.db import DatabaseManager
from app.models import event_to_record

if TYPE_CHECKING:
    from app.config import Settings

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MAX_RECONNECT_DELAY = 60  # seconds
_INITIAL_RECONNECT_DELAY = 1  # seconds


def _deserialise(raw: bytes) -> dict:
    """Deserialise a Kafka message value from JSON bytes."""
    return orjson.loads(raw)


# ---------------------------------------------------------------------------
# Raw Writer Consumer
# ---------------------------------------------------------------------------

class RawWriterConsumer:
    """
    Stateless Kafka consumer that writes telemetry events to TimescaleDB.

    The consumer loop:
    1. Poll a batch of messages from Kafka
    2. Deserialise and convert to DB record tuples
    3. Batch-insert into ``telemetry_raw``
    4. Commit Kafka offsets

    On transient failures the consumer retries with exponential backoff.
    On fatal failures the consumer shuts down cleanly.
    """

    def __init__(
        self,
        settings: "Settings",
        db: DatabaseManager,
    ) -> None:
        self._settings = settings
        self._db = db
        self._consumer: AIOKafkaConsumer | None = None
        self._running = False
        self._reconnect_delay = _INITIAL_RECONNECT_DELAY

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Initialise the Kafka consumer and connect to brokers."""
        if self._consumer is not None:
            raise RuntimeError("Consumer is already running")

        await self._create_consumer()

    async def _create_consumer(self) -> None:
        """Create and start the underlying aiokafka consumer."""
        logger.info(
            "consumer_starting",
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            topic=self._settings.kafka_topic,
            group_id=self._settings.kafka_group_id,
        )

        self._consumer = AIOKafkaConsumer(
            self._settings.kafka_topic,
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            group_id=self._settings.kafka_group_id,
            auto_offset_reset=self._settings.kafka_auto_offset_reset,
            enable_auto_commit=self._settings.kafka_enable_auto_commit,
            max_poll_records=self._settings.kafka_max_poll_records,
            session_timeout_ms=self._settings.kafka_session_timeout_ms,
            heartbeat_interval_ms=self._settings.kafka_heartbeat_interval_ms,
            fetch_max_wait_ms=self._settings.kafka_fetch_max_wait_ms,
            value_deserializer=_deserialise,
        )

        await self._consumer.start()
        self._reconnect_delay = _INITIAL_RECONNECT_DELAY
        logger.info("consumer_started")

    async def stop(self) -> None:
        """Stop the consumer and release resources."""
        self._running = False
        if self._consumer is not None:
            logger.info("consumer_stopping")
            await self._consumer.stop()
            self._consumer = None
            logger.info("consumer_stopped")

    @property
    def is_running(self) -> bool:
        """Return ``True`` if the consumer loop is active."""
        return self._running

    # ------------------------------------------------------------------
    # Main consume loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """
        Main consume-process-commit loop.

        Runs until ``stop()`` is called or a fatal error occurs.
        Handles transient errors via exponential-backoff reconnection.
        """
        self._running = True
        logger.info("consumer_loop_started")

        while self._running:
            try:
                await self._consume_batch()
            except KafkaError as exc:
                logger.error(
                    "consumer_kafka_error",
                    error=str(exc),
                )
                await self._reconnect()
            except asyncio.CancelledError:
                logger.info("consumer_loop_cancelled")
                break
            except Exception:
                logger.exception("consumer_unexpected_error")
                await self._reconnect()

        logger.info("consumer_loop_stopped")

    async def _consume_batch(self) -> None:
        """
        Poll one batch, convert, insert, and commit.

        Uses ``getmany()`` with a timeout to poll up to
        ``max_poll_records`` messages, then processes them as a single
        DB batch.
        """
        if self._consumer is None:
            await self._reconnect()
            return

        # Poll messages — returns dict of {TopicPartition: [messages]}
        timeout_ms = self._settings.consumer_poll_timeout_ms
        batch = await self._consumer.getmany(
            timeout_ms=timeout_ms,
        )

        if not batch:
            return  # No messages available, loop again

        # Flatten all messages from all partitions
        messages = []
        for tp, msgs in batch.items():
            messages.extend(msgs)

        if not messages:
            return

        # Convert to DB records
        records = []
        errors = 0
        for msg in messages:
            try:
                record = event_to_record(msg.value)
                records.append(record)
            except (KeyError, ValueError, TypeError) as exc:
                errors += 1
                logger.warning(
                    "consumer_record_conversion_failed",
                    offset=msg.offset,
                    partition=msg.partition,
                    error=str(exc),
                )

        if errors > 0:
            logger.warning(
                "consumer_batch_conversion_errors",
                total=len(messages),
                failed=errors,
                success=len(records),
            )

        # Batch insert into TimescaleDB
        if records:
            inserted = await self._insert_with_retry(records)
            logger.info(
                "consumer_batch_processed",
                polled=len(messages),
                converted=len(records),
                inserted=inserted,
                conversion_errors=errors,
            )

        # Commit offsets after successful DB write
        await self._consumer.commit()
        logger.debug("consumer_offsets_committed")

    # ------------------------------------------------------------------
    # DB insert with retry
    # ------------------------------------------------------------------

    async def _insert_with_retry(
        self,
        records: list[tuple],
        max_retries: int = 3,
    ) -> int:
        """
        Insert records into the database with retry on transient failures.

        Uses exponential backoff between retries.  On final failure,
        the exception propagates to the main loop for reconnect handling.
        """
        delay = 1.0
        for attempt in range(1, max_retries + 1):
            try:
                return await self._db.insert_batch(records)
            except Exception as exc:
                if attempt == max_retries:
                    logger.error(
                        "db_insert_retries_exhausted",
                        attempts=max_retries,
                        error=str(exc),
                    )
                    raise
                logger.warning(
                    "db_insert_retry",
                    attempt=attempt,
                    max_retries=max_retries,
                    delay=delay,
                    error=str(exc),
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30.0)

        return 0  # unreachable

    # ------------------------------------------------------------------
    # Reconnect logic
    # ------------------------------------------------------------------

    async def _reconnect(self) -> None:
        """
        Handle reconnection with exponential backoff.

        Tears down the existing consumer (if any), waits, and creates
        a new one.
        """
        logger.info(
            "consumer_reconnecting",
            delay_seconds=self._reconnect_delay,
        )

        # Tear down old consumer
        if self._consumer is not None:
            try:
                await self._consumer.stop()
            except Exception:
                pass  # Best-effort cleanup
            self._consumer = None

        # Back off
        await asyncio.sleep(self._reconnect_delay)
        self._reconnect_delay = min(
            self._reconnect_delay * 2,
            _MAX_RECONNECT_DELAY,
        )

        # Recreate
        try:
            await self._create_consumer()
        except Exception:
            logger.exception("consumer_reconnect_failed")
