"""
Application entry-point for the raw-writer service.

Creates the async event loop, initialises the database and Kafka consumer,
and runs the consume-process-commit loop until interrupted.

Run locally with::

    python -m app.main
"""

from __future__ import annotations

import asyncio
import signal
import sys

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[3]))

from shared.utils.logger import configure_logging, get_logger

from app.config import get_settings
from app.consumer import RawWriterConsumer
from app.db import DatabaseManager


async def main() -> None:
    """
    Service entry-point.

    1. Load configuration
    2. Initialise structured logging
    3. Connect to TimescaleDB (with connection pool)
    4. Start Kafka consumer
    5. Run the consume loop until SIGINT / SIGTERM
    6. Graceful shutdown
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
    )

    # --- Database ---
    db = DatabaseManager(settings)
    try:
        await db.connect()
    except Exception:
        logger.exception("db_connection_failed")
        sys.exit(1)

    # --- Consumer ---
    consumer = RawWriterConsumer(settings, db)
    try:
        await consumer.start()
    except Exception:
        logger.exception("consumer_start_failed")
        await db.close()
        sys.exit(1)

    # --- Graceful shutdown handler ---
    shutdown_event = asyncio.Event()

    def _signal_handler() -> None:
        logger.info("shutdown_signal_received")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler
            pass

    logger.info("service_started", service=settings.service_name)

    # --- Run consumer loop ---
    consumer_task = asyncio.create_task(consumer.run())

    # Wait for shutdown signal or consumer task completion
    done, pending = await asyncio.wait(
        [
            asyncio.create_task(shutdown_event.wait()),
            consumer_task,
        ],
        return_when=asyncio.FIRST_COMPLETED,
    )

    # --- Shutdown ---
    logger.info("service_stopping", service=settings.service_name)

    await consumer.stop()

    if not consumer_task.done():
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass

    await db.close()

    logger.info("service_stopped", service=settings.service_name)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
