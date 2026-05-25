"""
Timestamp utilities for the telemetry system.

Provides ISO 8601 parsing, UTC normalisation, epoch conversion,
and monotonicity checks for ordered telemetry frame sequences.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Sequence


def utc_now() -> datetime:
    """Return the current UTC datetime (timezone-aware)."""
    return datetime.now(tz=timezone.utc)


def utc_now_iso() -> str:
    """Return the current UTC time as an ISO 8601 string."""
    return utc_now().isoformat()


def parse_iso_timestamp(raw: str) -> datetime:
    """
    Parse an ISO 8601 timestamp string into a timezone-aware ``datetime``.

    If the input has no timezone information it is assumed to be UTC.

    Raises
    ------
    ValueError
        If *raw* cannot be parsed as ISO 8601.
    """
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def to_epoch_ms(dt: datetime) -> int:
    """Convert a datetime to milliseconds since the Unix epoch."""
    return int(dt.timestamp() * 1000)


def iso_to_epoch_ms(raw: str) -> int:
    """Shortcut: parse an ISO string and return epoch-milliseconds."""
    return to_epoch_ms(parse_iso_timestamp(raw))


def ensure_utc(dt: datetime) -> datetime:
    """
    Normalise *dt* to UTC.

    * Naive datetimes are assumed UTC.
    * Aware datetimes are converted to UTC.
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def is_monotonically_increasing(timestamps: Sequence[str]) -> bool:
    """
    Return ``True`` when a sequence of ISO 8601 timestamp strings is
    strictly monotonically increasing.

    An empty or single-element sequence is considered valid.
    """
    if len(timestamps) <= 1:
        return True
    parsed = [parse_iso_timestamp(t) for t in timestamps]
    return all(a < b for a, b in zip(parsed, parsed[1:]))
