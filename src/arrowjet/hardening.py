"""
Hardening utilities  - retry logic, connection health checks, and error classification.

Provides:
  - retry_on_transient: decorator for retrying transient failures
  - classify_error: categorize errors as transient vs permanent
  - check_connection: verify a connection is still alive
"""

from __future__ import annotations

import logging
import time
import functools
from typing import Callable, Optional, Type

logger = logging.getLogger(__name__)


# ── Error Classification ──────────────────────────────────

class ErrorCategory:
    TRANSIENT = "transient"       # Retry may help (network, throttling)
    PERMANENT = "permanent"       # Retry won't help (bad SQL, missing table)
    CONNECTION = "connection"     # Connection lost, need reconnect
    UNKNOWN = "unknown"


# Patterns that indicate transient errors
_TRANSIENT_PATTERNS = [
    "throttl",           # S3 throttling
    "503",               # S3 SlowDown
    "timeout",           # Network timeout
    "timed out",
    "connection reset",
    "broken pipe",
    "temporarily unavailable",
    "too many connections",
    "serializable isolation violation",
]

# Patterns that indicate connection loss
_CONNECTION_PATTERNS = [
    "connection refused",
    "connection closed",
    "connection lost",
    "ssl connection has been closed",
    "server closed the connection",
    "could not connect",
    "network is unreachable",
]


def classify_error(error: Exception) -> str:
    """Classify an error as transient, permanent, or connection-related."""
    msg = str(error).lower()

    for pattern in _CONNECTION_PATTERNS:
        if pattern in msg:
            return ErrorCategory.CONNECTION

    for pattern in _TRANSIENT_PATTERNS:
        if pattern in msg:
            return ErrorCategory.TRANSIENT

    # S3 ClientError with specific codes
    if hasattr(error, "response"):
        code = getattr(error, "response", {}).get("Error", {}).get("Code", "")
        if code in ("SlowDown", "ServiceUnavailable", "InternalError"):
            return ErrorCategory.TRANSIENT
        if code in ("RequestTimeout", "RequestTimeTooSkewed"):
            return ErrorCategory.TRANSIENT

    return ErrorCategory.PERMANENT


# ── Retry Logic ───────────────────────────────────────────

def retry_on_transient(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    backoff_factor: float = 2.0,
    retryable_categories: tuple = (ErrorCategory.TRANSIENT,),
):
    """
    Decorator that retries a function on transient errors with exponential backoff.

    Only retries errors classified as transient. Permanent errors raise immediately.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    category = classify_error(e)

                    if category not in retryable_categories:
                        logger.debug(
                            "Non-retryable error (category=%s): %s", category, e
                        )
                        raise

                    if attempt == max_retries:
                        logger.warning(
                            "Max retries (%d) exceeded for %s: %s",
                            max_retries, func.__name__, e,
                        )
                        raise

                    delay = min(base_delay * (backoff_factor ** attempt), max_delay)
                    logger.info(
                        "Retrying %s (attempt %d/%d, delay=%.1fs): %s",
                        func.__name__, attempt + 1, max_retries, delay, e,
                    )
                    time.sleep(delay)

            raise last_error  # Should not reach here
        return wrapper
    return decorator


# ── Connection Health Check ───────────────────────────────

def check_connection(conn) -> bool:
    """
    Check if a database connection is still alive.
    Returns True if healthy, False if dead.
    """
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        return True
    except Exception:
        return False


def check_connection_or_raise(conn, label: str = "connection"):
    """Check connection health, raise ConnectionLostError if dead."""
    if not check_connection(conn):
        raise ConnectionLostError(
            f"{label} is no longer responsive. "
            "The connection may have been dropped by the server."
        )


class ConnectionLostError(Exception):
    """Connection to the database was lost."""
    pass
