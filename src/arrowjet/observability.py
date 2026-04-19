"""
Observability — metrics, tracing hooks, and cost logging.

Provides:
  - ConnectionMetrics: cumulative counters per connection
  - TracingHook: callback for bulk operation events
  - CostLogger: per-operation cost visibility
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Optional

logger = logging.getLogger(__name__)


# ── Metrics ───────────────────────────────────────────────

@dataclass
class ConnectionMetrics:
    """Cumulative counters for a connection's lifetime."""

    ops_direct_read: int = 0
    ops_bulk_read: int = 0
    ops_direct_write: int = 0
    ops_bulk_write: int = 0
    bytes_staged_total: int = 0
    bytes_read_total: int = 0
    s3_requests_total: int = 0
    cleanup_failures: int = 0
    errors_total: int = 0

    def record_bulk_read(self, bytes_staged: int, s3_requests: int = 0):
        self.ops_bulk_read += 1
        self.bytes_staged_total += bytes_staged
        self.bytes_read_total += bytes_staged
        self.s3_requests_total += s3_requests

    def record_bulk_write(self, bytes_staged: int, s3_requests: int = 0):
        self.ops_bulk_write += 1
        self.bytes_staged_total += bytes_staged
        self.s3_requests_total += s3_requests

    def record_direct_read(self):
        self.ops_direct_read += 1

    def record_direct_write(self):
        self.ops_direct_write += 1

    def record_error(self):
        self.errors_total += 1

    def record_cleanup_failure(self):
        self.cleanup_failures += 1

    def to_dict(self) -> dict:
        return {
            "ops_direct_read": self.ops_direct_read,
            "ops_bulk_read": self.ops_bulk_read,
            "ops_direct_write": self.ops_direct_write,
            "ops_bulk_write": self.ops_bulk_write,
            "bytes_staged_total": self.bytes_staged_total,
            "bytes_read_total": self.bytes_read_total,
            "s3_requests_total": self.s3_requests_total,
            "cleanup_failures": self.cleanup_failures,
            "errors_total": self.errors_total,
        }

    def __repr__(self):
        total_ops = (self.ops_direct_read + self.ops_bulk_read +
                     self.ops_direct_write + self.ops_bulk_write)
        return (
            f"ConnectionMetrics(total_ops={total_ops}, "
            f"bulk_reads={self.ops_bulk_read}, bulk_writes={self.ops_bulk_write}, "
            f"staged={self.bytes_staged_total:,} bytes)"
        )


# ── Tracing Hook ──────────────────────────────────────────

@dataclass
class BulkOperationEvent:
    """Structured event fired after every bulk operation."""

    operation_type: str  # "read" or "write"
    success: bool
    rows: int
    bytes_staged: int
    files_count: int
    s3_requests: int
    duration_s: float
    path_chosen: str  # "unload" or "copy"
    s3_path: str
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "operation_type": self.operation_type,
            "success": self.success,
            "rows": self.rows,
            "bytes_staged": self.bytes_staged,
            "files_count": self.files_count,
            "s3_requests": self.s3_requests,
            "duration_s": self.duration_s,
            "path_chosen": self.path_chosen,
            "s3_path": self.s3_path,
            "error": self.error,
        }


# Type alias for the tracing callback
TracingHook = Callable[[BulkOperationEvent], None]


def _noop_hook(event: BulkOperationEvent) -> None:
    """Default no-op tracing hook."""
    pass


# ── Cost Logging ──────────────────────────────────────────

class CostLogLevel(Enum):
    OFF = "off"
    SUMMARY = "summary"
    VERBOSE = "verbose"


class CostLogger:
    """Logs per-operation cost information at configurable verbosity."""

    def __init__(self, level: CostLogLevel = CostLogLevel.OFF):
        self._level = level

    @property
    def level(self) -> CostLogLevel:
        return self._level

    @level.setter
    def level(self, value: CostLogLevel):
        self._level = value

    def log_operation(
        self,
        operation_type: str,
        rows: int,
        bytes_staged: int,
        files_count: int,
        duration_s: float,
        s3_path: str,
        cleanup_outcome: str = "completed",
    ):
        if self._level == CostLogLevel.OFF:
            return

        if self._level == CostLogLevel.SUMMARY:
            logger.info(
                "[Arrowjet] %s: %d rows, %d bytes staged, %.2fs",
                operation_type, rows, bytes_staged, duration_s,
            )

        elif self._level == CostLogLevel.VERBOSE:
            # Estimate S3 request costs (rough)
            est_put = files_count  # one PUT per file
            est_get = files_count if operation_type == "read" else 0
            est_list = 1  # one LIST per operation
            est_delete = files_count if cleanup_outcome == "completed" else 0
            total_requests = est_put + est_get + est_list + est_delete

            logger.info(
                "[Arrowjet] %s completed:\n"
                "  rows: %d\n"
                "  staged_bytes: %d\n"
                "  s3_files: %d\n"
                "  s3_requests: ~%d (PUT=%d, GET=%d, LIST=%d, DELETE=%d)\n"
                "  duration: %.2fs\n"
                "  cleanup: %s\n"
                "  path: %s",
                operation_type, rows, bytes_staged, files_count,
                total_requests, est_put, est_get, est_list, est_delete,
                duration_s, cleanup_outcome, s3_path,
            )
