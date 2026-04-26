"""
Staging operation lifecycle  - tracks state transitions.

States:
  planned -> files_staging -> manifest_written -> command_submitted -> completed -> cleanup_done
                                                                      ↓
                                                                cleanup_failed
"""

from __future__ import annotations

import time
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from .namespace import OperationPath

logger = logging.getLogger(__name__)


class OperationState(Enum):
    PLANNED = "planned"
    FILES_STAGING = "files_staging"
    COMMAND_SUBMITTED = "command_submitted"
    COMPLETED = "completed"
    FAILED = "failed"
    CLEANUP_DONE = "cleanup_done"
    CLEANUP_FAILED = "cleanup_failed"


# Valid state transitions
_TRANSITIONS = {
    OperationState.PLANNED: {OperationState.FILES_STAGING, OperationState.FAILED},
    OperationState.FILES_STAGING: {OperationState.COMMAND_SUBMITTED, OperationState.FAILED},
    OperationState.COMMAND_SUBMITTED: {OperationState.COMPLETED, OperationState.FAILED},
    OperationState.COMPLETED: {OperationState.CLEANUP_DONE, OperationState.CLEANUP_FAILED},
    OperationState.FAILED: {OperationState.CLEANUP_DONE, OperationState.CLEANUP_FAILED},
    OperationState.CLEANUP_DONE: set(),
    OperationState.CLEANUP_FAILED: set(),
}


@dataclass
class StagingOperation:
    """
    Tracks the lifecycle of a single staging operation.

    Every bulk read or write creates one of these. It records
    state transitions, timing, and metadata for observability.
    """

    path: OperationPath
    operation_type: str  # "read" or "write"
    state: OperationState = OperationState.PLANNED

    # Timing
    started_at: float = field(default_factory=time.monotonic)
    completed_at: Optional[float] = None

    # Metadata (populated during execution)
    files_count: int = 0
    bytes_staged: int = 0
    rows_affected: int = 0
    error: Optional[str] = None

    def transition(self, new_state: OperationState) -> None:
        """Transition to a new state. Raises if transition is invalid."""
        valid = _TRANSITIONS.get(self.state, set())
        if new_state not in valid:
            raise InvalidTransitionError(
                f"Cannot transition from {self.state.value} to {new_state.value}. "
                f"Valid transitions: {[s.value for s in valid]}"
            )
        old = self.state
        self.state = new_state

        if new_state in (
            OperationState.COMPLETED,
            OperationState.FAILED,
            OperationState.CLEANUP_DONE,
            OperationState.CLEANUP_FAILED,
        ):
            self.completed_at = time.monotonic()

        logger.debug(
            "staging op %s: %s -> %s (type=%s, path=%s)",
            self.path.stmt_id, old.value, new_state.value,
            self.operation_type, self.path.key_prefix,
        )

    def fail(self, error: str) -> None:
        """Mark operation as failed with an error message."""
        self.error = error
        self.transition(OperationState.FAILED)

    @property
    def duration_s(self) -> Optional[float]:
        if self.completed_at is None:
            return None
        return self.completed_at - self.started_at

    @property
    def is_terminal(self) -> bool:
        return self.state in (
            OperationState.CLEANUP_DONE,
            OperationState.CLEANUP_FAILED,
        )

    @property
    def needs_cleanup(self) -> bool:
        return self.state in (
            OperationState.COMPLETED,
            OperationState.FAILED,
        )

    def summary(self) -> dict:
        """Return a summary dict for logging/observability."""
        return {
            "stmt_id": self.path.stmt_id,
            "type": self.operation_type,
            "state": self.state.value,
            "files": self.files_count,
            "bytes_staged": self.bytes_staged,
            "rows": self.rows_affected,
            "duration_s": round(self.duration_s, 3) if self.duration_s else None,
            "error": self.error,
            "s3_prefix": self.path.key_prefix,
        }


class InvalidTransitionError(Exception):
    pass
