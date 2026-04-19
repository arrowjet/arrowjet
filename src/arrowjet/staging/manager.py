"""
Staging Manager — the main entry point for all staging operations.

Coordinates namespace, upload, download, cleanup, and concurrency control.
Validates configuration at initialization (fail-fast on misconfiguration).
"""

from __future__ import annotations

import logging
import threading
from typing import Optional

import boto3

from .config import StagingConfig
from .namespace import StagingNamespace, OperationPath
from .lifecycle import StagingOperation, OperationState
from .upload import S3Uploader
from .download import S3Downloader
from .cleanup import CleanupManager

logger = logging.getLogger(__name__)


class StagingManager:
    """
    Central coordinator for S3 staging operations.

    Provides:
      - Connection-time validation (bucket reachable, region match)
      - Namespace generation for isolated operations
      - Upload/download engines
      - Cleanup management
      - Concurrency control (semaphore-based)
    """

    def __init__(
        self,
        config: StagingConfig,
        cluster_id: str = "unknown",
        database: str = "unknown",
    ):
        self._config = config
        self._namespace = StagingNamespace(
            config=config,
            cluster_id=cluster_id,
            database=database,
        )
        self._uploader = S3Uploader(config)
        self._downloader = S3Downloader(config)
        self._cleanup = CleanupManager(config)

        # Concurrency control
        self._semaphore = threading.Semaphore(config.max_concurrent_ops)
        self._active_ops: dict[str, StagingOperation] = {}
        self._lock = threading.Lock()

        # Validate on init
        self._validate()

    def _validate(self) -> None:
        """Fail-fast validation at connection time."""
        client = boto3.client("s3", **self._config.s3_client_kwargs())

        # Check bucket exists and is accessible
        try:
            client.head_bucket(Bucket=self._config.bucket)
        except Exception as e:
            raise StagingValidationError(
                f"Cannot access staging bucket '{self._config.bucket}': {e}\n"
                f"Check: bucket exists, IAM permissions, VPC endpoint if private subnet"
            ) from e

        # Check region match
        if self._config.disallow_cross_region:
            try:
                resp = client.get_bucket_location(Bucket=self._config.bucket)
                bucket_region = resp.get("LocationConstraint") or "us-east-1"
                if bucket_region != self._config.region:
                    raise StagingValidationError(
                        f"Staging bucket is in {bucket_region} but config specifies "
                        f"{self._config.region}. Cross-region staging is disabled. "
                        f"Set disallow_cross_region=False to override."
                    )
            except StagingValidationError:
                raise
            except Exception as e:
                logger.warning("Could not verify bucket region: %s", e)

        logger.info(
            "Staging validated: bucket=%s, region=%s, prefix=%s",
            self._config.bucket, self._config.region, self._config.prefix,
        )

    def begin_operation(self, operation_type: str) -> StagingOperation:
        """
        Begin a new staging operation. Acquires a concurrency slot.

        Args:
            operation_type: "read" or "write"

        Returns:
            A StagingOperation with a unique path, in PLANNED state.

        Raises:
            StagingConcurrencyError: if queue_behavior is REJECT and no slot available.
        """
        if self._config.queue_behavior.value == "reject":
            acquired = self._semaphore.acquire(blocking=False)
            if not acquired:
                raise StagingConcurrencyError(
                    f"Max concurrent bulk operations ({self._config.max_concurrent_ops}) "
                    f"reached. Set queue_behavior='wait' to queue instead of rejecting."
                )
        else:
            self._semaphore.acquire()

        path = self._namespace.new_operation()
        op = StagingOperation(path=path, operation_type=operation_type)

        with self._lock:
            self._active_ops[path.stmt_id] = op

        logger.debug(
            "Began staging op %s (type=%s, path=%s)",
            path.stmt_id, operation_type, path.key_prefix,
        )
        return op

    def complete_operation(self, operation: StagingOperation) -> None:
        """
        Complete a staging operation. Runs cleanup and releases concurrency slot.

        Call this in a finally block to ensure cleanup always runs.
        """
        try:
            if operation.needs_cleanup:
                self._cleanup.cleanup(operation)
            elif not operation.is_terminal:
                # Operation was never completed or failed — mark as failed
                operation.fail("Operation abandoned without completion")
                self._cleanup.cleanup(operation)
        finally:
            with self._lock:
                self._active_ops.pop(operation.path.stmt_id, None)
            self._semaphore.release()

            logger.info(
                "Staging op complete: %s",
                operation.summary(),
            )

    @property
    def uploader(self) -> S3Uploader:
        return self._uploader

    @property
    def downloader(self) -> S3Downloader:
        return self._downloader

    @property
    def cleanup_manager(self) -> CleanupManager:
        return self._cleanup

    @property
    def config(self) -> StagingConfig:
        return self._config

    def active_operations(self) -> list[dict]:
        """Return summaries of all active operations."""
        with self._lock:
            return [op.summary() for op in self._active_ops.values()]


class StagingValidationError(Exception):
    pass


class StagingConcurrencyError(Exception):
    pass
