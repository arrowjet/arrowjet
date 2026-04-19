"""
Cleanup manager — handles S3 staged file cleanup based on policy.

Policies:
  ALWAYS      — delete after every operation
  ON_SUCCESS  — delete on success, preserve on failure for debugging
  NEVER       — user manages lifecycle
  TTL_MANAGED — rely on S3 lifecycle rules
"""

from __future__ import annotations

import logging

import boto3
from botocore.config import Config as BotoConfig

from .config import StagingConfig, CleanupPolicy
from .namespace import OperationPath
from .lifecycle import StagingOperation, OperationState

logger = logging.getLogger(__name__)

_S3_RETRY_CONFIG = BotoConfig(
    retries={"max_attempts": 3, "mode": "adaptive"}
)


class CleanupManager:
    """Manages cleanup of staged S3 files based on configured policy."""

    def __init__(self, config: StagingConfig):
        self._config = config
        self._client = boto3.client(
            "s3",
            config=_S3_RETRY_CONFIG,
            **config.s3_client_kwargs(),
        )

    def cleanup(self, operation: StagingOperation) -> bool:
        """
        Clean up staged files for an operation based on policy.

        Returns True if cleanup was performed, False if skipped.
        Logs warnings on failure but does not raise — the operation
        result is already returned to the user.
        """
        policy = self._config.cleanup_policy

        if policy == CleanupPolicy.NEVER:
            logger.debug("Cleanup skipped (policy=never): %s", operation.path.key_prefix)
            return False

        if policy == CleanupPolicy.TTL_MANAGED:
            logger.debug("Cleanup skipped (policy=ttl_managed): %s", operation.path.key_prefix)
            return False

        if policy == CleanupPolicy.ON_SUCCESS and operation.state == OperationState.FAILED:
            logger.info(
                "Cleanup skipped (policy=on_success, state=failed): %s — "
                "staged files preserved for debugging",
                operation.path.key_prefix,
            )
            return False

        # ALWAYS or ON_SUCCESS with successful operation
        return self._delete_prefix(operation)

    def force_cleanup(self, path: OperationPath) -> bool:
        """Force cleanup regardless of policy. Used for orphan recovery."""
        return self._delete_prefix_by_path(path)

    def list_orphans(self, prefix: str = "") -> list[str]:
        """
        List S3 prefixes that may be orphaned (no active operation).
        Useful for diagnostics and manual cleanup.
        """
        search_prefix = f"{self._config.prefix}/{prefix}" if prefix else self._config.prefix
        prefixes = set()
        paginator = self._client.get_paginator("list_objects_v2")

        for page in paginator.paginate(
            Bucket=self._config.bucket,
            Prefix=search_prefix,
            Delimiter="/",
        ):
            for cp in page.get("CommonPrefixes", []):
                prefixes.add(cp["Prefix"])

        return sorted(prefixes)

    def _delete_prefix(self, operation: StagingOperation) -> bool:
        """Delete all objects under an operation's S3 prefix."""
        try:
            deleted = self._do_delete(operation.path.key_prefix)
            if deleted > 0:
                logger.info(
                    "Cleaned up %d objects from %s",
                    deleted, operation.path.key_prefix,
                )
                operation.transition(OperationState.CLEANUP_DONE)
            else:
                operation.transition(OperationState.CLEANUP_DONE)
            return True
        except Exception as e:
            logger.warning(
                "Cleanup failed for %s: %s — staged files may remain",
                operation.path.key_prefix, e,
            )
            try:
                operation.transition(OperationState.CLEANUP_FAILED)
            except Exception:
                pass
            return False

    def _delete_prefix_by_path(self, path: OperationPath) -> bool:
        """Delete all objects under a path (no operation tracking)."""
        try:
            deleted = self._do_delete(path.key_prefix)
            logger.info("Force cleaned %d objects from %s", deleted, path.key_prefix)
            return True
        except Exception as e:
            logger.warning("Force cleanup failed for %s: %s", path.key_prefix, e)
            return False

    def _do_delete(self, prefix: str) -> int:
        """Delete all objects under a prefix. Returns count deleted."""
        deleted = 0
        paginator = self._client.get_paginator("list_objects_v2")

        for page in paginator.paginate(
            Bucket=self._config.bucket, Prefix=prefix
        ):
            objects = page.get("Contents", [])
            if not objects:
                continue

            self._client.delete_objects(
                Bucket=self._config.bucket,
                Delete={"Objects": [{"Key": obj["Key"]} for obj in objects]},
            )
            deleted += len(objects)

        return deleted
