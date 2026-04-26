"""
Staging namespace  - generates unique, collision-free S3 paths.

Every staged operation gets a path like:
  s3://<bucket>/<prefix>/<cluster-id>/<database>/<conn-uuid>/<stmt-uuid>/<attempt>/

This isolates:
  - concurrent statements (different stmt-uuid)
  - retries (different attempt)
  - multiple connections (different conn-uuid)
  - multiple users sharing a bucket (different conn-uuid + cluster-id)
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field

from .config import StagingConfig


@dataclass
class StagingNamespace:
    """Generates isolated S3 paths for staging operations."""

    config: StagingConfig
    cluster_id: str = "unknown"
    database: str = "unknown"
    connection_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])

    def new_operation(self, attempt: int = 1) -> OperationPath:
        """Create a new unique path for a staging operation."""
        stmt_id = uuid.uuid4().hex[:12]
        key_prefix = (
            f"{self.config.prefix}/"
            f"{self.cluster_id}/"
            f"{self.database}/"
            f"{self.connection_id}/"
            f"{stmt_id}/"
            f"attempt_{attempt}/"
        )
        return OperationPath(
            bucket=self.config.bucket,
            key_prefix=key_prefix,
            stmt_id=stmt_id,
            attempt=attempt,
        )


@dataclass(frozen=True)
class OperationPath:
    """A unique S3 path for a single staging operation."""

    bucket: str
    key_prefix: str
    stmt_id: str
    attempt: int

    @property
    def s3_uri(self) -> str:
        return f"s3://{self.bucket}/{self.key_prefix}"

    def file_key(self, filename: str) -> str:
        return f"{self.key_prefix}{filename}"

    def retry(self) -> OperationPath:
        """Create a new path for a retry of this operation."""
        # Replace attempt number in the prefix
        parts = self.key_prefix.rstrip("/").rsplit("/", 1)
        new_prefix = f"{parts[0]}/attempt_{self.attempt + 1}/"
        return OperationPath(
            bucket=self.bucket,
            key_prefix=new_prefix,
            stmt_id=self.stmt_id,
            attempt=self.attempt + 1,
        )
