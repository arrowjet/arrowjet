"""Staging configuration model."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class CleanupPolicy(Enum):
    """When to clean up staged S3 files."""
    ALWAYS = "always"           # Delete after every operation
    ON_SUCCESS = "on_success"   # Delete on success, preserve on failure
    NEVER = "never"             # User manages lifecycle
    TTL_MANAGED = "ttl_managed" # Rely on S3 lifecycle rules


class EncryptionMode(Enum):
    """S3 server-side encryption mode."""
    NONE = "none"
    SSE_S3 = "SSE_S3"
    SSE_KMS = "SSE_KMS"


class QueueBehavior(Enum):
    """What to do when max concurrent bulk ops is reached."""
    WAIT = "wait"     # Queue until a slot opens
    REJECT = "reject" # Raise immediately


@dataclass(frozen=True)
class StagingConfig:
    """
    Configuration for S3 staging operations.

    All bulk read/write operations stage data through S3.
    This config defines where and how that staging happens.
    """
    # Required
    bucket: str
    iam_role: str
    region: str

    # Optional with defaults
    prefix: str = "arrowjet-staging"
    cleanup_policy: CleanupPolicy = CleanupPolicy.ON_SUCCESS
    encryption: EncryptionMode = EncryptionMode.NONE
    kms_key_id: Optional[str] = None

    # Concurrency
    max_concurrent_ops: int = 4
    queue_behavior: QueueBehavior = QueueBehavior.WAIT

    # Safety
    max_staging_bytes: int = 10 * 1024 * 1024 * 1024  # 10 GB
    disallow_cross_region: bool = True

    # S3 endpoint override (for VPC endpoints)
    s3_endpoint_url: Optional[str] = None

    def __post_init__(self):
        if not self.bucket:
            raise ValueError("staging bucket is required")
        if not self.iam_role:
            raise ValueError("IAM role ARN is required")
        if not self.region:
            raise ValueError("region is required")
        if self.encryption == EncryptionMode.SSE_KMS and not self.kms_key_id:
            raise ValueError("kms_key_id is required when encryption is SSE_KMS")
        if self.max_concurrent_ops < 1:
            raise ValueError("max_concurrent_ops must be >= 1")

    def s3_client_kwargs(self) -> dict:
        """Kwargs for boto3.client('s3', ...)."""
        kwargs = {"region_name": self.region}
        if self.s3_endpoint_url:
            kwargs["endpoint_url"] = self.s3_endpoint_url
        return kwargs

    def encryption_headers(self) -> dict:
        """Extra headers for S3 PutObject encryption."""
        if self.encryption == EncryptionMode.SSE_S3:
            return {"ServerSideEncryption": "AES256"}
        elif self.encryption == EncryptionMode.SSE_KMS:
            return {
                "ServerSideEncryption": "aws:kms",
                "SSEKMSKeyId": self.kms_key_id,
            }
        return {}
