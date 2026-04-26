"""
COPY command builder  - generates Redshift COPY SQL with proper options.

Handles IAM role, format, encryption, compression, and error handling options.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from ..staging.config import StagingConfig, EncryptionMode


@dataclass(frozen=True)
class CopyCommandBuilder:
    """Builds Redshift COPY commands for loading staged Parquet files."""

    staging_config: StagingConfig

    def build(
        self,
        target_table: str,
        s3_uri: str,
        manifest: bool = False,
    ) -> str:
        """
        Build a COPY command.

        Args:
            target_table: Fully qualified table name
            s3_uri: S3 URI pointing to staged data (prefix or manifest)
            manifest: If True, s3_uri points to a manifest file

        Returns:
            Complete COPY SQL string
        """
        parts = [
            f"COPY {target_table}",
            f"FROM '{s3_uri}'",
            f"IAM_ROLE '{self.staging_config.iam_role}'",
            "FORMAT PARQUET",
        ]

        if manifest:
            parts.append("MANIFEST")

        # Encryption
        if self.staging_config.encryption == EncryptionMode.SSE_KMS:
            parts.append(f"ENCRYPTED")
            parts.append(f"MASTER_SYMMETRIC_KEY '{self.staging_config.kms_key_id}'")

        return "\n".join(parts)

    def build_from_path(
        self,
        target_table: str,
        bucket: str,
        key_prefix: str,
    ) -> str:
        """Build COPY from a staging path prefix."""
        s3_uri = f"s3://{bucket}/{key_prefix}"
        return self.build(target_table=target_table, s3_uri=s3_uri)
