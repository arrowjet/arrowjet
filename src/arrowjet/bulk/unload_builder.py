"""
UNLOAD command builder — generates Redshift UNLOAD SQL with proper options.

Handles IAM role, format, compression, parallelism, encryption,
and Redshift UNLOAD quirks (e.g., LIMIT not supported directly).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from ..staging.config import StagingConfig, EncryptionMode

# Detect LIMIT or OFFSET clause at the end of a query
_NEEDS_WRAP_PATTERN = re.compile(
    r'\b(LIMIT\s+\d+|OFFSET\s+\d+)\s*;?\s*$', re.IGNORECASE
)


@dataclass(frozen=True)
class UnloadCommandBuilder:
    """Builds Redshift UNLOAD commands for exporting query results to S3."""

    staging_config: StagingConfig

    def build(
        self,
        query: str,
        s3_uri: str,
        parallel: bool = True,
        compression: Optional[str] = None,
    ) -> str:
        """
        Build an UNLOAD command.

        Args:
            query: SELECT query to unload
            s3_uri: S3 URI prefix for output files
            parallel: Enable parallel UNLOAD across slices
            compression: Optional compression (None = uncompressed Parquet)

        Returns:
            Complete UNLOAD SQL string
        """
        # Redshift UNLOAD doesn't support LIMIT directly — wrap in subquery
        query = _wrap_if_limit(query)

        parts = [
            f"UNLOAD ($${query}$$)",
            f"TO '{s3_uri}'",
            f"IAM_ROLE '{self.staging_config.iam_role}'",
            "FORMAT PARQUET",
            "ALLOWOVERWRITE",
        ]

        if parallel:
            parts.append("PARALLEL ON")
        else:
            parts.append("PARALLEL OFF")

        if compression:
            # Redshift supports ZSTD for Parquet UNLOAD
            parts.append(f"EXTENSION '{compression}'")

        # Encryption
        if self.staging_config.encryption == EncryptionMode.SSE_KMS:
            parts.append("ENCRYPTED")
            parts.append(f"KMS_KEY_ID '{self.staging_config.kms_key_id}'")

        return "\n".join(parts)


def _wrap_if_limit(query: str) -> str:
    """
    Wrap query in a subquery if it contains LIMIT or OFFSET.

    Redshift UNLOAD doesn't support LIMIT or OFFSET directly.
    This transparently wraps the query so users never have to adjust
    their SQL for arrowjet.
    """
    stripped = query.strip().rstrip(";")
    if _NEEDS_WRAP_PATTERN.search(stripped):
        return f"SELECT * FROM ({stripped})"
    return query
