"""
RedshiftProvider — bulk operations for Amazon Redshift.

Export: UNLOAD query TO S3 FORMAT PARQUET
Import: COPY table FROM S3 FORMAT PARQUET

Handles Redshift-specific quirks:
  - Dollar-quoting for queries with single quotes
  - LIMIT/OFFSET must be wrapped in a subquery for UNLOAD
  - IAM role authentication for S3 access
  - SSE-KMS encryption options
"""

from __future__ import annotations

import re
from typing import Optional

from .base import BulkProvider, ExportCommand, ImportCommand
from ..staging.config import StagingConfig, EncryptionMode

# Detect LIMIT or OFFSET at the end of a query (Redshift UNLOAD limitation)
_NEEDS_WRAP_PATTERN = re.compile(
    r'\b(LIMIT\s+\d+|OFFSET\s+\d+)\s*;?\s*$', re.IGNORECASE
)


class RedshiftProvider(BulkProvider):
    """
    BulkProvider implementation for Amazon Redshift.

    Uses UNLOAD for exports and COPY for imports, both via S3.
    Requires an IAM role that Redshift can assume for S3 access.
    """

    def __init__(self, config: StagingConfig):
        self._config = config

    def build_export_sql(
        self,
        query: str,
        staging_path: str,
        parallel: bool = True,
        compression: Optional[str] = None,
    ) -> ExportCommand:
        """
        Build a Redshift UNLOAD command.

        Automatically wraps queries with LIMIT/OFFSET in a subquery
        since Redshift UNLOAD doesn't support them directly.
        """
        query = _wrap_if_limit(query)

        parts = [
            f"UNLOAD ($${query}$$)",
            f"TO '{staging_path}'",
            f"IAM_ROLE '{self._config.iam_role}'",
            "FORMAT PARQUET",
            "ALLOWOVERWRITE",
            "PARALLEL ON" if parallel else "PARALLEL OFF",
        ]

        if compression:
            parts.append(f"EXTENSION '{compression}'")

        if self._config.encryption == EncryptionMode.SSE_KMS:
            parts.append("ENCRYPTED")
            parts.append(f"KMS_KEY_ID '{self._config.kms_key_id}'")

        return ExportCommand(
            sql="\n".join(parts),
            staging_path=staging_path,
            format="parquet",
        )

    def build_import_sql(
        self,
        target_table: str,
        staging_path: str,
        compression: Optional[str] = None,
    ) -> ImportCommand:
        """Build a Redshift COPY command."""
        parts = [
            f"COPY {target_table}",
            f"FROM '{staging_path}'",
            f"IAM_ROLE '{self._config.iam_role}'",
            "FORMAT PARQUET",
        ]

        if self._config.encryption == EncryptionMode.SSE_KMS:
            parts.append("ENCRYPTED")
            parts.append(f"MASTER_SYMMETRIC_KEY '{self._config.kms_key_id}'")

        return ImportCommand(
            sql="\n".join(parts),
            staging_path=staging_path,
            format="parquet",
        )

    @property
    def staging_format(self) -> str:
        return "parquet"

    @property
    def staging_backend(self) -> str:
        return "s3"

    @property
    def name(self) -> str:
        return "redshift"


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
