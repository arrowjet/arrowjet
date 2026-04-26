"""
Arrowjet Engine  - unified bulk data movement, bring your own connection.

Supports multiple databases through a single interface. The `provider`
parameter selects the execution strategy:

  - "redshift": COPY/UNLOAD via S3 (requires staging config)
  - "postgresql": COPY FROM STDIN / COPY TO STDOUT (no staging needed)

Usage:
    import arrowjet

    # PostgreSQL  - no staging config needed
    engine = arrowjet.Engine(provider="postgresql")
    engine.write_dataframe(pg_conn, df, "my_table")
    result = engine.read_bulk(pg_conn, "SELECT * FROM my_table")

    # Redshift  - needs S3 staging
    engine = arrowjet.Engine(
        provider="redshift",
        staging_bucket="my-bucket",
        staging_iam_role="arn:aws:iam::123:role/RedshiftS3",
        staging_region="us-east-1",
    )
    engine.write_dataframe(rs_conn, df, "my_table")
    result = engine.read_bulk(rs_conn, "SELECT * FROM my_table")

    # Default (no provider) = redshift for backward compatibility
    engine = arrowjet.Engine(staging_bucket=..., staging_iam_role=..., staging_region=...)
"""

from __future__ import annotations

import logging
from typing import Optional

import pyarrow as pa

logger = logging.getLogger(__name__)


class Engine:
    """
    Unified bulk data movement engine  - bring your own connection.

    Args:
        provider: Database provider ("postgresql" or "redshift"). Default: "redshift".
        staging_bucket: S3 bucket for staging (Redshift only).
        staging_iam_role: IAM role ARN for S3 access (Redshift only).
        staging_region: AWS region (Redshift only).
        ... (other staging options for Redshift)

    PostgreSQL requires no staging config  - just pass provider="postgresql".
    """

    def __init__(
        self,
        provider: Optional[str] = None,
        # Redshift staging config (ignored for postgresql)
        staging_bucket: Optional[str] = None,
        staging_iam_role: Optional[str] = None,
        staging_region: Optional[str] = None,
        staging_prefix: str = "arrowjet-staging",
        staging_cleanup: str = "on_success",
        staging_encryption: str = "none",
        staging_kms_key_id: Optional[str] = None,
        max_concurrent_bulk_ops: int = 4,
        max_staging_bytes: int = 10 * 1024 * 1024 * 1024,
        disallow_cross_region: bool = True,
        s3_endpoint_url: Optional[str] = None,
        cluster_id: str = "default",
        database: str = "default",
    ):
        # Infer provider from arguments if not specified
        if provider is None:
            if staging_bucket:
                provider = "redshift"
            else:
                raise ValueError(
                    "provider is required. Use provider='postgresql' (no staging needed) "
                    "or provider='redshift' with staging_bucket, staging_iam_role, staging_region."
                )

        self._provider_name = provider

        if provider == "postgresql":
            self._init_postgresql()
        elif provider == "mysql":
            self._init_mysql()
        elif provider == "redshift":
            self._init_redshift(
                staging_bucket=staging_bucket,
                staging_iam_role=staging_iam_role,
                staging_region=staging_region,
                staging_prefix=staging_prefix,
                staging_cleanup=staging_cleanup,
                staging_encryption=staging_encryption,
                staging_kms_key_id=staging_kms_key_id,
                max_concurrent_bulk_ops=max_concurrent_bulk_ops,
                max_staging_bytes=max_staging_bytes,
                disallow_cross_region=disallow_cross_region,
                s3_endpoint_url=s3_endpoint_url,
                cluster_id=cluster_id,
                database=database,
            )
        else:
            raise ValueError(
                f"Unknown provider: '{provider}'. "
                f"Supported: 'postgresql', 'mysql', 'redshift'."
            )

    def _init_postgresql(self):
        """Initialize PostgreSQL provider (COPY protocol, no staging)."""
        from .providers.postgresql import PostgreSQLProvider
        self._pg_provider = PostgreSQLProvider()
        self._is_pg = True
        self._is_mysql = False
        logger.info("Arrowjet Engine ready (provider=postgresql, COPY protocol)")

    def _init_mysql(self):
        """Initialize MySQL provider (LOAD DATA LOCAL INFILE, no staging)."""
        from .providers.mysql import MySQLProvider
        self._mysql_provider = MySQLProvider()
        self._is_pg = False
        self._is_mysql = True
        logger.info("Arrowjet Engine ready (provider=mysql, LOAD DATA LOCAL INFILE)")

    def _init_redshift(self, **kwargs):
        """Initialize Redshift provider (COPY/UNLOAD via S3)."""
        from .staging.config import StagingConfig, CleanupPolicy, EncryptionMode
        from .staging.manager import StagingManager
        from .bulk.writer import BulkWriter
        from .bulk.reader import BulkReader

        cleanup_map = {
            "always": CleanupPolicy.ALWAYS,
            "on_success": CleanupPolicy.ON_SUCCESS,
            "never": CleanupPolicy.NEVER,
            "ttl_managed": CleanupPolicy.TTL_MANAGED,
        }
        enc_map = {
            "none": EncryptionMode.NONE,
            "sse_s3": EncryptionMode.SSE_S3,
            "sse_kms": EncryptionMode.SSE_KMS,
        }

        if not kwargs.get("staging_bucket") or not kwargs.get("staging_iam_role"):
            raise ValueError(
                "staging_bucket and staging_iam_role are required for provider='redshift'."
            )

        config = StagingConfig(
            bucket=kwargs["staging_bucket"],
            iam_role=kwargs["staging_iam_role"],
            region=kwargs.get("staging_region") or "us-east-1",
            prefix=kwargs.get("staging_prefix", "arrowjet-staging"),
            cleanup_policy=cleanup_map.get(kwargs.get("staging_cleanup", "on_success"), CleanupPolicy.ON_SUCCESS),
            encryption=enc_map.get(kwargs.get("staging_encryption", "none"), EncryptionMode.NONE),
            kms_key_id=kwargs.get("staging_kms_key_id"),
            max_concurrent_ops=kwargs.get("max_concurrent_bulk_ops", 4),
            max_staging_bytes=kwargs.get("max_staging_bytes", 10 * 1024 * 1024 * 1024),
            disallow_cross_region=kwargs.get("disallow_cross_region", True),
            s3_endpoint_url=kwargs.get("s3_endpoint_url"),
        )

        self._staging_manager = StagingManager(
            config=config,
            cluster_id=kwargs.get("cluster_id", "default"),
            database=kwargs.get("database", "default"),
        )
        self._bulk_reader = BulkReader(self._staging_manager)
        self._bulk_writer = BulkWriter(self._staging_manager)
        self._is_pg = False
        self._is_mysql = False

        logger.info(
            "Arrowjet Engine ready (provider=redshift, bucket=%s)",
            kwargs["staging_bucket"],
        )

    def read_bulk(self, conn, query: str, **kwargs):
        """
        Bulk read from the database.

        PostgreSQL: COPY (query) TO STDOUT -> Arrow
        Redshift: UNLOAD -> S3 -> Parquet -> Arrow

        Args:
            conn: DBAPI-compatible connection
            query: SELECT query to execute

        Returns:
            ReadResult (Redshift) or PgReadResult (PostgreSQL)
        """
        if self._is_pg:
            return self._pg_provider.read_bulk(conn, query)
        if self._is_mysql:
            return self._mysql_provider.read_bulk(conn, query)
        return self._bulk_reader.read(
            conn, query, autocommit=True, explicit_mode=True, **kwargs
        )

    def write_bulk(self, conn, table: pa.Table, target_table: str, **kwargs):
        """
        Bulk write an Arrow table to the database.

        PostgreSQL: Arrow -> COPY FROM STDIN
        Redshift: Arrow -> Parquet -> S3 -> COPY

        Args:
            conn: DBAPI-compatible connection
            table: PyArrow Table to write
            target_table: Target table name (must exist)

        Returns:
            WriteResult (Redshift) or PgWriteResult (PostgreSQL)
        """
        if self._is_pg:
            return self._pg_provider.write_bulk(conn, table, target_table)
        if self._is_mysql:
            return self._mysql_provider.write_bulk(conn, table, target_table)
        return self._bulk_writer.write(conn, table, target_table, **kwargs)

    def write_dataframe(self, conn, df, target_table: str, **kwargs):
        """
        Bulk write a pandas DataFrame to the database.

        Args:
            conn: DBAPI-compatible connection
            df: pandas DataFrame to write
            target_table: Target table name (must exist)

        Returns:
            WriteResult (Redshift) or PgWriteResult (PostgreSQL)
        """
        if self._is_pg:
            return self._pg_provider.write_dataframe(conn, df, target_table)
        if self._is_mysql:
            return self._mysql_provider.write_dataframe(conn, df, target_table)
        table = pa.Table.from_pandas(df, preserve_index=False)
        return self.write_bulk(conn, table, target_table, **kwargs)

    @property
    def provider(self) -> str:
        """The active database provider name."""
        return self._provider_name

    def __repr__(self):
        if self._is_pg:
            return "Engine(provider=postgresql)"
        if self._is_mysql:
            return "Engine(provider=mysql)"
        cfg = self._staging_manager.config
        return f"Engine(provider=redshift, bucket={cfg.bucket})"


# Backward-compatible alias
PostgreSQLEngine = lambda: Engine(provider="postgresql")
PostgreSQLEngine.__doc__ = "Alias for Engine(provider='postgresql'). Deprecated  - use Engine(provider='postgresql') instead."
