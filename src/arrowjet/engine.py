"""
Arrowjet Engine — bring your own connection (BYOC).

For users who already have connection management (Airflow, ETL scripts, dbt)
and just want to add bulk data movement without rewiring their setup.

Usage:
    import arrowjet
    import redshift_connector  # or psycopg2, or any DBAPI connection

    # Create the engine once (staging config only — no connection details)
    engine = arrowjet.Engine(
        staging_bucket="my-bucket",
        staging_iam_role="arn:aws:iam::123:role/RedshiftS3",
        staging_region="us-east-1",
    )

    # Use with any existing DBAPI connection
    conn = redshift_connector.connect(host=..., database=..., ...)
    result = engine.read_bulk(conn, "SELECT * FROM events")
    engine.write_bulk(conn, arrow_table, "target_table")
    engine.write_dataframe(conn, df, "target_table")

The Engine only needs conn.cursor() and cursor.execute(sql) — standard DBAPI.
Works with redshift_connector, psycopg2, ADBC dbapi, or any DBAPI-compatible
connection that can talk to Redshift.
"""

from __future__ import annotations

import logging
from typing import Optional

import pyarrow as pa

from .staging.config import StagingConfig, CleanupPolicy, EncryptionMode
from .staging.manager import StagingManager
from .bulk.writer import BulkWriter, WriteResult
from .bulk.reader import BulkReader, ReadResult

logger = logging.getLogger(__name__)

_CLEANUP_MAP = {
    "always": CleanupPolicy.ALWAYS,
    "on_success": CleanupPolicy.ON_SUCCESS,
    "never": CleanupPolicy.NEVER,
    "ttl_managed": CleanupPolicy.TTL_MANAGED,
}

_ENC_MAP = {
    "none": EncryptionMode.NONE,
    "sse_s3": EncryptionMode.SSE_S3,
    "sse_kms": EncryptionMode.SSE_KMS,
}


class Engine:
    """
    Bulk data movement engine — bring your own connection.

    Accepts any DBAPI-compatible connection for UNLOAD/COPY operations.
    No connection ownership — the caller manages connection lifecycle.
    """

    def __init__(
        self,
        staging_bucket: str,
        staging_iam_role: str,
        staging_region: str,
        staging_prefix: str = "arrowjet-staging",
        staging_cleanup: str = "on_success",
        staging_encryption: str = "none",
        staging_kms_key_id: Optional[str] = None,
        max_concurrent_bulk_ops: int = 4,
        max_staging_bytes: int = 10 * 1024 * 1024 * 1024,
        disallow_cross_region: bool = True,
        s3_endpoint_url: Optional[str] = None,
        # Optional cluster hint for namespace isolation
        cluster_id: str = "default",
        database: str = "default",
    ):
        """
        Create a Arrowjet Engine with staging configuration.

        Args:
            staging_bucket: S3 bucket for staging (same region as Redshift)
            staging_iam_role: IAM role ARN that Redshift can assume for S3 access
            staging_region: AWS region of the staging bucket
            staging_prefix: S3 key prefix for staged files
            staging_cleanup: When to clean up staged files
                             ("on_success", "always", "never", "ttl_managed")
            staging_encryption: Encryption mode ("none", "sse_s3", "sse_kms")
            staging_kms_key_id: KMS key ARN (required if staging_encryption="sse_kms")
            max_concurrent_bulk_ops: Max parallel UNLOAD/COPY operations
            max_staging_bytes: Max bytes per staging operation
            disallow_cross_region: Reject staging buckets in a different region
            s3_endpoint_url: Override S3 endpoint (for VPC endpoints or testing)
            cluster_id: Cluster identifier for S3 namespace isolation
            database: Database name for S3 namespace isolation
        """
        config = StagingConfig(
            bucket=staging_bucket,
            iam_role=staging_iam_role,
            region=staging_region,
            prefix=staging_prefix,
            cleanup_policy=_CLEANUP_MAP.get(staging_cleanup, CleanupPolicy.ON_SUCCESS),
            encryption=_ENC_MAP.get(staging_encryption, EncryptionMode.NONE),
            kms_key_id=staging_kms_key_id,
            max_concurrent_ops=max_concurrent_bulk_ops,
            max_staging_bytes=max_staging_bytes,
            disallow_cross_region=disallow_cross_region,
            s3_endpoint_url=s3_endpoint_url,
        )

        self._staging_manager = StagingManager(
            config=config,
            cluster_id=cluster_id,
            database=database,
        )
        self._bulk_reader = BulkReader(self._staging_manager)
        self._bulk_writer = BulkWriter(self._staging_manager)

        logger.info(
            "Arrowjet Engine ready (bucket=%s, prefix=%s)",
            staging_bucket, staging_prefix,
        )

    def read_bulk(self, conn, query: str, **kwargs) -> ReadResult:
        """
        Bulk read via UNLOAD → S3 → Parquet → Arrow.

        Args:
            conn: Any DBAPI-compatible connection (redshift_connector, psycopg2, etc.)
            query: SELECT query to execute via UNLOAD
            **kwargs: Passed to BulkReader.read() (e.g., parallel=False)

        Returns:
            ReadResult with Arrow table, row count, timing, and S3 path
        """
        return self._bulk_reader.read(
            conn, query, autocommit=True, explicit_mode=True, **kwargs
        )

    def write_bulk(self, conn, table: pa.Table, target_table: str, **kwargs) -> WriteResult:
        """
        Bulk write via Arrow → Parquet → S3 → COPY.

        Args:
            conn: Any DBAPI-compatible connection
            table: PyArrow Table to write
            target_table: Target Redshift table name
            **kwargs: Passed to BulkWriter.write()

        Returns:
            WriteResult with row count, timing, and S3 path
        """
        return self._bulk_writer.write(conn, table, target_table, **kwargs)

    def write_dataframe(self, conn, df, target_table: str, **kwargs) -> WriteResult:
        """
        Bulk write a pandas DataFrame via COPY.

        Args:
            conn: Any DBAPI-compatible connection
            df: pandas DataFrame to write
            target_table: Target Redshift table name

        Returns:
            WriteResult with row count, timing, and S3 path
        """
        table = pa.Table.from_pandas(df, preserve_index=False)
        return self.write_bulk(conn, table, target_table, **kwargs)

    def __repr__(self):
        cfg = self._staging_manager.config
        return f"Engine(bucket={cfg.bucket}, region={cfg.region})"


class PostgreSQLEngine:
    """
    Bulk data movement engine for PostgreSQL — no cloud staging required.

    Uses PostgreSQL's native COPY protocol for high-speed bulk transfers.
    Works with psycopg2 or psycopg3 connections to any PostgreSQL-compatible
    database: Aurora PostgreSQL, RDS PostgreSQL, or self-hosted PostgreSQL.

    Usage:
        import arrowjet
        import psycopg2

        conn = psycopg2.connect(host=..., dbname=..., ...)

        engine = arrowjet.PostgreSQLEngine()

        # Bulk write — COPY FROM STDIN (10-50x faster than INSERT)
        engine.write_dataframe(conn, df, "my_table")

        # Bulk read — COPY TO STDOUT (2-5x faster than cursor.fetchall)
        result = engine.read_bulk(conn, "SELECT * FROM my_table")
        df = result.to_pandas()
    """

    def __init__(self):
        from .providers.postgresql import PostgreSQLProvider
        self._provider = PostgreSQLProvider()
        logger.info("Arrowjet PostgreSQLEngine ready (COPY protocol, no S3 staging)")

    def read_bulk(self, conn, query: str):
        """
        Bulk read via COPY (query) TO STDOUT → Arrow.

        Args:
            conn: psycopg2 or psycopg3 connection
            query: SELECT query to execute

        Returns:
            PgReadResult with Arrow table, row count, and timing
        """
        return self._provider.read_bulk(conn, query)

    def write_bulk(self, conn, table: pa.Table, target_table: str):
        """
        Bulk write via Arrow → COPY FROM STDIN.

        Args:
            conn: psycopg2 or psycopg3 connection
            table: PyArrow Table to write
            target_table: Target table name (must exist)

        Returns:
            PgWriteResult with row count and timing
        """
        return self._provider.write_bulk(conn, table, target_table)

    def write_dataframe(self, conn, df, target_table: str):
        """
        Bulk write a pandas DataFrame via COPY.

        Args:
            conn: psycopg2 or psycopg3 connection
            df: pandas DataFrame to write
            target_table: Target table name (must exist)

        Returns:
            PgWriteResult with row count and timing
        """
        return self._provider.write_dataframe(conn, df, target_table)

    def __repr__(self):
        return "PostgreSQLEngine(protocol=COPY)"
