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
from typing import Any, Callable, Optional

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
        self._hooks = {
            "on_read_complete": [],
            "on_write_complete": [],
            "on_transfer_complete": [],
        }

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

    def read_bulk(self, conn: Any, query: str, **kwargs: Any) -> Any:
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
            result = self._pg_provider.read_bulk(conn, query)
            self._fire_hooks("on_read_complete", result)
            return result
        if self._is_mysql:
            result = self._mysql_provider.read_bulk(conn, query)
            self._fire_hooks("on_read_complete", result)
            return result
        result = self._bulk_reader.read(
            conn, query, autocommit=True, explicit_mode=True, **kwargs
        )
        self._fire_hooks("on_read_complete", result)
        return result

    def read_bulk_iter(self, conn: Any, query: str, batch_size: int = 10000, **kwargs: Any):
        """
        Bulk read with chunked iteration for memory-constrained environments.

        Yields Arrow RecordBatches instead of materializing the full result.
        Useful for Lambda, notebooks, or very large result sets.

        PostgreSQL: COPY TO STDOUT -> parse in chunks
        MySQL: cursor fetch in batches -> Arrow batches
        Redshift: UNLOAD -> download Parquet files one at a time -> yield batches

        Args:
            conn: DBAPI-compatible connection
            query: SELECT query to execute
            batch_size: Number of rows per batch (PostgreSQL/MySQL only)

        Yields:
            pyarrow.RecordBatch
        """
        if self._is_pg:
            yield from self._pg_read_iter(conn, query, batch_size)
            return
        if self._is_mysql:
            yield from self._mysql_read_iter(conn, query, batch_size)
            return
        # Redshift: download parquet files one at a time
        yield from self._redshift_read_iter(conn, query, **kwargs)

    def _pg_read_iter(self, conn, query: str, batch_size: int):
        """Chunked read for PostgreSQL using server-side cursor."""
        import pyarrow as _pa

        cursor = conn.cursor(name="arrowjet_chunked")
        cursor.itersize = batch_size
        cursor.execute(query)

        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            columns = [desc[0] for desc in cursor.description]
            arrays = []
            for i, col in enumerate(columns):
                values = [row[i] for row in rows]
                arrays.append(_pa.array(values))
            batch = _pa.RecordBatch.from_arrays(arrays, names=columns)
            yield batch

        cursor.close()

    def _mysql_read_iter(self, conn, query: str, batch_size: int):
        """Chunked read for MySQL using cursor fetchmany."""
        import pyarrow as _pa

        cursor = conn.cursor()
        cursor.execute(query)

        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            columns = [desc[0] for desc in cursor.description]
            arrays = []
            for i, col in enumerate(columns):
                values = [row[i] for row in rows]
                arrays.append(_pa.array(values))
            batch = _pa.RecordBatch.from_arrays(arrays, names=columns)
            yield batch

        cursor.close()

    def _redshift_read_iter(self, conn, query: str, **kwargs):
        """Chunked read for Redshift: UNLOAD then yield one Parquet file at a time."""
        import pyarrow.parquet as _pq

        from .bulk.eligibility import check_read_eligibility

        eligibility = check_read_eligibility(
            query=query, autocommit=True, explicit_mode=True, staging_valid=True,
        )
        if not eligibility:
            from .bulk.reader import ReadNotEligibleError
            raise ReadNotEligibleError(eligibility.reason)

        op = self._staging_manager.begin_operation("read")
        try:
            export_cmd = self._bulk_reader._provider.build_export_sql(
                query=query, staging_path=op.path.s3_uri, parallel=True,
            )
            cursor = conn.cursor()
            cursor.execute(export_cmd.sql)

            files = self._staging_manager.downloader.list_parquet_files(op.path)
            for f in files:
                table = self._staging_manager.downloader.read_single_parquet(
                    op.path, f.key
                ) if hasattr(self._staging_manager.downloader, 'read_single_parquet') else (
                    _pq.read_table(f"s3://{self._staging_manager.config.bucket}/{f.key}")
                )
                for batch in table.to_batches():
                    yield batch
        finally:
            self._staging_manager.complete_operation(op)

    def write_bulk(self, conn: Any, table: pa.Table, target_table: str, **kwargs: Any) -> Any:
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
            result = self._pg_provider.write_bulk(conn, table, target_table)
            self._fire_hooks("on_write_complete", result)
            return result
        if self._is_mysql:
            result = self._mysql_provider.write_bulk(conn, table, target_table)
            self._fire_hooks("on_write_complete", result)
            return result
        result = self._bulk_writer.write(conn, table, target_table, **kwargs)
        self._fire_hooks("on_write_complete", result)
        return result

    def write_dataframe(self, conn: Any, df: Any, target_table: str, **kwargs: Any) -> Any:
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

    def on(self, event: str, callback: Callable) -> Engine:
        """
        Register a lifecycle hook.

        Supported events:
          - "on_read_complete": called after every read_bulk with (engine, result)
          - "on_write_complete": called after every write_bulk with (engine, result)
          - "on_transfer_complete": called after every transfer with (engine, result)

        Usage:
            engine.on("on_write_complete", lambda eng, res: print(f"Wrote {res.rows} rows"))

        Pro extensions use this to add validation, metrics, and audit logging
        without modifying the core engine.
        """
        if event not in self._hooks:
            raise ValueError(f"Unknown hook event: '{event}'. Supported: {list(self._hooks.keys())}")
        self._hooks[event].append(callback)
        return self

    def _fire_hooks(self, event: str, result: Any) -> None:
        """Fire all registered callbacks for an event."""
        for callback in self._hooks.get(event, []):
            try:
                callback(self, result)
            except Exception as e:
                logger.warning("Hook %s raised: %s", event, e)

    def __repr__(self) -> str:
        if self._is_pg:
            return "Engine(provider=postgresql)"
        if self._is_mysql:
            return "Engine(provider=mysql)"
        cfg = self._staging_manager.config
        return f"Engine(provider=redshift, bucket={cfg.bucket})"


# Backward-compatible alias
PostgreSQLEngine = lambda: Engine(provider="postgresql")
PostgreSQLEngine.__doc__ = "Alias for Engine(provider='postgresql'). Deprecated  - use Engine(provider='postgresql') instead."
