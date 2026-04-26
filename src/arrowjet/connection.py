"""
Unified Arrowjet connection  - the main entry point.

Provides both safe mode (DBAPI via ADBC PG driver) and bulk mode
(read_bulk/write_bulk via the staging engine) through a single connect() call.

Usage:
    import arrowjet as arrowjet

    conn = arrowjet.connect(
        host="...", database="dev", user="awsuser", password="...",
        staging_bucket="my-bucket", staging_iam_role="arn:...",
    )

    # Safe mode (DBAPI-compatible)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE id = %s", (42,))
    row = cursor.fetchone()
    df = conn.fetch_dataframe("SELECT * FROM users")

    # Bulk mode (explicit)
    table = conn.read_bulk("SELECT * FROM events")
    conn.write_bulk(arrow_table, "target_table")
"""

from __future__ import annotations

import logging
from typing import Optional

try:
    import adbc_driver_postgresql.dbapi
    import redshift_connector
except ImportError:
    raise ImportError(
        "arrowjet.connect() requires the Redshift driver extras.\n"
        "Install with: pip install arrowjet[redshift]"
    )

import pyarrow as pa
import adbc_driver_postgresql.dbapi
import redshift_connector

from .staging.config import StagingConfig, CleanupPolicy, EncryptionMode, QueueBehavior
from .staging.manager import StagingManager
from .bulk.writer import BulkWriter, WriteResult
from .bulk.reader import BulkReader, ReadResult
from .observability import ConnectionMetrics, BulkOperationEvent, TracingHook, CostLogger, CostLogLevel, _noop_hook
from .auto_mode import AutoRouter, AutoModeConfig, ReadMode, RoutingDecision

logger = logging.getLogger(__name__)


def connect(
    host: str,
    database: str = "dev",
    user: str = "awsuser",
    password: str = "",
    port: int = 5439,
    # Authentication
    auth_type: str = "password",
    db_user: Optional[str] = None,
    db_groups: Optional[list] = None,
    auto_create: bool = False,
    duration_seconds: int = 900,
    secret_arn: Optional[str] = None,
    aws_region: Optional[str] = None,
    aws_profile: Optional[str] = None,
    # Staging config (required for bulk mode)
    staging_bucket: Optional[str] = None,
    staging_iam_role: Optional[str] = None,
    staging_region: Optional[str] = None,
    staging_prefix: str = "arrowjet-staging",
    staging_cleanup: str = "on_success",
    staging_encryption: str = "none",
    staging_kms_key_id: Optional[str] = None,
    # Concurrency
    max_concurrent_bulk_ops: int = 4,
    # Safety
    max_staging_bytes: int = 10 * 1024 * 1024 * 1024,
    disallow_cross_region: bool = True,
    # S3 endpoint override
    s3_endpoint_url: Optional[str] = None,
    # Observability
    cost_logging: str = "off",
    on_bulk_operation: Optional[TracingHook] = None,
    # Auto mode
    read_mode: str = "direct",
    auto_mode_threshold_rows: int = 100_000,
    auto_mode_threshold_bytes: int = 50_000_000,
) -> ArrowjetConnection:
    """
    Create an Arrowjet connection to Redshift.

    Provides both safe mode (DBAPI) and bulk mode (read_bulk/write_bulk).
    Staging parameters are required for bulk operations.

    Authentication:
        auth_type="password" (default)  - uses user/password directly.
        auth_type="iam"  - fetches temporary credentials via IAM
            (GetClusterCredentials for provisioned, GetCredentials for serverless).
        auth_type="secrets_manager"  - fetches credentials from AWS Secrets Manager.

    For IAM and Secrets Manager, boto3 must be installed and AWS credentials
    must be available (environment, IAM role, or ~/.aws/credentials).
    """
    # Resolve credentials
    from .auth.redshift import resolve_credentials

    creds = resolve_credentials(
        host=host,
        auth_type=auth_type,
        port=port,
        database=database,
        user=user,
        password=password,
        db_user=db_user,
        db_groups=db_groups,
        auto_create=auto_create,
        duration_seconds=duration_seconds,
        secret_arn=secret_arn,
        region=aws_region or staging_region,
        profile=aws_profile,
    )

    return ArrowjetConnection(
        host=creds.host, database=creds.database,
        user=creds.user, password=creds.password, port=creds.port,
        staging_bucket=staging_bucket, staging_iam_role=staging_iam_role,
        staging_region=staging_region, staging_prefix=staging_prefix,
        staging_cleanup=staging_cleanup, staging_encryption=staging_encryption,
        staging_kms_key_id=staging_kms_key_id,
        max_concurrent_bulk_ops=max_concurrent_bulk_ops,
        max_staging_bytes=max_staging_bytes,
        disallow_cross_region=disallow_cross_region,
        s3_endpoint_url=s3_endpoint_url,
        cost_logging=cost_logging,
        on_bulk_operation=on_bulk_operation,
        read_mode=read_mode,
        auto_mode_threshold_rows=auto_mode_threshold_rows,
        auto_mode_threshold_bytes=auto_mode_threshold_bytes,
    )


class ArrowjetConnection:
    """
    Unified connection providing safe mode + bulk mode.

    Safe mode: DBAPI-compatible cursor via ADBC PostgreSQL driver.
    Bulk mode: read_bulk/write_bulk via S3 staging engine.
    """

    def __init__(self, host, database, user, password, port, **staging_kwargs):
        self._host = host
        self._database = database
        self._user = user
        self._password = password
        self._port = port

        # Safe mode connection (ADBC PG driver)
        from urllib.parse import quote_plus
        uri = f"postgresql://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{database}"
        self._adbc_conn = adbc_driver_postgresql.dbapi.connect(uri)

        # Also keep a redshift_connector for COPY/UNLOAD commands
        # (ADBC PG driver's Redshift support is experimental)
        self._rs_conn = redshift_connector.connect(
            host=host, port=port, database=database,
            user=user, password=password,
        )
        self._rs_conn.autocommit = True
        self._autocommit = False  # safe mode default

        # Observability
        self._metrics = ConnectionMetrics()
        cost_level_map = {"off": CostLogLevel.OFF, "summary": CostLogLevel.SUMMARY, "verbose": CostLogLevel.VERBOSE}
        self._cost_logger = CostLogger(cost_level_map.get(
            staging_kwargs.get("cost_logging", "off"), CostLogLevel.OFF
        ))
        self._tracing_hook: TracingHook = staging_kwargs.get("on_bulk_operation") or _noop_hook

        # Auto mode
        read_mode_str = staging_kwargs.get("read_mode", "direct")
        self._read_mode = ReadMode(read_mode_str) if read_mode_str in ("direct", "unload", "auto") else ReadMode.DIRECT
        self._auto_router = AutoRouter(AutoModeConfig(
            threshold_rows=staging_kwargs.get("auto_mode_threshold_rows", 100_000),
            threshold_bytes=staging_kwargs.get("auto_mode_threshold_bytes", 50_000_000),
            use_explain=True,
            require_hint=(self._read_mode != ReadMode.AUTO),
        ))

        # Bulk mode (optional  - only if staging config provided)
        self._staging_manager = None
        self._bulk_writer = None
        self._bulk_reader = None

        bucket = staging_kwargs.get("staging_bucket")
        iam_role = staging_kwargs.get("staging_iam_role")
        region = staging_kwargs.get("staging_region")

        if bucket and iam_role and region:
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

            config = StagingConfig(
                bucket=bucket,
                iam_role=iam_role,
                region=region,
                prefix=staging_kwargs.get("staging_prefix", "arrowjet-staging"),
                cleanup_policy=cleanup_map.get(
                    staging_kwargs.get("staging_cleanup", "on_success"),
                    CleanupPolicy.ON_SUCCESS,
                ),
                encryption=enc_map.get(
                    staging_kwargs.get("staging_encryption", "none"),
                    EncryptionMode.NONE,
                ),
                kms_key_id=staging_kwargs.get("staging_kms_key_id"),
                max_concurrent_ops=staging_kwargs.get("max_concurrent_bulk_ops", 4),
                max_staging_bytes=staging_kwargs.get("max_staging_bytes", 10 * 1024**3),
                disallow_cross_region=staging_kwargs.get("disallow_cross_region", True),
                s3_endpoint_url=staging_kwargs.get("s3_endpoint_url"),
            )

            # Extract cluster ID from host
            cluster_id = host.split(".")[0] if "." in host else "unknown"

            self._staging_manager = StagingManager(
                config=config, cluster_id=cluster_id, database=database,
            )
            self._bulk_writer = BulkWriter(self._staging_manager)
            self._bulk_reader = BulkReader(self._staging_manager)

            logger.info("Arrowjet connected: safe + bulk mode (bucket=%s)", bucket)
        else:
            logger.info("Arrowjet connected: safe mode only (no staging config)")

    # ── Safe mode (DBAPI) ─────────────────────────────────────

    def cursor(self):
        """Return a DBAPI cursor for safe-mode queries."""
        return self._adbc_conn.cursor()

    def execute(self, query: str, parameters=None):
        """Execute a query in safe mode. Returns cursor."""
        cursor = self.cursor()
        cursor.execute(query, parameters)
        return cursor

    def fetch_dataframe(self, query: str, parameters=None):
        """Execute query and return pandas DataFrame (safe mode)."""
        cursor = self.execute(query, parameters)
        table = cursor.fetch_arrow_table()
        return table.to_pandas()

    def fetch_numpy_array(self, query: str, parameters=None):
        """Execute query and return numpy array (safe mode)."""
        cursor = self.execute(query, parameters)
        table = cursor.fetch_arrow_table()
        df = table.to_pandas()
        return df.to_numpy()

    def fetch_arrow_table(self, query: str, parameters=None) -> pa.Table:
        """Execute query and return Arrow table (safe mode)."""
        cursor = self.execute(query, parameters)
        return cursor.fetch_arrow_table()

    # ── Metadata (safe mode only) ─────────────────────────────

    def get_tables(self, schema: str = "public") -> list[str]:
        """List table names in a schema. Always uses safe mode."""
        cursor = self.cursor()
        cursor.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = $1 ORDER BY table_name",
            (schema,),
        )
        return [row[0] for row in cursor.fetchall()]

    def get_columns(self, table_name: str, schema: str = "public") -> list[dict]:
        """Get column info for a table. Always uses safe mode."""
        cursor = self.cursor()
        cursor.execute(
            "SELECT column_name, data_type, is_nullable, column_default "
            "FROM information_schema.columns "
            "WHERE table_schema = $1 AND table_name = $2 "
            "ORDER BY ordinal_position",
            (schema, table_name),
        )
        return [
            {"name": r[0], "type": r[1], "nullable": r[2] == "YES", "default": r[3]}
            for r in cursor.fetchall()
        ]

    def get_schemas(self) -> list[str]:
        """List schema names. Always uses safe mode."""
        cursor = self.cursor()
        cursor.execute(
            "SELECT nspname FROM pg_namespace "
            "WHERE nspname NOT LIKE 'pg_%' "
            "ORDER BY nspname"
        )
        return [row[0] for row in cursor.fetchall()]

    # ── Transaction support (safe mode) ───────────────────────

    def commit(self):
        """Commit the current transaction (safe mode)."""
        self._adbc_conn.commit()

    def rollback(self):
        """Rollback the current transaction (safe mode)."""
        self._adbc_conn.rollback()

    @property
    def autocommit(self) -> bool:
        try:
            return self._adbc_conn.autocommit
        except AttributeError:
            return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool):
        try:
            self._adbc_conn.autocommit = value
        except AttributeError:
            pass
        self._autocommit = value


    # ── Bulk mode (explicit) ──────────────────────────────────

    def read_bulk(self, query: str, **kwargs) -> ReadResult:
        """
        Bulk read via UNLOAD -> S3 -> Parquet -> Arrow.

        User knows: this uses UNLOAD, hits S3, not transactional.
        Requires staging config.
        """
        if not self._bulk_reader:
            raise ArrowjetError(
                "Bulk mode not available. Provide staging_bucket, "
                "staging_iam_role, and staging_region to connect()."
            )
        try:
            result = self._bulk_reader.read(
                self._rs_conn, query, autocommit=True, explicit_mode=True, **kwargs,
            )
            self._metrics.record_bulk_read(result.bytes_staged, result.files_count + 1)
            self._cost_logger.log_operation(
                "bulk_read", result.rows, result.bytes_staged,
                result.files_count, result.total_time_s, result.s3_path,
            )
            self._tracing_hook(BulkOperationEvent(
                operation_type="read", success=True, rows=result.rows,
                bytes_staged=result.bytes_staged, files_count=result.files_count,
                s3_requests=result.files_count + 1, duration_s=result.total_time_s,
                path_chosen="unload", s3_path=result.s3_path,
            ))
            return result
        except Exception as e:
            self._metrics.record_error()
            self._tracing_hook(BulkOperationEvent(
                operation_type="read", success=False, rows=0,
                bytes_staged=0, files_count=0, s3_requests=0,
                duration_s=0, path_chosen="unload", s3_path="", error=str(e),
            ))
            raise

    def read_auto(self, query: str, bulk_hint: bool = False, **kwargs):
        """
        Auto-routed read  - router decides direct vs bulk based on query analysis.

        In Phase 1 (conservative): requires bulk_hint=True to use bulk path.
        In Phase 2 (read_mode="auto"): uses EXPLAIN to estimate and decide.

        Returns either a pandas DataFrame (direct) or ReadResult (bulk).
        """
        routing = self._auto_router.route(
            self._rs_conn, query,
            autocommit=self._autocommit or True,
            bulk_hint=bulk_hint,
        )

        logger.info(
            "Auto routing: %s  - %s",
            routing.decision.value, routing.reason,
        )

        if routing.decision == RoutingDecision.BULK and self._bulk_reader:
            return self.read_bulk(query, **kwargs)
        else:
            return self.fetch_arrow_table(query)

    def write_bulk(self, table: pa.Table, target_table: str, **kwargs) -> WriteResult:
        """
        Bulk write via Arrow -> Parquet -> S3 -> COPY.

        User knows: this uses COPY, stages through S3.
        Requires staging config.
        """
        if not self._bulk_writer:
            raise ArrowjetError(
                "Bulk mode not available. Provide staging_bucket, "
                "staging_iam_role, and staging_region to connect()."
            )
        try:
            result = self._bulk_writer.write(self._rs_conn, table, target_table, **kwargs)
            self._metrics.record_bulk_write(result.bytes_staged, 2)  # PUT + DELETE
            self._cost_logger.log_operation(
                "bulk_write", result.rows, result.bytes_staged,
                1, result.total_time_s, result.s3_path,
            )
            self._tracing_hook(BulkOperationEvent(
                operation_type="write", success=True, rows=result.rows,
                bytes_staged=result.bytes_staged, files_count=1,
                s3_requests=2, duration_s=result.total_time_s,
                path_chosen="copy", s3_path=result.s3_path,
            ))
            return result
        except Exception as e:
            self._metrics.record_error()
            self._tracing_hook(BulkOperationEvent(
                operation_type="write", success=False, rows=0,
                bytes_staged=0, files_count=0, s3_requests=0,
                duration_s=0, path_chosen="copy", s3_path="", error=str(e),
            ))
            raise

    def write_dataframe(self, df, target_table: str, **kwargs) -> WriteResult:
        """Bulk write a pandas DataFrame via COPY."""
        table = pa.Table.from_pandas(df, preserve_index=False)
        return self.write_bulk(table, target_table, **kwargs)

    # ── Connection management ─────────────────────────────────

    @property
    def has_bulk(self) -> bool:
        """Whether bulk mode is available."""
        return self._staging_manager is not None

    def get_metrics(self) -> ConnectionMetrics:
        """Return cumulative metrics for this connection."""
        return self._metrics

    def close(self):
        """Close all connections."""
        try:
            self._adbc_conn.close()
        except Exception:
            pass
        try:
            self._rs_conn.close()
        except Exception:
            pass
        logger.info("Arrowjet connection closed")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __repr__(self):
        mode = "safe+bulk" if self.has_bulk else "safe-only"
        return f"ArrowjetConnection(host={self._host}, mode={mode})"


class ArrowjetError(Exception):
    pass
