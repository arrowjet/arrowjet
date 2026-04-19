"""
Bulk Write Engine — Arrow → Parquet → S3 → COPY.

Production implementation built on M1 staging subsystem.
Replaces the M0 prototype with proper lifecycle management,
error handling, observability, and cleanup.

Provider-agnostic: uses BulkProvider interface for database-specific
import commands. Currently supports Redshift via RedshiftProvider.
"""

from __future__ import annotations

import logging
import time
from typing import Optional

import pyarrow as pa

from ..staging.config import StagingConfig
from ..staging.manager import StagingManager
from ..staging.lifecycle import OperationState
from ..providers.base import BulkProvider
from ..providers.redshift import RedshiftProvider

logger = logging.getLogger(__name__)


class BulkWriter:
    """
    Writes Arrow tables to Redshift via the COPY path.

    Usage:
        writer = BulkWriter(staging_manager)
        result = writer.write(conn, table, "target_table")
    """

    def __init__(
        self,
        staging_manager: StagingManager,
        compression: str = "snappy",
        provider: Optional[BulkProvider] = None,
    ):
        self._staging = staging_manager
        self._compression = compression
        # Default to RedshiftProvider for backward compatibility
        self._provider = provider or RedshiftProvider(staging_manager.config)

    def write(
        self,
        conn,
        table: pa.Table,
        target_table: str,
        compression: Optional[str] = None,
    ) -> WriteResult:
        """
        Write an Arrow table to a Redshift table via COPY.

        Pipeline: Arrow → Parquet (temp file) → S3 → COPY → cleanup

        Args:
            conn: redshift_connector Connection
            table: PyArrow Table to write
            target_table: Redshift table name (must exist)
            compression: Parquet compression (default: snappy)

        Returns:
            WriteResult with timing and metadata

        Raises:
            BulkWriteError: on any failure (staged files cleaned per policy)
        """
        compression = compression or self._compression
        op = self._staging.begin_operation("write")
        start = time.perf_counter()

        try:
            # Phase 1: Upload Parquet to S3
            op.transition(OperationState.FILES_STAGING)
            t = time.perf_counter()
            upload_result = self._staging.uploader.upload_parquet(
                table, op.path,
                compression=compression,
            )
            upload_time = time.perf_counter() - t

            op.files_count = 1
            op.bytes_staged = upload_result.size_bytes

            logger.info(
                "Staged %d bytes (%s) to %s",
                upload_result.size_bytes, compression, op.path.s3_uri,
            )

            # Phase 2: Execute import command
            op.transition(OperationState.COMMAND_SUBMITTED)
            t = time.perf_counter()

            import_cmd = self._provider.build_import_sql(
                target_table=target_table,
                staging_path=f"s3://{self._staging.config.bucket}/{op.path.key_prefix}",
                compression=compression,
            )

            cursor = conn.cursor()
            try:
                cursor.execute(import_cmd.sql)
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise BulkWriteError(
                    f"Import failed for {target_table}: {e}\n"
                    f"SQL: {import_cmd.sql}\n"
                    f"Staged data: {op.path.s3_uri}"
                ) from e

            copy_time = time.perf_counter() - t
            op.rows_affected = table.num_rows

            # Phase 3: Mark completed
            op.transition(OperationState.COMPLETED)
            total_time = time.perf_counter() - start

            result = WriteResult(
                rows=table.num_rows,
                bytes_staged=upload_result.size_bytes,
                compression=compression,
                upload_time_s=round(upload_time, 3),
                copy_time_s=round(copy_time, 3),
                total_time_s=round(total_time, 3),
                s3_path=op.path.s3_uri,
                target_table=target_table,
            )

            logger.info(
                "Bulk write complete: %d rows to %s in %.2fs "
                "(upload=%.2fs, copy=%.2fs, staged=%d bytes)",
                result.rows, target_table, result.total_time_s,
                result.upload_time_s, result.copy_time_s, result.bytes_staged,
            )

            return result

        except BulkWriteError:
            op.fail(str(op.error or "COPY failed"))
            raise
        except Exception as e:
            op.fail(str(e))
            raise BulkWriteError(f"Bulk write failed: {e}") from e
        finally:
            self._staging.complete_operation(op)

    def write_dataframe(
        self,
        conn,
        df,
        target_table: str,
        compression: Optional[str] = None,
    ) -> WriteResult:
        """
        Write a pandas DataFrame to Redshift via COPY.

        Convenience method — converts DataFrame to Arrow, then calls write().
        Compatible with redshift_connector's write_dataframe() pattern.
        """
        table = pa.Table.from_pandas(df, preserve_index=False)
        return self.write(conn, table, target_table, compression=compression)


class WriteResult:
    """Result of a bulk write operation."""

    __slots__ = (
        "rows", "bytes_staged", "compression",
        "upload_time_s", "copy_time_s", "total_time_s",
        "s3_path", "target_table",
    )

    def __init__(
        self,
        rows: int,
        bytes_staged: int,
        compression: str,
        upload_time_s: float,
        copy_time_s: float,
        total_time_s: float,
        s3_path: str,
        target_table: str,
    ):
        self.rows = rows
        self.bytes_staged = bytes_staged
        self.compression = compression
        self.upload_time_s = upload_time_s
        self.copy_time_s = copy_time_s
        self.total_time_s = total_time_s
        self.s3_path = s3_path
        self.target_table = target_table

    def __repr__(self):
        return (
            f"WriteResult(rows={self.rows:,}, staged={self.bytes_staged:,} bytes, "
            f"total={self.total_time_s}s, table={self.target_table})"
        )


class BulkWriteError(Exception):
    pass
