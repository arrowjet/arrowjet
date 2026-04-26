"""
Bulk Read Engine  - UNLOAD -> S3 -> Parquet -> Arrow.

Production implementation built on M1 staging subsystem.
Replaces the M0 prototype with proper lifecycle management,
eligibility checking, error handling, and cleanup.

Provider-agnostic: uses BulkProvider interface for database-specific
export commands. Currently supports Redshift via RedshiftProvider.
"""

from __future__ import annotations

import logging
import time
from typing import Optional

import pyarrow as pa

from ..staging.manager import StagingManager
from ..staging.lifecycle import OperationState
from ..providers.base import BulkProvider
from ..providers.redshift import RedshiftProvider
from .eligibility import check_read_eligibility, EligibilityResult

logger = logging.getLogger(__name__)


class BulkReader:
    """
    Reads from Redshift via the UNLOAD path.

    Usage:
        reader = BulkReader(staging_manager)
        result = reader.read(conn, "SELECT * FROM my_table")
    """

    def __init__(self, staging_manager: StagingManager, provider: Optional[BulkProvider] = None):
        self._staging = staging_manager
        # Default to RedshiftProvider for backward compatibility
        self._provider = provider or RedshiftProvider(staging_manager.config)

    def read(
        self,
        conn,
        query: str,
        autocommit: bool = True,
        explicit_mode: bool = False,
        parallel: bool = True,
    ) -> ReadResult:
        """
        Read query results from Redshift via UNLOAD.

        Pipeline: UNLOAD -> S3 (Parquet) -> download -> Arrow table

        Args:
            conn: redshift_connector Connection
            query: SELECT query to execute
            autocommit: Whether connection is in autocommit mode
            explicit_mode: If True, bypass autocommit eligibility check
            parallel: Enable parallel UNLOAD across slices

        Returns:
            ReadResult with Arrow table, timing, and metadata

        Raises:
            BulkReadError: on any failure
            ReadNotEligibleError: if query fails eligibility check
        """
        # Check eligibility
        eligibility = check_read_eligibility(
            query=query,
            autocommit=autocommit,
            explicit_mode=explicit_mode,
            staging_valid=True,
        )
        if not eligibility:
            raise ReadNotEligibleError(eligibility.reason)

        op = self._staging.begin_operation("read")
        start = time.perf_counter()

        try:
            # Phase 1: Execute export command
            op.transition(OperationState.FILES_STAGING)
            t = time.perf_counter()

            export_cmd = self._provider.build_export_sql(
                query=query,
                staging_path=op.path.s3_uri,
                parallel=parallel,
            )

            cursor = conn.cursor()
            try:
                cursor.execute(export_cmd.sql)
            except Exception as e:
                raise BulkReadError(
                    f"Export failed: {e}\n"
                    f"SQL: {export_cmd.sql}\n"
                    f"Staging path: {op.path.s3_uri}"
                ) from e

            unload_time = time.perf_counter() - t

            # Phase 2: List staged files
            t = time.perf_counter()
            files = self._staging.downloader.list_parquet_files(op.path)
            list_time = time.perf_counter() - t

            if not files:
                # UNLOAD produced no files  - query returned 0 rows
                op.transition(OperationState.COMMAND_SUBMITTED)
                op.transition(OperationState.COMPLETED)
                op.rows_affected = 0

                total_time = time.perf_counter() - start
                return ReadResult(
                    table=pa.table({}),
                    rows=0,
                    files_count=0,
                    bytes_staged=0,
                    unload_time_s=round(unload_time, 3),
                    download_time_s=0.0,
                    total_time_s=round(total_time, 3),
                    s3_path=op.path.s3_uri,
                )

            staged_bytes = sum(f.size_bytes for f in files)
            op.files_count = len(files)
            op.bytes_staged = staged_bytes
            op.transition(OperationState.COMMAND_SUBMITTED)

            logger.info(
                "UNLOAD complete: %d files, %d bytes staged at %s in %.2fs",
                len(files), staged_bytes, op.path.s3_uri, unload_time,
            )

            # Phase 3: Download Parquet -> Arrow
            t = time.perf_counter()
            table = self._staging.downloader.read_parquet(op.path)
            download_time = time.perf_counter() - t

            op.transition(OperationState.COMPLETED)
            op.rows_affected = table.num_rows
            total_time = time.perf_counter() - start

            result = ReadResult(
                table=table,
                rows=table.num_rows,
                files_count=len(files),
                bytes_staged=staged_bytes,
                unload_time_s=round(unload_time, 3),
                download_time_s=round(download_time, 3),
                total_time_s=round(total_time, 3),
                s3_path=op.path.s3_uri,
            )

            logger.info(
                "Bulk read complete: %d rows in %.2fs "
                "(unload=%.2fs, download=%.2fs, files=%d, staged=%d bytes)",
                result.rows, result.total_time_s,
                result.unload_time_s, result.download_time_s,
                result.files_count, result.bytes_staged,
            )

            return result

        except (BulkReadError, ReadNotEligibleError):
            op.fail(str(op.error or "read failed"))
            raise
        except Exception as e:
            op.fail(str(e))
            raise BulkReadError(f"Bulk read failed: {e}") from e
        finally:
            self._staging.complete_operation(op)


class ReadResult:
    """Result of a bulk read operation."""

    __slots__ = (
        "table", "rows", "files_count", "bytes_staged",
        "unload_time_s", "download_time_s", "total_time_s",
        "s3_path",
    )

    def __init__(
        self,
        table: pa.Table,
        rows: int,
        files_count: int,
        bytes_staged: int,
        unload_time_s: float,
        download_time_s: float,
        total_time_s: float,
        s3_path: str,
    ):
        self.table = table
        self.rows = rows
        self.files_count = files_count
        self.bytes_staged = bytes_staged
        self.unload_time_s = unload_time_s
        self.download_time_s = download_time_s
        self.total_time_s = total_time_s
        self.s3_path = s3_path

    def to_pandas(self):
        """Convert result to pandas DataFrame."""
        return self.table.to_pandas()

    def __repr__(self):
        return (
            f"ReadResult(rows={self.rows:,}, files={self.files_count}, "
            f"staged={self.bytes_staged:,} bytes, total={self.total_time_s}s)"
        )


class BulkReadError(Exception):
    pass


class ReadNotEligibleError(Exception):
    pass
