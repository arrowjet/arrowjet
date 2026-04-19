"""
Integration tests for the bulk read engine — requires real Redshift + S3.

Tests the full pipeline: UNLOAD → S3 → Parquet → Arrow → verify.
Requires env vars: REDSHIFT_HOST, REDSHIFT_PASS, STAGING_BUCKET, STAGING_IAM_ROLE
"""

import os
import pytest
import pyarrow as pa

from arrowjet.bulk.reader import BulkReader, BulkReadError, ReadNotEligibleError, ReadResult

pytestmark = pytest.mark.skipif(
    not os.environ.get("REDSHIFT_HOST"),
    reason="REDSHIFT_HOST not set",
)


def _setup_test_table(conn, table_name, rows):
    """Create and populate a test table."""
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id BIGINT, value DOUBLE PRECISION)")
    conn.commit()

    if rows > 0:
        cursor.execute(
            f"INSERT INTO {table_name} "
            f"SELECT n, RANDOM() * 1000.0 "
            f"FROM (SELECT ROW_NUMBER() OVER () AS n FROM stv_blocklist LIMIT {rows})"
        )
        conn.commit()


def _drop_table(conn, table_name):
    conn.cursor().execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.commit()


class TestBulkReadEndToEnd:
    def test_read_and_verify(self, redshift_conn, staging_manager):
        _setup_test_table(redshift_conn, "m3_test_read", 5000)
        reader = BulkReader(staging_manager)

        try:
            result = reader.read(
                redshift_conn, "SELECT * FROM m3_test_read",
                autocommit=True,
            )
            assert isinstance(result, ReadResult)
            assert result.rows > 0  # stv_blocklist may return fewer than 5000
            assert result.files_count > 0
            assert result.bytes_staged > 0
            assert result.unload_time_s > 0
            assert result.download_time_s > 0
            assert result.table.num_columns == 2
        finally:
            _drop_table(redshift_conn, "m3_test_read")

    def test_read_to_pandas(self, redshift_conn, staging_manager):
        _setup_test_table(redshift_conn, "m3_test_pandas", 1000)
        reader = BulkReader(staging_manager)

        try:
            result = reader.read(
                redshift_conn, "SELECT * FROM m3_test_pandas",
                autocommit=True,
            )
            df = result.to_pandas()
            assert len(df) > 0
            assert "id" in df.columns
            assert "value" in df.columns
        finally:
            _drop_table(redshift_conn, "m3_test_pandas")

    def test_read_with_where_clause(self, redshift_conn, staging_manager):
        _setup_test_table(redshift_conn, "m3_test_where", 5000)
        reader = BulkReader(staging_manager)

        try:
            result = reader.read(
                redshift_conn, "SELECT * FROM m3_test_where WHERE id <= 100",
                autocommit=True,
            )
            assert result.rows <= 100
        finally:
            _drop_table(redshift_conn, "m3_test_where")

    def test_read_empty_result(self, redshift_conn, staging_manager):
        _setup_test_table(redshift_conn, "m3_test_empty", 0)
        reader = BulkReader(staging_manager)

        try:
            result = reader.read(
                redshift_conn, "SELECT * FROM m3_test_empty",
                autocommit=True,
            )
            assert result.rows == 0
        finally:
            _drop_table(redshift_conn, "m3_test_empty")

    def test_cleanup_after_read(self, redshift_conn, staging_manager):
        _setup_test_table(redshift_conn, "m3_test_cleanup", 1000)
        reader = BulkReader(staging_manager)

        try:
            reader.read(
                redshift_conn, "SELECT * FROM m3_test_cleanup",
                autocommit=True,
            )
            assert len(staging_manager.active_operations()) == 0
        finally:
            _drop_table(redshift_conn, "m3_test_cleanup")

    def test_not_eligible_without_autocommit(self, redshift_conn, staging_manager):
        reader = BulkReader(staging_manager)
        with pytest.raises(ReadNotEligibleError, match="autocommit"):
            reader.read(
                redshift_conn, "SELECT 1",
                autocommit=False,
            )

    def test_explicit_mode_bypasses_autocommit(self, redshift_conn, staging_manager):
        _setup_test_table(redshift_conn, "m3_test_explicit", 100)
        reader = BulkReader(staging_manager)

        try:
            result = reader.read(
                redshift_conn, "SELECT * FROM m3_test_explicit",
                autocommit=False,
                explicit_mode=True,
            )
            assert result.rows > 0
        finally:
            _drop_table(redshift_conn, "m3_test_explicit")

    def test_read_nonexistent_table_fails(self, redshift_conn, staging_manager):
        reader = BulkReader(staging_manager)
        _drop_table(redshift_conn, "m3_nonexistent")
        with pytest.raises(BulkReadError, match="Export failed"):
            reader.read(
                redshift_conn, "SELECT * FROM m3_nonexistent",
                autocommit=True,
            )


    def test_read_with_limit(self, redshift_conn, staging_manager):
        """LIMIT queries should work transparently — auto-wrapped by builder."""
        _setup_test_table(redshift_conn, "m3_test_limit", 500)
        reader = BulkReader(staging_manager)

        try:
            result = reader.read(
                redshift_conn, "SELECT * FROM m3_test_limit LIMIT 100",
                autocommit=True, explicit_mode=True,
            )
            assert result.rows == 100
        finally:
            _drop_table(redshift_conn, "m3_test_limit")

    def test_read_with_limit_offset(self, redshift_conn, staging_manager):
        """LIMIT + OFFSET queries should work transparently — auto-wrapped."""
        _setup_test_table(redshift_conn, "m3_test_offset", 500)
        reader = BulkReader(staging_manager)

        try:
            result = reader.read(
                redshift_conn, "SELECT * FROM m3_test_offset LIMIT 50 OFFSET 10",
                autocommit=True, explicit_mode=True,
            )
            assert result.rows == 50
        finally:
            _drop_table(redshift_conn, "m3_test_offset")
