"""
Integration tests for the bulk write engine — requires real Redshift + S3.

Tests the full pipeline: Arrow → Parquet → S3 → COPY → verify.
Run with: PYTHONPATH=src pytest tests/test_bulk_writer_integration.py -v

Requires env vars: REDSHIFT_HOST, REDSHIFT_PASS, STAGING_BUCKET, STAGING_IAM_ROLE
"""

import os
import pytest
import numpy as np
import pyarrow as pa

from arrowjet.bulk.writer import BulkWriter, BulkWriteError, WriteResult

pytestmark = pytest.mark.skipif(
    not os.environ.get("REDSHIFT_HOST"),
    reason="REDSHIFT_HOST not set — integration tests require env vars from .env",
)


def _make_table(rows=10000):
    return pa.table({
        "id": pa.array(np.arange(rows), type=pa.int64()),
        "value": pa.array(np.random.random(rows), type=pa.float64()),
        "name": pa.array([f"row_{i}" for i in range(rows)], type=pa.string()),
    })


def _create_target(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    cursor.execute(
        f"CREATE TABLE {table_name} (id BIGINT, value DOUBLE PRECISION, name VARCHAR(64))"
    )
    conn.commit()


def _count_rows(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    return cursor.fetchone()[0]


def _drop_table(conn, table_name):
    conn.cursor().execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.commit()


class TestBulkWriteEndToEnd:
    def test_write_and_verify(self, redshift_conn, staging_manager):
        writer = BulkWriter(staging_manager)
        table = _make_table(rows=10000)

        try:
            _create_target(redshift_conn, "m2_test_write")
            result = writer.write(redshift_conn, table, "m2_test_write")

            assert isinstance(result, WriteResult)
            assert result.rows == 10000
            assert result.bytes_staged > 0
            assert result.total_time_s > 0
            assert _count_rows(redshift_conn, "m2_test_write") == 10000
        finally:
            _drop_table(redshift_conn, "m2_test_write")

    def test_write_dataframe(self, redshift_conn, staging_manager):
        import pandas as pd
        writer = BulkWriter(staging_manager)

        df = pd.DataFrame({
            "id": np.arange(5000, dtype=np.int64),
            "value": np.random.random(5000),
            "name": [f"row_{i}" for i in range(5000)],
        })

        try:
            _create_target(redshift_conn, "m2_test_write_df")
            result = writer.write_dataframe(redshift_conn, df, "m2_test_write_df")
            assert result.rows == 5000
            assert _count_rows(redshift_conn, "m2_test_write_df") == 5000
        finally:
            _drop_table(redshift_conn, "m2_test_write_df")

    def test_write_large_batch(self, redshift_conn, staging_manager):
        writer = BulkWriter(staging_manager)
        table = _make_table(rows=100000)

        try:
            _create_target(redshift_conn, "m2_test_write_100k")
            result = writer.write(redshift_conn, table, "m2_test_write_100k")
            assert result.rows == 100000
            assert _count_rows(redshift_conn, "m2_test_write_100k") == 100000
        finally:
            _drop_table(redshift_conn, "m2_test_write_100k")

    def test_cleanup_after_success(self, redshift_conn, staging_manager):
        writer = BulkWriter(staging_manager)
        table = _make_table(rows=1000)

        try:
            _create_target(redshift_conn, "m2_test_cleanup")
            writer.write(redshift_conn, table, "m2_test_cleanup")
            assert len(staging_manager.active_operations()) == 0
        finally:
            _drop_table(redshift_conn, "m2_test_cleanup")

    def test_write_to_nonexistent_table_fails(self, redshift_conn, staging_manager):
        writer = BulkWriter(staging_manager)
        table = _make_table(rows=100)

        _drop_table(redshift_conn, "m2_nonexistent_table")
        with pytest.raises(BulkWriteError, match="Import failed"):
            writer.write(redshift_conn, table, "m2_nonexistent_table")

    def test_write_result_timing(self, redshift_conn, staging_manager):
        writer = BulkWriter(staging_manager)
        table = _make_table(rows=1000)

        try:
            _create_target(redshift_conn, "m2_test_timing")
            result = writer.write(redshift_conn, table, "m2_test_timing")

            assert result.upload_time_s >= 0
            assert result.copy_time_s >= 0
            assert result.total_time_s >= 0
            assert result.compression == "snappy"
        finally:
            _drop_table(redshift_conn, "m2_test_timing")
