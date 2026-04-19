"""
Integration tests for the BYOC Engine — requires real Redshift + S3.

Tests that Engine works with a user-provided redshift_connector connection,
validating the full UNLOAD → S3 → Parquet → Arrow pipeline.

Requires env vars: REDSHIFT_HOST, REDSHIFT_PASS, STAGING_BUCKET, STAGING_IAM_ROLE
"""

import os
import pytest
import pyarrow as pa
import redshift_connector

import arrowjet
from arrowjet.engine import Engine
from arrowjet.bulk.reader import ReadResult
from arrowjet.bulk.writer import WriteResult

pytestmark = pytest.mark.skipif(
    not os.environ.get("REDSHIFT_HOST"),
    reason="REDSHIFT_HOST not set",
)


@pytest.fixture
def user_conn():
    """A plain redshift_connector connection — what a user would bring."""
    conn = redshift_connector.connect(
        host=os.environ["REDSHIFT_HOST"],
        port=int(os.environ.get("REDSHIFT_PORT", "5439")),
        database=os.environ.get("REDSHIFT_DATABASE", "dev"),
        user=os.environ.get("REDSHIFT_USER", "awsuser"),
        password=os.environ["REDSHIFT_PASS"],
    )
    conn.autocommit = True
    yield conn
    conn.close()


@pytest.fixture
def engine():
    """A Arrowjet Engine with staging config — no connection details."""
    return Engine(
        staging_bucket=os.environ["STAGING_BUCKET"],
        staging_iam_role=os.environ["STAGING_IAM_ROLE"],
        staging_region=os.environ.get("STAGING_REGION", "us-east-1"),
        cluster_id="test",
        database="dev",
    )


def _setup_table(conn, table_name, rows):
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id BIGINT, value DOUBLE PRECISION)")
    if rows > 0:
        cursor.execute(
            f"INSERT INTO {table_name} "
            f"SELECT n, RANDOM() * 1000.0 "
            f"FROM (SELECT ROW_NUMBER() OVER () AS n FROM stv_blocklist LIMIT {rows})"
        )


def _drop_table(conn, table_name):
    conn.cursor().execute(f"DROP TABLE IF EXISTS {table_name}")


class TestEngineReadBulk:
    def test_read_bulk_returns_result(self, user_conn, engine):
        _setup_table(user_conn, "byoc_test_read", 1000)
        try:
            result = engine.read_bulk(user_conn, "SELECT * FROM byoc_test_read")
            assert isinstance(result, ReadResult)
            assert result.rows > 0
            assert result.table.num_columns == 2
        finally:
            _drop_table(user_conn, "byoc_test_read")

    def test_read_bulk_to_pandas(self, user_conn, engine):
        _setup_table(user_conn, "byoc_test_pandas", 500)
        try:
            result = engine.read_bulk(user_conn, "SELECT * FROM byoc_test_pandas")
            df = result.to_pandas()
            assert len(df) > 0
            assert "id" in df.columns
        finally:
            _drop_table(user_conn, "byoc_test_pandas")

    def test_read_bulk_with_limit(self, user_conn, engine):
        """LIMIT queries should work transparently via provider wrapping."""
        _setup_table(user_conn, "byoc_test_limit", 500)
        try:
            result = engine.read_bulk(user_conn, "SELECT * FROM byoc_test_limit LIMIT 100")
            assert result.rows == 100
        finally:
            _drop_table(user_conn, "byoc_test_limit")

    def test_read_bulk_cleanup(self, user_conn, engine):
        """Staged files should be cleaned up after read."""
        _setup_table(user_conn, "byoc_test_cleanup", 200)
        try:
            engine.read_bulk(user_conn, "SELECT * FROM byoc_test_cleanup")
            assert len(engine._staging_manager.active_operations()) == 0
        finally:
            _drop_table(user_conn, "byoc_test_cleanup")

    def test_engine_does_not_close_user_conn(self, user_conn, engine):
        """Engine must not close the user's connection."""
        _setup_table(user_conn, "byoc_test_noclose", 100)
        try:
            engine.read_bulk(user_conn, "SELECT * FROM byoc_test_noclose")
            # Connection should still be usable
            cursor = user_conn.cursor()
            cursor.execute("SELECT 1")
            assert cursor.fetchone()[0] == 1
        finally:
            _drop_table(user_conn, "byoc_test_noclose")


class TestEngineWriteBulk:
    def test_write_bulk_arrow_table(self, user_conn, engine):
        table = pa.table({"id": list(range(100)), "val": [float(i) for i in range(100)]})
        cursor = user_conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS byoc_test_write")
        cursor.execute("CREATE TABLE byoc_test_write (id BIGINT, val DOUBLE PRECISION)")

        try:
            result = engine.write_bulk(user_conn, table, "byoc_test_write")
            assert isinstance(result, WriteResult)
            assert result.rows == 100

            cursor.execute("SELECT COUNT(*) FROM byoc_test_write")
            assert cursor.fetchone()[0] == 100
        finally:
            cursor.execute("DROP TABLE IF EXISTS byoc_test_write")

    def test_write_dataframe(self, user_conn, engine):
        import pandas as pd
        df = pd.DataFrame({"id": list(range(50)), "val": [float(i) * 1.5 for i in range(50)]})
        cursor = user_conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS byoc_test_df")
        cursor.execute("CREATE TABLE byoc_test_df (id BIGINT, val DOUBLE PRECISION)")

        try:
            result = engine.write_dataframe(user_conn, df, "byoc_test_df")
            assert result.rows == 50

            cursor.execute("SELECT COUNT(*) FROM byoc_test_df")
            assert cursor.fetchone()[0] == 50
        finally:
            cursor.execute("DROP TABLE IF EXISTS byoc_test_df")


class TestEngineRepr:
    def test_repr(self, engine):
        r = repr(engine)
        assert "Engine(" in r
        assert os.environ["STAGING_BUCKET"] in r


class TestEngineVsConnect:
    """Verify Engine and arrowjet.connect() produce equivalent results."""

    def test_same_row_count(self, user_conn, engine):
        """Engine and connect() should return the same data for the same query."""
        _setup_table(user_conn, "byoc_test_equiv", 500)
        try:
            # BYOC path
            byoc_result = engine.read_bulk(user_conn, "SELECT * FROM byoc_test_equiv")

            # Unified connection path
            bf_conn = arrowjet.connect(
                host=os.environ["REDSHIFT_HOST"],
                database=os.environ.get("REDSHIFT_DATABASE", "dev"),
                user=os.environ.get("REDSHIFT_USER", "awsuser"),
                password=os.environ["REDSHIFT_PASS"],
                staging_bucket=os.environ["STAGING_BUCKET"],
                staging_iam_role=os.environ["STAGING_IAM_ROLE"],
                staging_region=os.environ.get("STAGING_REGION", "us-east-1"),
            )
            try:
                connect_result = bf_conn.read_bulk("SELECT * FROM byoc_test_equiv")
            finally:
                bf_conn.close()

            assert byoc_result.rows == connect_result.rows
        finally:
            _drop_table(user_conn, "byoc_test_equiv")
