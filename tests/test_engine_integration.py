"""
Integration tests for the BYOC Engine  - requires real Redshift + S3.

Tests that Engine works with a user-provided redshift_connector connection,
validating the full UNLOAD -> S3 -> Parquet -> Arrow pipeline.

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
    """A plain redshift_connector connection  - what a user would bring."""
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
    """A Arrowjet Engine with staging config  - no connection details."""
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


# --- PostgreSQL Engine Integration Tests ---

import time

PG_HOST = os.environ.get("PG_HOST")
PG_PASS = os.environ.get("PG_PASS")


@pytest.mark.skipif(
    not PG_HOST or not PG_PASS,
    reason="PG_HOST and PG_PASS not set",
)
class TestPostgreSQLEngineReadBulk:
    """Test Engine(provider='postgresql') read operations against real RDS PostgreSQL."""

    @pytest.fixture
    def pg_conn(self):
        import psycopg2
        conn = psycopg2.connect(
            host=PG_HOST,
            port=int(os.environ.get("PG_PORT", "5432")),
            dbname=os.environ.get("PG_DATABASE", "dev"),
            user=os.environ.get("PG_USER", "awsuser"),
            password=PG_PASS,
            connect_timeout=10,
        )
        yield conn
        conn.close()

    @pytest.fixture
    def pg_engine(self):
        return Engine(provider="postgresql")

    @pytest.fixture
    def populated_table(self, pg_conn):
        table_name = f"engine_pg_read_{int(time.time())}"
        cursor = pg_conn.cursor()
        cursor.execute(f"CREATE TABLE {table_name} (id BIGINT, value DOUBLE PRECISION)")
        for i in range(0, 500, 100):
            values = ",".join(f"({j}, {j * 1.5})" for j in range(i, min(i + 100, 500)))
            cursor.execute(f"INSERT INTO {table_name} VALUES {values}")
        pg_conn.commit()
        yield table_name
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        pg_conn.commit()

    def test_read_bulk_returns_result(self, pg_conn, pg_engine, populated_table):
        result = pg_engine.read_bulk(pg_conn, f"SELECT * FROM {populated_table}")
        assert result.rows == 500
        assert result.table.num_columns == 2

    def test_read_bulk_to_pandas(self, pg_conn, pg_engine, populated_table):
        result = pg_engine.read_bulk(pg_conn, f"SELECT * FROM {populated_table}")
        df = result.to_pandas()
        assert len(df) == 500
        assert "id" in df.columns

    def test_read_with_where(self, pg_conn, pg_engine, populated_table):
        result = pg_engine.read_bulk(pg_conn, f"SELECT * FROM {populated_table} WHERE id < 10")
        assert result.rows == 10

    def test_engine_does_not_close_user_conn(self, pg_conn, pg_engine, populated_table):
        pg_engine.read_bulk(pg_conn, f"SELECT * FROM {populated_table}")
        cursor = pg_conn.cursor()
        cursor.execute("SELECT 1")
        assert cursor.fetchone()[0] == 1


@pytest.mark.skipif(
    not PG_HOST or not PG_PASS,
    reason="PG_HOST and PG_PASS not set",
)
class TestPostgreSQLEngineWriteBulk:
    """Test Engine(provider='postgresql') write operations against real RDS PostgreSQL."""

    @pytest.fixture
    def pg_conn(self):
        import psycopg2
        conn = psycopg2.connect(
            host=PG_HOST,
            port=int(os.environ.get("PG_PORT", "5432")),
            dbname=os.environ.get("PG_DATABASE", "dev"),
            user=os.environ.get("PG_USER", "awsuser"),
            password=PG_PASS,
            connect_timeout=10,
        )
        yield conn
        conn.close()

    @pytest.fixture
    def pg_engine(self):
        return Engine(provider="postgresql")

    @pytest.fixture
    def test_table(self, pg_conn):
        table_name = f"engine_pg_write_{int(time.time())}"
        cursor = pg_conn.cursor()
        cursor.execute(f"CREATE TABLE {table_name} (id BIGINT, val DOUBLE PRECISION)")
        pg_conn.commit()
        yield table_name
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        pg_conn.commit()

    def test_write_bulk_arrow_table(self, pg_conn, pg_engine, test_table):
        table = pa.table({"id": list(range(100)), "val": [float(i) for i in range(100)]})
        result = pg_engine.write_bulk(pg_conn, table, test_table)
        assert result.rows == 100

        cursor = pg_conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {test_table}")
        assert cursor.fetchone()[0] == 100

    def test_write_dataframe(self, pg_conn, pg_engine, test_table):
        import pandas as pd_
        df = pd_.DataFrame({"id": list(range(50)), "val": [float(i) for i in range(50)]})
        result = pg_engine.write_dataframe(pg_conn, df, test_table)
        assert result.rows == 50

    def test_roundtrip(self, pg_conn, pg_engine, test_table):
        """Write then read back  - verify data integrity."""
        table = pa.table({"id": pa.array([10, 20, 30], type=pa.int64()),
                          "val": pa.array([1.1, 2.2, 3.3], type=pa.float64())})
        pg_engine.write_bulk(pg_conn, table, test_table)

        result = pg_engine.read_bulk(pg_conn, f"SELECT * FROM {test_table} ORDER BY id")
        df = result.to_pandas()
        assert list(df["id"]) == [10, 20, 30]


@pytest.mark.skipif(
    not PG_HOST or not PG_PASS,
    reason="PG_HOST and PG_PASS not set",
)
class TestPostgreSQLEngineRepr:
    def test_repr(self):
        engine = Engine(provider="postgresql")
        assert "postgresql" in repr(engine)
        assert "Engine(" in repr(engine)


# --- Lifecycle hooks integration tests ---

@pytest.mark.skipif(
    not PG_HOST or not PG_PASS,
    reason="PG_HOST and PG_PASS not set",
)
class TestHooksIntegration:
    """Test lifecycle hooks fire correctly against real databases."""

    @pytest.fixture
    def pg_conn(self):
        import psycopg2
        conn = psycopg2.connect(
            host=PG_HOST, port=int(os.environ.get("PG_PORT", "5432")),
            dbname=os.environ.get("PG_DATABASE", "dev"),
            user=os.environ.get("PG_USER", "awsuser"),
            password=PG_PASS, connect_timeout=10,
        )
        yield conn
        conn.close()

    @pytest.fixture
    def test_table(self, pg_conn):
        name = f"hook_test_{int(time.time())}"
        cursor = pg_conn.cursor()
        cursor.execute(f"CREATE TABLE {name} (id BIGINT, val DOUBLE PRECISION)")
        pg_conn.commit()
        yield name
        cursor.execute(f"DROP TABLE IF EXISTS {name}")
        pg_conn.commit()

    def test_write_hook_fires_with_real_db(self, pg_conn, test_table):
        engine = Engine(provider="postgresql")
        captured = []
        engine.on("on_write_complete", lambda eng, res: captured.append(res.rows))

        table = pa.table({"id": pa.array(range(100), type=pa.int64()),
                          "val": pa.array([float(i) for i in range(100)], type=pa.float64())})
        engine.write_bulk(pg_conn, table, test_table)

        assert len(captured) == 1
        assert captured[0] == 100

    def test_read_hook_fires_with_real_db(self, pg_conn, test_table):
        engine = Engine(provider="postgresql")

        # Write some data first
        table = pa.table({"id": pa.array(range(50), type=pa.int64()),
                          "val": pa.array([float(i) for i in range(50)], type=pa.float64())})
        engine.write_bulk(pg_conn, table, test_table)

        # Now read with hook
        captured = []
        engine.on("on_read_complete", lambda eng, res: captured.append(res.rows))
        engine.read_bulk(pg_conn, f"SELECT * FROM {test_table}")

        assert len(captured) == 1
        assert captured[0] == 50

    def test_hook_can_inspect_result(self, pg_conn, test_table):
        """Hook receives the full result object for inspection."""
        engine = Engine(provider="postgresql")

        table = pa.table({"id": pa.array([1, 2, 3], type=pa.int64()),
                          "val": pa.array([10.0, 20.0, 30.0], type=pa.float64())})
        engine.write_bulk(pg_conn, table, test_table)

        results = []
        engine.on("on_read_complete", lambda eng, res: results.append(res))
        engine.read_bulk(pg_conn, f"SELECT * FROM {test_table}")

        assert results[0].rows == 3
        assert results[0].table.num_columns == 2

    def test_hook_error_does_not_break_real_write(self, pg_conn, test_table):
        engine = Engine(provider="postgresql")

        def bad_hook(eng, res):
            raise RuntimeError("hook exploded")

        engine.on("on_write_complete", bad_hook)

        table = pa.table({"id": pa.array([1], type=pa.int64()),
                          "val": pa.array([1.0], type=pa.float64())})
        result = engine.write_bulk(pg_conn, table, test_table)
        assert result.rows == 1

        # Data should still be in the table
        cursor = pg_conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {test_table}")
        assert cursor.fetchone()[0] == 1
