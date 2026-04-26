"""
Integration tests for PostgreSQLProvider against a real PostgreSQL instance.

Requires environment variables:
    PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASS

Run:
    set -a && source .env && set +a
    PYTHONPATH=src python -m pytest tests/test_postgresql_provider_integration.py -v
"""

import os
import time

import pytest
import pyarrow as pa
import pandas as pd

from arrowjet.providers.postgresql import PostgreSQLProvider, PgWriteResult, PgReadResult
from arrowjet.engine import PostgreSQLEngine

# --- Skip if no PostgreSQL credentials ---

PG_HOST = os.environ.get("PG_HOST")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_DATABASE = os.environ.get("PG_DATABASE", "dev")
PG_USER = os.environ.get("PG_USER", "awsuser")
PG_PASS = os.environ.get("PG_PASS")

SKIP_REASON = "PG_HOST and PG_PASS required for PostgreSQL integration tests"
requires_pg = pytest.mark.skipif(not PG_HOST or not PG_PASS, reason=SKIP_REASON)


def get_pg_conn():
    """Create a psycopg2 connection to the test PostgreSQL instance."""
    import psycopg2
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DATABASE,
        user=PG_USER,
        password=PG_PASS,
        connect_timeout=10,
    )


# --- Fixtures ---

@pytest.fixture
def pg_conn():
    conn = get_pg_conn()
    yield conn
    conn.close()


@pytest.fixture
def provider():
    return PostgreSQLProvider()


@pytest.fixture
def engine():
    return PostgreSQLEngine()


@pytest.fixture
def test_table(pg_conn):
    """Create a test table and clean up after."""
    table_name = f"test_bulk_{int(time.time())}"
    cursor = pg_conn.cursor()
    cursor.execute(f"""
        CREATE TABLE {table_name} (
            id BIGINT,
            name VARCHAR(100),
            score DOUBLE PRECISION,
            active BOOLEAN
        )
    """)
    pg_conn.commit()
    yield table_name
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    pg_conn.commit()


@pytest.fixture
def populated_table(pg_conn):
    """Create and populate a test table for read tests."""
    table_name = f"test_read_{int(time.time())}"
    cursor = pg_conn.cursor()
    cursor.execute(f"""
        CREATE TABLE {table_name} (
            id BIGINT,
            name VARCHAR(100),
            score DOUBLE PRECISION
        )
    """)
    # Insert 1000 rows using standard INSERT for setup
    for i in range(0, 1000, 100):
        values = ",".join(
            f"({j}, 'user_{j}', {j * 1.5})" for j in range(i, min(i + 100, 1000))
        )
        cursor.execute(f"INSERT INTO {table_name} VALUES {values}")
    pg_conn.commit()
    yield table_name
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    pg_conn.commit()


# --- Connection test ---

@requires_pg
class TestConnection:
    def test_basic_connectivity(self, pg_conn):
        cursor = pg_conn.cursor()
        cursor.execute("SELECT 1")
        assert cursor.fetchone()[0] == 1

    def test_version(self, pg_conn):
        cursor = pg_conn.cursor()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0]
        assert "PostgreSQL" in version


# --- Write tests ---

@requires_pg
class TestWriteBulk:
    def test_write_and_verify_row_count(self, provider, pg_conn, test_table):
        table = pa.table({
            "id": pa.array(range(100), type=pa.int64()),
            "name": pa.array([f"user_{i}" for i in range(100)], type=pa.utf8()),
            "score": pa.array([i * 1.1 for i in range(100)], type=pa.float64()),
            "active": pa.array([i % 2 == 0 for i in range(100)], type=pa.bool_()),
        })

        result = provider.write_bulk(pg_conn, table, test_table)

        assert isinstance(result, PgWriteResult)
        assert result.rows == 100

        # Verify in database
        cursor = pg_conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {test_table}")
        assert cursor.fetchone()[0] == 100

    def test_write_dataframe(self, provider, pg_conn, test_table):
        df = pd.DataFrame({
            "id": range(50),
            "name": [f"user_{i}" for i in range(50)],
            "score": [i * 2.0 for i in range(50)],
            "active": [True] * 50,
        })

        result = provider.write_dataframe(pg_conn, df, test_table)
        assert result.rows == 50

        cursor = pg_conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {test_table}")
        assert cursor.fetchone()[0] == 50

    def test_write_data_integrity(self, provider, pg_conn, test_table):
        """Verify written data matches the source."""
        table = pa.table({
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "name": pa.array(["alice", "bob", "charlie"], type=pa.utf8()),
            "score": pa.array([95.5, 87.3, 92.1], type=pa.float64()),
            "active": pa.array([True, False, True], type=pa.bool_()),
        })

        provider.write_bulk(pg_conn, table, test_table)

        cursor = pg_conn.cursor()
        cursor.execute(f"SELECT id, name, score, active FROM {test_table} ORDER BY id")
        rows = cursor.fetchall()

        assert rows[0] == (1, "alice", 95.5, True)
        assert rows[1] == (2, "bob", 87.3, False)
        assert rows[2] == (3, "charlie", 92.1, True)

    def test_write_10k_rows(self, provider, pg_conn, test_table):
        """Write 10K rows and verify count + timing."""
        n = 10_000
        table = pa.table({
            "id": pa.array(range(n), type=pa.int64()),
            "name": pa.array([f"user_{i}" for i in range(n)], type=pa.utf8()),
            "score": pa.array([i * 0.1 for i in range(n)], type=pa.float64()),
            "active": pa.array([i % 2 == 0 for i in range(n)], type=pa.bool_()),
        })

        result = provider.write_bulk(pg_conn, table, test_table)

        assert result.rows == n
        assert result.total_time_s > 0
        assert result.bytes_transferred > 0

        cursor = pg_conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {test_table}")
        assert cursor.fetchone()[0] == n

    def test_write_with_nulls(self, provider, pg_conn, test_table):
        """Verify NULL values are handled correctly."""
        table = pa.table({
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "name": pa.array(["alice", None, "charlie"], type=pa.utf8()),
            "score": pa.array([95.5, None, 92.1], type=pa.float64()),
            "active": pa.array([True, None, False], type=pa.bool_()),
        })

        provider.write_bulk(pg_conn, table, test_table)

        cursor = pg_conn.cursor()
        cursor.execute(f"SELECT * FROM {test_table} ORDER BY id")
        rows = cursor.fetchall()

        assert rows[0] == (1, "alice", 95.5, True)
        assert rows[1][0] == 2
        assert rows[1][1] is None  # name is NULL
        assert rows[1][2] is None  # score is NULL
        assert rows[2] == (3, "charlie", 92.1, False)

    def test_write_timing_fields(self, provider, pg_conn, test_table):
        table = pa.table({
            "id": pa.array([1], type=pa.int64()),
            "name": pa.array(["test"], type=pa.utf8()),
            "score": pa.array([1.0], type=pa.float64()),
            "active": pa.array([True], type=pa.bool_()),
        })

        result = provider.write_bulk(pg_conn, table, test_table)

        assert result.copy_time_s >= 0
        assert result.total_time_s >= result.copy_time_s
        assert result.target_table == test_table


# --- Read tests ---

@requires_pg
class TestReadBulk:
    def test_read_and_verify_row_count(self, provider, pg_conn, populated_table):
        result = provider.read_bulk(pg_conn, f"SELECT * FROM {populated_table}")

        assert isinstance(result, PgReadResult)
        assert result.rows == 1000

    def test_read_to_pandas(self, provider, pg_conn, populated_table):
        result = provider.read_bulk(pg_conn, f"SELECT * FROM {populated_table}")
        df = result.to_pandas()

        assert len(df) == 1000
        assert "id" in df.columns
        assert "name" in df.columns
        assert "score" in df.columns

    def test_read_with_where_clause(self, provider, pg_conn, populated_table):
        result = provider.read_bulk(
            pg_conn, f"SELECT * FROM {populated_table} WHERE id < 10"
        )
        assert result.rows == 10

    def test_read_with_aggregation(self, provider, pg_conn, populated_table):
        result = provider.read_bulk(
            pg_conn, f"SELECT COUNT(*) as cnt FROM {populated_table}"
        )
        df = result.to_pandas()
        assert df["cnt"].iloc[0] == 1000

    def test_read_empty_result(self, provider, pg_conn, populated_table):
        result = provider.read_bulk(
            pg_conn, f"SELECT * FROM {populated_table} WHERE id < 0"
        )
        # Empty result should have 0 rows (just header, no data)
        assert result.rows == 0

    def test_read_timing_fields(self, provider, pg_conn, populated_table):
        result = provider.read_bulk(pg_conn, f"SELECT * FROM {populated_table}")

        assert result.copy_time_s >= 0
        assert result.parse_time_s >= 0
        assert result.total_time_s >= 0
        assert result.bytes_transferred > 0


# --- Engine integration ---

@requires_pg
class TestPostgreSQLEngine:
    def test_engine_write_and_read(self, engine, pg_conn, test_table):
        """Full round-trip: write via engine, read back via engine."""
        df = pd.DataFrame({
            "id": range(200),
            "name": [f"user_{i}" for i in range(200)],
            "score": [i * 0.5 for i in range(200)],
            "active": [i % 3 == 0 for i in range(200)],
        })

        # Write
        write_result = engine.write_dataframe(pg_conn, df, test_table)
        assert write_result.rows == 200

        # Read back
        read_result = engine.read_bulk(pg_conn, f"SELECT * FROM {test_table}")
        assert read_result.rows == 200

        # Verify data matches
        result_df = read_result.to_pandas()
        assert len(result_df) == 200

    def test_engine_write_arrow_table(self, engine, pg_conn, test_table):
        table = pa.table({
            "id": pa.array([10, 20, 30], type=pa.int64()),
            "name": pa.array(["x", "y", "z"], type=pa.utf8()),
            "score": pa.array([1.0, 2.0, 3.0], type=pa.float64()),
            "active": pa.array([True, True, False], type=pa.bool_()),
        })

        result = engine.write_bulk(pg_conn, table, test_table)
        assert result.rows == 3

    def test_engine_no_s3_required(self):
        """PostgreSQLEngine should not require any S3/staging config."""
        engine = PostgreSQLEngine()
        assert "postgresql" in repr(engine)
