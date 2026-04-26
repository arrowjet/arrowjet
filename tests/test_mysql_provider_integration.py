"""
Integration tests for MySQLProvider against a real MySQL instance.

Requires environment variables:
    MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE, MYSQL_USER, MYSQL_PASS

Run:
    set -a && source .env && set +a
    PYTHONPATH=src python -m pytest tests/test_mysql_provider_integration.py -v
"""

import os
import time

import pytest
import pyarrow as pa
import pandas as pd

from arrowjet.providers.mysql import MySQLProvider, MySQLWriteResult, MySQLReadResult
from arrowjet.engine import Engine

# --- Skip if no MySQL credentials ---

MYSQL_HOST = os.environ.get("MYSQL_HOST")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3306"))
MYSQL_DATABASE = os.environ.get("MYSQL_DATABASE", "dev")
MYSQL_USER = os.environ.get("MYSQL_USER", "awsuser")
MYSQL_PASS = os.environ.get("MYSQL_PASS")

SKIP_REASON = "MYSQL_HOST and MYSQL_PASS required for MySQL integration tests"
requires_mysql = pytest.mark.skipif(not MYSQL_HOST or not MYSQL_PASS, reason=SKIP_REASON)


def get_mysql_conn():
    """Create a pymysql connection to the test MySQL instance."""
    import pymysql
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        database=MYSQL_DATABASE,
        user=MYSQL_USER,
        password=MYSQL_PASS,
        connect_timeout=10,
        local_infile=True,  # Required for LOAD DATA LOCAL INFILE
    )


# --- Fixtures ---

@pytest.fixture
def mysql_conn():
    conn = get_mysql_conn()
    yield conn
    conn.close()


@pytest.fixture
def provider():
    return MySQLProvider()


@pytest.fixture
def engine():
    return Engine(provider="mysql")


@pytest.fixture
def test_table(mysql_conn):
    """Create a test table and clean up after."""
    table_name = f"test_bulk_{int(time.time())}"
    cursor = mysql_conn.cursor()
    cursor.execute(f"""
        CREATE TABLE {table_name} (
            id BIGINT,
            name VARCHAR(100),
            score DOUBLE
        )
    """)
    mysql_conn.commit()
    yield table_name
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    mysql_conn.commit()


@pytest.fixture
def populated_table(mysql_conn):
    """Create and populate a test table for read tests."""
    table_name = f"test_read_{int(time.time())}"
    cursor = mysql_conn.cursor()
    cursor.execute(f"""
        CREATE TABLE {table_name} (
            id BIGINT,
            name VARCHAR(100),
            score DOUBLE
        )
    """)
    for i in range(0, 1000, 100):
        values = ",".join(
            f"({j}, 'user_{j}', {j * 1.5})" for j in range(i, min(i + 100, 1000))
        )
        cursor.execute(f"INSERT INTO {table_name} VALUES {values}")
    mysql_conn.commit()
    yield table_name
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    mysql_conn.commit()


# --- Connection test ---

@requires_mysql
class TestConnection:
    def test_basic_connectivity(self, mysql_conn):
        cursor = mysql_conn.cursor()
        cursor.execute("SELECT 1")
        assert cursor.fetchone()[0] == 1

    def test_version(self, mysql_conn):
        cursor = mysql_conn.cursor()
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()[0]
        assert "8." in version


# --- Write tests ---

@requires_mysql
class TestWriteBulk:
    def test_write_and_verify_row_count(self, provider, mysql_conn, test_table):
        table = pa.table({
            "id": pa.array(range(100), type=pa.int64()),
            "name": pa.array([f"user_{i}" for i in range(100)], type=pa.utf8()),
            "score": pa.array([i * 1.1 for i in range(100)], type=pa.float64()),
        })

        result = provider.write_bulk(mysql_conn, table, test_table)

        assert isinstance(result, MySQLWriteResult)
        assert result.rows == 100

        cursor = mysql_conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {test_table}")
        assert cursor.fetchone()[0] == 100

    def test_write_dataframe(self, provider, mysql_conn, test_table):
        df = pd.DataFrame({
            "id": range(50),
            "name": [f"user_{i}" for i in range(50)],
            "score": [i * 2.0 for i in range(50)],
        })

        result = provider.write_dataframe(mysql_conn, df, test_table)
        assert result.rows == 50

        cursor = mysql_conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {test_table}")
        assert cursor.fetchone()[0] == 50

    def test_write_data_integrity(self, provider, mysql_conn, test_table):
        table = pa.table({
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "name": pa.array(["alice", "bob", "charlie"], type=pa.utf8()),
            "score": pa.array([95.5, 87.3, 92.1], type=pa.float64()),
        })

        provider.write_bulk(mysql_conn, table, test_table)

        cursor = mysql_conn.cursor()
        cursor.execute(f"SELECT id, name, score FROM {test_table} ORDER BY id")
        rows = cursor.fetchall()

        assert rows[0] == (1, "alice", 95.5)
        assert rows[1] == (2, "bob", 87.3)
        assert rows[2] == (3, "charlie", 92.1)

    def test_write_10k_rows(self, provider, mysql_conn, test_table):
        n = 10_000
        table = pa.table({
            "id": pa.array(range(n), type=pa.int64()),
            "name": pa.array([f"user_{i}" for i in range(n)], type=pa.utf8()),
            "score": pa.array([i * 0.1 for i in range(n)], type=pa.float64()),
        })

        result = provider.write_bulk(mysql_conn, table, test_table)

        assert result.rows == n
        assert result.total_time_s > 0

        cursor = mysql_conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {test_table}")
        assert cursor.fetchone()[0] == n

    def test_write_timing_fields(self, provider, mysql_conn, test_table):
        table = pa.table({
            "id": pa.array([1], type=pa.int64()),
            "name": pa.array(["test"], type=pa.utf8()),
            "score": pa.array([1.0], type=pa.float64()),
        })

        result = provider.write_bulk(mysql_conn, table, test_table)
        assert result.load_time_s >= 0
        assert result.total_time_s >= result.load_time_s
        assert result.target_table == test_table


# --- Read tests ---

@requires_mysql
class TestReadBulk:
    def test_read_and_verify_row_count(self, provider, mysql_conn, populated_table):
        result = provider.read_bulk(mysql_conn, f"SELECT * FROM {populated_table}")
        assert isinstance(result, MySQLReadResult)
        assert result.rows == 1000

    def test_read_to_pandas(self, provider, mysql_conn, populated_table):
        result = provider.read_bulk(mysql_conn, f"SELECT * FROM {populated_table}")
        df = result.to_pandas()
        assert len(df) == 1000
        assert "id" in df.columns

    def test_read_with_where_clause(self, provider, mysql_conn, populated_table):
        result = provider.read_bulk(
            mysql_conn, f"SELECT * FROM {populated_table} WHERE id < 10"
        )
        assert result.rows == 10

    def test_read_with_aggregation(self, provider, mysql_conn, populated_table):
        result = provider.read_bulk(
            mysql_conn, f"SELECT COUNT(*) as cnt FROM {populated_table}"
        )
        df = result.to_pandas()
        assert df["cnt"].iloc[0] == 1000

    def test_read_timing_fields(self, provider, mysql_conn, populated_table):
        result = provider.read_bulk(mysql_conn, f"SELECT * FROM {populated_table}")
        assert result.query_time_s >= 0
        assert result.fetch_time_s >= 0
        assert result.total_time_s >= 0


# --- Engine integration ---

@requires_mysql
class TestMySQLEngine:
    def test_engine_write_and_read(self, engine, mysql_conn, test_table):
        df = pd.DataFrame({
            "id": range(200),
            "name": [f"user_{i}" for i in range(200)],
            "score": [i * 0.5 for i in range(200)],
        })

        write_result = engine.write_dataframe(mysql_conn, df, test_table)
        assert write_result.rows == 200

        read_result = engine.read_bulk(mysql_conn, f"SELECT * FROM {test_table}")
        assert read_result.rows == 200

    def test_engine_write_arrow_table(self, engine, mysql_conn, test_table):
        table = pa.table({
            "id": pa.array([10, 20, 30], type=pa.int64()),
            "name": pa.array(["x", "y", "z"], type=pa.utf8()),
            "score": pa.array([1.0, 2.0, 3.0], type=pa.float64()),
        })

        result = engine.write_bulk(mysql_conn, table, test_table)
        assert result.rows == 3

    def test_engine_no_s3_required(self):
        engine = Engine(provider="mysql")
        assert "mysql" in repr(engine)
