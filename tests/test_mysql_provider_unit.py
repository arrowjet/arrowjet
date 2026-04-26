"""
Unit tests for MySQLProvider  - LOAD DATA LOCAL INFILE bulk operations.

Tests the provider logic without requiring a real MySQL connection.
Uses mock connections to verify SQL generation and data flow.
"""

import io
import os
import pytest
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pandas as pd

from arrowjet.providers.mysql import (
    MySQLProvider,
    MySQLWriteResult,
    MySQLReadResult,
    MySQLBulkError,
    _arrow_to_csv,
)
from arrowjet.engine import Engine


# --- Fixtures ---

@pytest.fixture
def provider():
    return MySQLProvider()


@pytest.fixture
def sample_table():
    return pa.table({
        "id": pa.array([1, 2, 3], type=pa.int64()),
        "name": pa.array(["alice", "bob", "charlie"], type=pa.utf8()),
        "score": pa.array([95.5, 87.3, 92.1], type=pa.float64()),
    })


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["alice", "bob", "charlie"],
        "score": [95.5, 87.3, 92.1],
    })


@pytest.fixture
def mock_conn():
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn


# --- Provider properties ---

class TestProviderProperties:
    def test_name(self, provider):
        assert provider.name == "mysql"

    def test_uses_cloud_staging(self, provider):
        assert provider.uses_cloud_staging is False


# --- CSV conversion ---

class TestArrowToCsv:
    def test_basic_conversion(self, sample_table):
        buf = io.BytesIO()
        _arrow_to_csv(sample_table, buf)
        buf.seek(0)
        content = buf.read().decode("utf-8")
        lines = content.strip().split("\n")
        assert len(lines) == 3  # no header
        assert "alice" in lines[0]

    def test_empty_table(self):
        table = pa.table({"id": pa.array([], type=pa.int64())})
        buf = io.BytesIO()
        _arrow_to_csv(table, buf)
        buf.seek(0)
        assert buf.read().decode("utf-8").strip() == ""

    def test_special_characters(self):
        table = pa.table({
            "name": pa.array(['hello, world', 'say "hi"', "normal"], type=pa.utf8()),
        })
        buf = io.BytesIO()
        _arrow_to_csv(table, buf)
        buf.seek(0)
        content = buf.read().decode("utf-8")
        assert "hello" in content


# --- Write bulk ---

class TestWriteBulk:
    def test_write_executes_load_data(self, provider, sample_table, mock_conn):
        result = provider.write_bulk(mock_conn, sample_table, "test_table")

        cursor = mock_conn.cursor.return_value
        assert cursor.execute.called
        sql = cursor.execute.call_args[0][0]
        assert "LOAD DATA LOCAL INFILE" in sql
        assert "test_table" in sql
        assert mock_conn.commit.called

    def test_write_result_fields(self, provider, sample_table, mock_conn):
        result = provider.write_bulk(mock_conn, sample_table, "my_table")

        assert isinstance(result, MySQLWriteResult)
        assert result.rows == 3
        assert result.target_table == "my_table"
        assert result.bytes_transferred > 0
        assert result.load_time_s >= 0
        assert result.total_time_s >= 0

    def test_write_result_repr(self, provider, sample_table, mock_conn):
        result = provider.write_bulk(mock_conn, sample_table, "test_table")
        assert "MySQLWriteResult" in repr(result)
        assert "rows=3" in repr(result)

    def test_write_dataframe(self, provider, sample_df, mock_conn):
        result = provider.write_dataframe(mock_conn, sample_df, "test_table")
        assert result.rows == 3

    def test_write_includes_column_names(self, provider, sample_table, mock_conn):
        provider.write_bulk(mock_conn, sample_table, "test_table")
        sql = mock_conn.cursor.return_value.execute.call_args[0][0]
        assert "id" in sql
        assert "name" in sql
        assert "score" in sql

    def test_write_error_raises(self, provider, sample_table):
        conn = MagicMock()
        conn.cursor.return_value.execute.side_effect = Exception("access denied")
        with pytest.raises(MySQLBulkError, match="LOAD DATA failed"):
            provider.write_bulk(conn, sample_table, "test_table")

    def test_write_cleans_up_temp_file(self, provider, sample_table, mock_conn):
        """Temp CSV file should be deleted after LOAD DATA."""
        result = provider.write_bulk(mock_conn, sample_table, "test_table")
        # The temp file path was in the SQL, extract it
        sql = mock_conn.cursor.return_value.execute.call_args[0][0]
        # File should have been cleaned up
        import re
        match = re.search(r"INFILE '([^']+)'", sql)
        if match:
            assert not os.path.exists(match.group(1))


# --- Read bulk ---

class TestReadBulk:
    def test_read_returns_arrow_table(self, provider, mock_conn):
        cursor = mock_conn.cursor.return_value
        cursor.description = [("id",), ("name",)]
        cursor.fetchall.return_value = [(1, "alice"), (2, "bob"), (3, "charlie")]

        result = provider.read_bulk(mock_conn, "SELECT * FROM users")

        assert isinstance(result, MySQLReadResult)
        assert result.rows == 3
        assert result.table.num_columns == 2

    def test_read_to_pandas(self, provider, mock_conn):
        cursor = mock_conn.cursor.return_value
        cursor.description = [("id",), ("name",)]
        cursor.fetchall.return_value = [(1, "alice"), (2, "bob")]

        result = provider.read_bulk(mock_conn, "SELECT * FROM users")
        df = result.to_pandas()
        assert len(df) == 2
        assert "id" in df.columns

    def test_read_empty_result(self, provider, mock_conn):
        cursor = mock_conn.cursor.return_value
        cursor.description = [("id",)]
        cursor.fetchall.return_value = []

        result = provider.read_bulk(mock_conn, "SELECT * FROM empty")
        assert result.rows == 0

    def test_read_no_description(self, provider, mock_conn):
        """Queries that return no result set."""
        cursor = mock_conn.cursor.return_value
        cursor.description = None

        result = provider.read_bulk(mock_conn, "SELECT 1 WHERE 1=0")
        assert result.rows == 0

    def test_read_result_repr(self, provider, mock_conn):
        cursor = mock_conn.cursor.return_value
        cursor.description = [("id",)]
        cursor.fetchall.return_value = [(1,), (2,)]

        result = provider.read_bulk(mock_conn, "SELECT id FROM t")
        assert "MySQLReadResult" in repr(result)
        assert "rows=2" in repr(result)

    def test_read_timing_fields(self, provider, mock_conn):
        cursor = mock_conn.cursor.return_value
        cursor.description = [("id",)]
        cursor.fetchall.return_value = [(1,)]

        result = provider.read_bulk(mock_conn, "SELECT 1")
        assert result.query_time_s >= 0
        assert result.fetch_time_s >= 0
        assert result.total_time_s >= 0


# --- Engine integration ---

class TestMySQLEngine:
    def test_engine_creation(self):
        engine = Engine(provider="mysql")
        assert engine.provider == "mysql"
        assert "mysql" in repr(engine)

    def test_engine_read_bulk(self, mock_conn):
        cursor = mock_conn.cursor.return_value
        cursor.description = [("id",)]
        cursor.fetchall.return_value = [(1,), (2,)]

        engine = Engine(provider="mysql")
        result = engine.read_bulk(mock_conn, "SELECT id FROM t")
        assert result.rows == 2

    def test_engine_write_bulk(self, mock_conn):
        table = pa.table({"id": pa.array([1, 2], type=pa.int64())})

        engine = Engine(provider="mysql")
        result = engine.write_bulk(mock_conn, table, "test_table")
        assert result.rows == 2

    def test_engine_write_dataframe(self, mock_conn):
        df = pd.DataFrame({"id": [1, 2, 3]})

        engine = Engine(provider="mysql")
        result = engine.write_dataframe(mock_conn, df, "test_table")
        assert result.rows == 3
