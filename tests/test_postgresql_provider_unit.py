"""
Unit tests for PostgreSQLProvider — COPY protocol bulk operations.

Tests the provider logic without requiring a real PostgreSQL connection.
Uses mock connections to verify COPY SQL generation and data flow.
"""

import io
import pytest
from unittest.mock import MagicMock, patch, call

import pyarrow as pa
import pandas as pd

from arrowjet.providers.postgresql import (
    PostgreSQLProvider,
    PgWriteResult,
    PgReadResult,
    PgBulkError,
    _arrow_to_csv_copy,
)
from arrowjet.engine import PostgreSQLEngine


# --- Fixtures ---

@pytest.fixture
def provider():
    return PostgreSQLProvider()


@pytest.fixture
def sample_table():
    """Small Arrow table for testing."""
    return pa.table({
        "id": pa.array([1, 2, 3], type=pa.int64()),
        "name": pa.array(["alice", "bob", "charlie"], type=pa.utf8()),
        "score": pa.array([95.5, 87.3, 92.1], type=pa.float64()),
    })


@pytest.fixture
def sample_df():
    """Small pandas DataFrame for testing."""
    return pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["alice", "bob", "charlie"],
        "score": [95.5, 87.3, 92.1],
    })


@pytest.fixture
def mock_conn():
    """Mock psycopg2-like connection with copy_expert support."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn


# --- Provider properties ---

class TestProviderProperties:
    def test_name(self, provider):
        assert provider.name == "postgresql"

    def test_uses_cloud_staging(self, provider):
        assert provider.uses_cloud_staging is False


# --- CSV conversion ---

class TestArrowToCsvCopy:
    def test_basic_conversion(self, sample_table):
        buf = io.BytesIO()
        _arrow_to_csv_copy(sample_table, buf)
        buf.seek(0)
        content = buf.read().decode("utf-8")

        # Should NOT have a header row (COPY FROM STDIN expects data only)
        lines = content.strip().split("\n")
        assert len(lines) == 3  # 3 data rows, no header

        # First row should be: 1,alice,95.5
        assert "1" in lines[0]
        assert "alice" in lines[0]

    def test_empty_table(self):
        table = pa.table({"id": pa.array([], type=pa.int64())})
        buf = io.BytesIO()
        _arrow_to_csv_copy(table, buf)
        buf.seek(0)
        content = buf.read().decode("utf-8")
        assert content.strip() == ""

    def test_null_values(self):
        table = pa.table({
            "id": pa.array([1, 2, None], type=pa.int64()),
            "name": pa.array(["alice", None, "charlie"], type=pa.utf8()),
        })
        buf = io.BytesIO()
        _arrow_to_csv_copy(table, buf)
        buf.seek(0)
        content = buf.read().decode("utf-8")
        lines = content.strip().split("\n")
        assert len(lines) == 3

    def test_special_characters(self):
        """CSV should handle commas and quotes in values."""
        table = pa.table({
            "name": pa.array(['hello, world', 'say "hi"', "normal"], type=pa.utf8()),
        })
        buf = io.BytesIO()
        _arrow_to_csv_copy(table, buf)
        buf.seek(0)
        content = buf.read().decode("utf-8")
        # PyArrow CSV writer should quote fields with commas/quotes
        assert "hello" in content
        assert "normal" in content


# --- Write bulk ---

class TestWriteBulk:
    def test_write_calls_copy_expert(self, provider, sample_table, mock_conn):
        result = provider.write_bulk(mock_conn, sample_table, "test_table")

        # Should have called cursor.copy_expert with COPY FROM STDIN
        cursor = mock_conn.cursor.return_value
        assert cursor.copy_expert.called
        copy_sql = cursor.copy_expert.call_args[0][0]
        assert "COPY test_table FROM STDIN" in copy_sql
        assert "FORMAT csv" in copy_sql

        # Should have committed
        assert mock_conn.commit.called

    def test_write_result_fields(self, provider, sample_table, mock_conn):
        result = provider.write_bulk(mock_conn, sample_table, "my_schema.my_table")

        assert isinstance(result, PgWriteResult)
        assert result.rows == 3
        assert result.target_table == "my_schema.my_table"
        assert result.bytes_transferred > 0
        assert result.copy_time_s >= 0
        assert result.total_time_s >= 0

    def test_write_result_repr(self, provider, sample_table, mock_conn):
        result = provider.write_bulk(mock_conn, sample_table, "test_table")
        repr_str = repr(result)
        assert "PgWriteResult" in repr_str
        assert "rows=3" in repr_str
        assert "test_table" in repr_str

    def test_write_dataframe(self, provider, sample_df, mock_conn):
        result = provider.write_dataframe(mock_conn, sample_df, "test_table")
        assert result.rows == 3

    def test_write_no_copy_expert_raises(self, provider, sample_table):
        """Connection without copy_expert should raise PgBulkError."""
        conn = MagicMock()
        cursor = MagicMock()
        cursor.copy_expert.side_effect = AttributeError("no copy_expert")
        conn.cursor.return_value = cursor

        with pytest.raises(PgBulkError, match="copy_expert"):
            provider.write_bulk(conn, sample_table, "test_table")

    def test_write_schema_qualified_table(self, provider, sample_table, mock_conn):
        result = provider.write_bulk(mock_conn, sample_table, "public.users")
        cursor = mock_conn.cursor.return_value
        copy_sql = cursor.copy_expert.call_args[0][0]
        assert "COPY public.users FROM STDIN" in copy_sql


# --- Read bulk ---

class TestReadBulk:
    def test_read_calls_copy_expert(self, provider, mock_conn):
        # Mock copy_expert to write CSV data to the buffer
        def fake_copy_out(sql, buf):
            buf.write(b"id,name,score\n1,alice,95.5\n2,bob,87.3\n")

        cursor = mock_conn.cursor.return_value
        cursor.copy_expert.side_effect = fake_copy_out

        result = provider.read_bulk(mock_conn, "SELECT * FROM users")

        assert cursor.copy_expert.called
        copy_sql = cursor.copy_expert.call_args[0][0]
        assert "COPY (SELECT * FROM users) TO STDOUT" in copy_sql
        assert "FORMAT csv" in copy_sql
        assert "HEADER true" in copy_sql

    def test_read_result_fields(self, provider, mock_conn):
        def fake_copy_out(sql, buf):
            buf.write(b"id,name\n1,alice\n2,bob\n3,charlie\n")

        cursor = mock_conn.cursor.return_value
        cursor.copy_expert.side_effect = fake_copy_out

        result = provider.read_bulk(mock_conn, "SELECT * FROM users")

        assert isinstance(result, PgReadResult)
        assert result.rows == 3
        assert result.bytes_transferred > 0
        assert result.copy_time_s >= 0
        assert result.parse_time_s >= 0
        assert result.total_time_s >= 0

    def test_read_to_pandas(self, provider, mock_conn):
        def fake_copy_out(sql, buf):
            buf.write(b"id,name\n1,alice\n2,bob\n")

        cursor = mock_conn.cursor.return_value
        cursor.copy_expert.side_effect = fake_copy_out

        result = provider.read_bulk(mock_conn, "SELECT * FROM users")
        df = result.to_pandas()

        assert len(df) == 2
        assert "id" in df.columns
        assert "name" in df.columns

    def test_read_empty_result(self, provider, mock_conn):
        def fake_copy_out(sql, buf):
            pass  # No data written

        cursor = mock_conn.cursor.return_value
        cursor.copy_expert.side_effect = fake_copy_out

        result = provider.read_bulk(mock_conn, "SELECT * FROM empty_table")
        assert result.rows == 0

    def test_read_result_repr(self, provider, mock_conn):
        def fake_copy_out(sql, buf):
            buf.write(b"id\n1\n2\n")

        cursor = mock_conn.cursor.return_value
        cursor.copy_expert.side_effect = fake_copy_out

        result = provider.read_bulk(mock_conn, "SELECT id FROM t")
        repr_str = repr(result)
        assert "PgReadResult" in repr_str
        assert "rows=2" in repr_str

    def test_read_no_copy_expert_raises(self, provider):
        conn = MagicMock()
        cursor = MagicMock()
        cursor.copy_expert.side_effect = AttributeError("no copy_expert")
        conn.cursor.return_value = cursor

        with pytest.raises(PgBulkError, match="copy_expert"):
            provider.read_bulk(conn, "SELECT 1")

    def test_read_complex_query(self, provider, mock_conn):
        """COPY should wrap complex queries correctly."""
        def fake_copy_out(sql, buf):
            buf.write(b"cnt\n42\n")

        cursor = mock_conn.cursor.return_value
        cursor.copy_expert.side_effect = fake_copy_out

        query = "SELECT COUNT(*) as cnt FROM users WHERE active = true"
        result = provider.read_bulk(mock_conn, query)

        copy_sql = cursor.copy_expert.call_args[0][0]
        assert f"COPY ({query}) TO STDOUT" in copy_sql


# --- PostgreSQLEngine ---

class TestPostgreSQLEngine:
    def test_engine_creation(self):
        engine = PostgreSQLEngine()
        assert repr(engine) == "Engine(provider=postgresql)"

    def test_engine_read_bulk(self, mock_conn):
        def fake_copy_out(sql, buf):
            buf.write(b"id\n1\n2\n")

        cursor = mock_conn.cursor.return_value
        cursor.copy_expert.side_effect = fake_copy_out

        engine = PostgreSQLEngine()
        result = engine.read_bulk(mock_conn, "SELECT id FROM t")
        assert result.rows == 2

    def test_engine_write_bulk(self, mock_conn):
        table = pa.table({"id": pa.array([1, 2], type=pa.int64())})

        engine = PostgreSQLEngine()
        result = engine.write_bulk(mock_conn, table, "test_table")
        assert result.rows == 2

    def test_engine_write_dataframe(self, mock_conn):
        df = pd.DataFrame({"id": [1, 2, 3]})

        engine = PostgreSQLEngine()
        result = engine.write_dataframe(mock_conn, df, "test_table")
        assert result.rows == 3

    def test_engine_importable_from_arrowjet(self):
        """PostgreSQLEngine should be importable from the top-level package."""
        import arrowjet
        assert hasattr(arrowjet, "PostgreSQLEngine")
        engine = arrowjet.PostgreSQLEngine()
        assert engine is not None
