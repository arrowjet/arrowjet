"""Unit tests for chunked read_bulk iterator mode (10.17)."""

from unittest.mock import MagicMock
from arrowjet.engine import Engine


class TestChunkedReadPostgreSQL:

    def test_yields_batches(self):
        engine = Engine(provider="postgresql")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchmany.side_effect = [
            [(1, "alice"), (2, "bob")],
            [(3, "charlie")],
            [],
        ]

        batches = list(engine.read_bulk_iter(mock_conn, "SELECT * FROM users", batch_size=2))

        assert len(batches) == 2
        assert batches[0].num_rows == 2
        assert batches[1].num_rows == 1
        assert batches[0].column_names == ["id", "name"]

    def test_empty_result_yields_nothing(self):
        engine = Engine(provider="postgresql")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.description = [("id",)]
        mock_cursor.fetchmany.return_value = []

        batches = list(engine.read_bulk_iter(mock_conn, "SELECT * FROM empty_table"))
        assert len(batches) == 0

    def test_single_large_batch(self):
        engine = Engine(provider="postgresql")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.description = [("val",)]
        mock_cursor.fetchmany.side_effect = [
            [(i,) for i in range(100)],
            [],
        ]

        batches = list(engine.read_bulk_iter(mock_conn, "SELECT val FROM big", batch_size=100))
        assert len(batches) == 1
        assert batches[0].num_rows == 100


class TestChunkedReadMySQL:

    def test_yields_batches(self):
        engine = Engine(provider="mysql")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.description = [("id",), ("value",)]
        mock_cursor.fetchmany.side_effect = [
            [(1, 100.0), (2, 200.0)],
            [],
        ]

        batches = list(engine.read_bulk_iter(mock_conn, "SELECT * FROM data", batch_size=2))
        assert len(batches) == 1
        assert batches[0].num_rows == 2
