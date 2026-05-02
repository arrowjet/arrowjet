"""
Integration tests for Iceberg output format (M25).

Tests the end-to-end flow: database -> ArrowJet -> Iceberg table -> read back.
Uses local SQLite catalog (no S3 needed for catalog, but S3 needed for Redshift source).
PostgreSQL tests need PG_HOST/PG_PASS. Redshift tests need REDSHIFT_HOST/REDSHIFT_PASS.
"""

import os

import pytest
import pyarrow as pa

from arrowjet.iceberg import write_iceberg, read_iceberg

PG_HOST = os.environ.get("PG_HOST")
PG_PASS = os.environ.get("PG_PASS")

requires_pg = pytest.mark.skipif(not PG_HOST or not PG_PASS, reason="PG_HOST and PG_PASS required")


def _pg_conn():
    import psycopg2
    return psycopg2.connect(
        host=PG_HOST, port=int(os.environ.get("PG_PORT", "5432")),
        dbname=os.environ.get("PG_DATABASE", "dev"),
        user=os.environ.get("PG_USER", "awsuser"), password=PG_PASS,
        connect_timeout=10,
    )


class TestIcebergLocalCatalog:
    """Test write/read with local SQLite catalog (no S3 needed)."""

    def test_roundtrip_types(self, tmp_path):
        """Write various Arrow types to Iceberg and read them back."""
        warehouse = str(tmp_path / "wh")
        table = pa.table({
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "price": pa.array([9.99, 19.50, 0.01], type=pa.float64()),
            "name": pa.array(["widget", "gadget", "thing"], type=pa.string()),
            "active": pa.array([True, False, True], type=pa.bool_()),
        })

        result = write_iceberg(table, "test.products", warehouse)
        assert result.rows == 3

        read_back = read_iceberg("test.products", warehouse)
        assert read_back.num_rows == 3
        assert set(read_back.column_names) == {"id", "price", "name", "active"}

    def test_multiple_appends(self, tmp_path):
        """Multiple appends accumulate rows."""
        warehouse = str(tmp_path / "wh")

        for i in range(3):
            batch = pa.table({"id": [i * 10 + j for j in range(10)]})
            write_iceberg(batch, "test.multi", warehouse, mode="append")

        result = read_iceberg("test.multi", warehouse)
        assert result.num_rows == 30

    def test_overwrite_then_append(self, tmp_path):
        """Overwrite clears data, append adds to it."""
        warehouse = str(tmp_path / "wh")

        write_iceberg(pa.table({"v": [1, 2, 3]}), "test.oa", warehouse)
        write_iceberg(pa.table({"v": [10]}), "test.oa", warehouse, mode="overwrite")
        write_iceberg(pa.table({"v": [20]}), "test.oa", warehouse, mode="append")

        result = read_iceberg("test.oa", warehouse)
        assert result.num_rows == 2
        assert sorted(result.column("v").to_pylist()) == [10, 20]


@requires_pg
class TestIcebergFromPostgreSQL:
    """End-to-end: PostgreSQL -> ArrowJet -> Iceberg -> read back."""

    def test_pg_to_iceberg(self, tmp_path):
        """Read from PostgreSQL via ArrowJet, write to Iceberg, verify."""
        from arrowjet.engine import Engine

        warehouse = str(tmp_path / "wh")
        engine = Engine(provider="postgresql")
        conn = _pg_conn()

        try:
            result = engine.read_bulk(conn, "SELECT generate_series(1, 100) AS id")
            table = result.table

            iceberg_result = write_iceberg(table, "pg_export.series", warehouse)
            assert iceberg_result.rows == 100

            read_back = read_iceberg("pg_export.series", warehouse)
            assert read_back.num_rows == 100
        finally:
            conn.close()

    def test_pg_to_iceberg_with_types(self, tmp_path):
        """Verify type preservation through the full pipeline."""
        from arrowjet.engine import Engine

        warehouse = str(tmp_path / "wh")
        engine = Engine(provider="postgresql")
        conn = _pg_conn()

        try:
            result = engine.read_bulk(
                conn,
                "SELECT 42::bigint AS int_val, 3.14::double precision AS float_val, "
                "'hello'::text AS str_val, true::boolean AS bool_val"
            )
            write_iceberg(result.table, "pg_export.typed", warehouse)

            read_back = read_iceberg("pg_export.typed", warehouse)
            assert read_back.num_rows == 1
            row = read_back.to_pydict()
            assert row["int_val"] == [42]
            assert abs(row["float_val"][0] - 3.14) < 0.001
            assert row["str_val"] == ["hello"]
            assert row["bool_val"] == [True] or row["bool_val"] == ["t"]
        finally:
            conn.close()

    def test_pg_chunked_to_iceberg(self, tmp_path):
        """Chunked read from PG, accumulate into Iceberg via multiple appends."""
        from arrowjet.engine import Engine

        warehouse = str(tmp_path / "wh")
        engine = Engine(provider="postgresql")
        conn = _pg_conn()

        try:
            for batch in engine.read_bulk_iter(
                conn, "SELECT generate_series(1, 50) AS id", batch_size=15
            ):
                batch_table = pa.Table.from_batches([batch])
                write_iceberg(batch_table, "pg_export.chunked", warehouse, mode="append")

            read_back = read_iceberg("pg_export.chunked", warehouse)
            assert read_back.num_rows == 50
        finally:
            conn.close()
