"""Unit tests for Iceberg output format (M25)."""

import pytest
import pyarrow as pa


class TestIcebergWriteResult:

    def test_repr(self):
        from arrowjet.iceberg import IcebergWriteResult
        r = IcebergWriteResult(
            rows=1000, table_name="analytics.orders", mode="append",
            catalog_type="sql", warehouse="/tmp/wh", total_time_s=1.5,
        )
        assert "1,000" in repr(r)
        assert "analytics.orders" in repr(r)
        assert "append" in repr(r)


class TestWriteIcebergValidation:

    def test_invalid_mode_raises(self):
        from arrowjet.iceberg import write_iceberg
        table = pa.table({"id": [1, 2, 3]})
        with pytest.raises(ValueError, match="mode must be"):
            write_iceberg(table, "ns.tbl", "/tmp/wh", mode="upsert")

    def test_unqualified_table_name_raises(self):
        from arrowjet.iceberg import write_iceberg
        table = pa.table({"id": [1, 2, 3]})
        with pytest.raises(ValueError, match="fully qualified"):
            write_iceberg(table, "just_a_table", "/tmp/wh")


class TestWriteIcebergLocalCatalog:
    """Test write_iceberg with a local SQLite catalog (no S3 needed)."""

    def test_create_and_append(self, tmp_path):
        from arrowjet.iceberg import write_iceberg, read_iceberg

        warehouse = str(tmp_path / "warehouse")
        table = pa.table({
            "id": [1, 2, 3],
            "name": ["alice", "bob", "charlie"],
            "amount": [10.5, 20.0, 30.75],
        })

        # First write creates the table
        result = write_iceberg(table, "test_ns.orders", warehouse, mode="append")
        assert result.rows == 3
        assert result.table_name == "test_ns.orders"
        assert result.mode == "append"
        assert result.total_time_s > 0

        # Read back
        read_back = read_iceberg("test_ns.orders", warehouse)
        assert read_back.num_rows == 3

    def test_append_adds_rows(self, tmp_path):
        from arrowjet.iceberg import write_iceberg, read_iceberg

        warehouse = str(tmp_path / "warehouse")
        batch1 = pa.table({"id": [1, 2], "val": ["a", "b"]})
        batch2 = pa.table({"id": [3, 4], "val": ["c", "d"]})

        write_iceberg(batch1, "test_ns.data", warehouse, mode="append")
        write_iceberg(batch2, "test_ns.data", warehouse, mode="append")

        result = read_iceberg("test_ns.data", warehouse)
        assert result.num_rows == 4

    def test_overwrite_replaces_data(self, tmp_path):
        from arrowjet.iceberg import write_iceberg, read_iceberg

        warehouse = str(tmp_path / "warehouse")
        original = pa.table({"id": [1, 2, 3]})
        replacement = pa.table({"id": [10, 20]})

        write_iceberg(original, "test_ns.tbl", warehouse, mode="append")
        write_iceberg(replacement, "test_ns.tbl", warehouse, mode="overwrite")

        result = read_iceberg("test_ns.tbl", warehouse)
        assert result.num_rows == 2
        assert result.column("id").to_pylist() == [10, 20]

    def test_schema_preserved(self, tmp_path):
        from arrowjet.iceberg import write_iceberg, read_iceberg

        warehouse = str(tmp_path / "warehouse")
        table = pa.table({
            "int_col": pa.array([1, 2], type=pa.int64()),
            "float_col": pa.array([1.5, 2.5], type=pa.float64()),
            "str_col": pa.array(["a", "b"], type=pa.string()),
            "bool_col": pa.array([True, False], type=pa.bool_()),
        })

        write_iceberg(table, "test_ns.typed", warehouse)
        result = read_iceberg("test_ns.typed", warehouse)

        assert result.num_rows == 2
        assert result.schema.field("int_col").type == pa.int64()
        assert result.schema.field("float_col").type == pa.float64()
        assert result.schema.field("bool_col").type == pa.bool_()

    def test_empty_table(self, tmp_path):
        from arrowjet.iceberg import write_iceberg, read_iceberg

        warehouse = str(tmp_path / "warehouse")
        table = pa.table({"id": pa.array([], type=pa.int64())})

        result = write_iceberg(table, "test_ns.empty", warehouse)
        assert result.rows == 0

        read_back = read_iceberg("test_ns.empty", warehouse)
        assert read_back.num_rows == 0


class TestCLIIcebergFormat:
    """Test the --format iceberg flag on the export CLI."""

    def test_export_help_shows_iceberg(self):
        from click.testing import CliRunner
        from arrowjet.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["export", "--help"])
        assert result.exit_code == 0
        assert "iceberg" in result.output
        assert "--iceberg-table" in result.output
        assert "--iceberg-mode" in result.output
        assert "--iceberg-catalog" in result.output
