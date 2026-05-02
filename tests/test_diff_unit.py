"""Unit tests for the data diff engine (M23)."""

import pytest
import pyarrow as pa

from arrowjet.diff import diff_tables, DiffResult


class TestDiffIdentical:

    def test_identical_tables(self):
        t = pa.table({"id": [1, 2, 3], "val": ["a", "b", "c"]})
        result = diff_tables(t, t, key_columns=["id"])
        assert result.added_count == 0
        assert result.removed_count == 0
        assert result.changed_count == 0
        assert result.unchanged_count == 3
        assert not result.has_differences

    def test_empty_tables(self):
        t = pa.table({"id": pa.array([], type=pa.int64())})
        result = diff_tables(t, t, key_columns=["id"])
        assert result.added_count == 0
        assert result.removed_count == 0
        assert result.unchanged_count == 0
        assert not result.has_differences


class TestDiffAdded:

    def test_rows_in_source_not_in_dest(self):
        src = pa.table({"id": [1, 2, 3], "val": ["a", "b", "c"]})
        dst = pa.table({"id": [1, 2], "val": ["a", "b"]})
        result = diff_tables(src, dst, key_columns=["id"])
        assert result.added_count == 1
        assert result.removed_count == 0
        assert result.unchanged_count == 2

    def test_all_added(self):
        src = pa.table({"id": [1, 2, 3]})
        dst = pa.table({"id": pa.array([], type=pa.int64())})
        result = diff_tables(src, dst, key_columns=["id"])
        assert result.added_count == 3
        assert result.removed_count == 0

    def test_added_rows_included(self):
        src = pa.table({"id": [1, 2, 3], "val": ["a", "b", "c"]})
        dst = pa.table({"id": [1], "val": ["a"]})
        result = diff_tables(src, dst, key_columns=["id"], include_rows=True)
        assert result.added_count == 2
        assert result.added_rows is not None
        assert result.added_rows.num_rows == 2


class TestDiffRemoved:

    def test_rows_in_dest_not_in_source(self):
        src = pa.table({"id": [1], "val": ["a"]})
        dst = pa.table({"id": [1, 2, 3], "val": ["a", "b", "c"]})
        result = diff_tables(src, dst, key_columns=["id"])
        assert result.added_count == 0
        assert result.removed_count == 2
        assert result.unchanged_count == 1

    def test_removed_rows_included(self):
        src = pa.table({"id": [1], "val": ["a"]})
        dst = pa.table({"id": [1, 2], "val": ["a", "b"]})
        result = diff_tables(src, dst, key_columns=["id"], include_rows=True)
        assert result.removed_rows is not None
        assert result.removed_rows.num_rows == 1


class TestDiffChanged:

    def test_value_changed(self):
        src = pa.table({"id": [1, 2], "val": ["a", "b_new"]})
        dst = pa.table({"id": [1, 2], "val": ["a", "b_old"]})
        result = diff_tables(src, dst, key_columns=["id"])
        assert result.changed_count == 1
        assert result.unchanged_count == 1
        assert "val" in result.changed_columns

    def test_multiple_columns_changed(self):
        src = pa.table({"id": [1], "name": ["alice"], "score": [100]})
        dst = pa.table({"id": [1], "name": ["bob"], "score": [50]})
        result = diff_tables(src, dst, key_columns=["id"])
        assert result.changed_count == 1
        assert "name" in result.changed_columns
        assert "score" in result.changed_columns

    def test_changed_rows_included(self):
        src = pa.table({"id": [1, 2], "val": ["changed", "same"]})
        dst = pa.table({"id": [1, 2], "val": ["original", "same"]})
        result = diff_tables(src, dst, key_columns=["id"], include_rows=True)
        assert result.changed_rows is not None
        assert result.changed_rows.num_rows == 1


class TestDiffMixed:

    def test_added_removed_changed(self):
        src = pa.table({"id": [1, 2, 4], "val": ["a", "b_new", "d"]})
        dst = pa.table({"id": [1, 2, 3], "val": ["a", "b_old", "c"]})
        result = diff_tables(src, dst, key_columns=["id"])
        assert result.added_count == 1      # id=4
        assert result.removed_count == 1    # id=3
        assert result.changed_count == 1    # id=2
        assert result.unchanged_count == 1  # id=1
        assert result.has_differences


class TestDiffCompositeKey:

    def test_two_column_key(self):
        src = pa.table({"a": [1, 1, 2], "b": ["x", "y", "x"], "val": [10, 20, 30]})
        dst = pa.table({"a": [1, 1, 2], "b": ["x", "y", "x"], "val": [10, 99, 30]})
        result = diff_tables(src, dst, key_columns=["a", "b"])
        assert result.changed_count == 1
        assert result.unchanged_count == 2


class TestDiffSchemaMismatch:

    def test_extra_column_in_source(self):
        src = pa.table({"id": [1], "val": ["a"], "extra": [True]})
        dst = pa.table({"id": [1], "val": ["a"]})
        result = diff_tables(src, dst, key_columns=["id"])
        assert result.schema_diff is not None
        assert "extra" in result.schema_diff["source_only"]
        assert result.unchanged_count == 1

    def test_extra_column_in_dest(self):
        src = pa.table({"id": [1], "val": ["a"]})
        dst = pa.table({"id": [1], "val": ["a"], "extra": [True]})
        result = diff_tables(src, dst, key_columns=["id"])
        assert result.schema_diff is not None
        assert "extra" in result.schema_diff["dest_only"]

    def test_missing_key_column_raises(self):
        src = pa.table({"id": [1], "val": ["a"]})
        dst = pa.table({"id": [1], "val": ["a"]})
        with pytest.raises(ValueError, match="Key column 'missing'"):
            diff_tables(src, dst, key_columns=["missing"])


class TestDiffResult:

    def test_summary_no_diff(self):
        result = DiffResult(total_source=100, total_dest=100, unchanged_count=100)
        assert "No differences" in result.summary()

    def test_summary_with_diff(self):
        result = DiffResult(
            added_count=5, removed_count=2, changed_count=3,
            unchanged_count=90, total_source=98, total_dest=95,
            changed_columns=["status", "amount"],
        )
        s = result.summary()
        assert "Added" in s
        assert "Removed" in s
        assert "Changed" in s
        assert "status" in s

    def test_repr(self):
        result = DiffResult(added_count=1, removed_count=2, changed_count=3, unchanged_count=4)
        assert "added=1" in repr(result)


class TestDiffPublicAPI:

    def test_diff_exported(self):
        import arrowjet
        assert hasattr(arrowjet, "diff")
        assert hasattr(arrowjet, "diff_tables")
        assert hasattr(arrowjet, "DiffResult")

    def test_cli_shows_diff_command(self):
        from click.testing import CliRunner
        from arrowjet.cli.main import cli
        runner = CliRunner()
        result = runner.invoke(cli, ["diff", "--help"])
        assert result.exit_code == 0
        assert "--from-profile" in result.output
        assert "--key" in result.output
