"""
Integration tests for preview command enhancements (truncation).

Requires: a local Parquet file (created in test).
"""

import tempfile
import os

import pyarrow as pa
import pyarrow.parquet as pq
from click.testing import CliRunner

from arrowjet.cli.main import cli


class TestPreviewTruncationIntegration:

    def _create_wide_parquet(self, path):
        """Create a Parquet file with long string values."""
        table = pa.table({
            "id": [1, 2, 3],
            "short_col": ["abc", "def", "ghi"],
            "long_col": [
                "This is a very long string that should be truncated in the preview output to keep things readable",
                "Another extremely long string value that goes on and on and on and on and on and on and on",
                "Yet another long string for testing the max-width truncation feature in the preview command",
            ],
        })
        pq.write_table(table, path)

    def test_default_truncation(self):
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "wide.parquet")
            self._create_wide_parquet(path)

            result = runner.invoke(cli, ["preview", "--file", path])
            assert result.exit_code == 0
            assert "Rows: 3" in result.output
            assert "long_col" in result.output

    def test_max_width_zero_no_truncation(self):
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "wide.parquet")
            self._create_wide_parquet(path)

            result = runner.invoke(cli, ["preview", "--file", path, "--max-width", "0"])
            assert result.exit_code == 0
            # Full string should appear when no truncation
            assert "This is a very long string that should be truncated" in result.output

    def test_schema_only_skips_sample(self):
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "wide.parquet")
            self._create_wide_parquet(path)

            result = runner.invoke(cli, ["preview", "--file", path, "--schema"])
            assert result.exit_code == 0
            assert "id: int64" in result.output
            assert "Sample" not in result.output
