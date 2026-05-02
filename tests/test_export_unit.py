"""Unit tests for export command enhancements (10.10 dry-run, 10.11 from-file, 10.12 row count)."""

import os
import tempfile
from unittest.mock import patch

from click.testing import CliRunner
from arrowjet.cli.main import cli


class TestDryRunExport:

    def _mock_params(self, provider="postgresql"):
        return {
            "host": "localhost",
            "database": "mydb",
            "auth_type": "password",
            "profile_name": "dev",
            "provider": provider,
            "staging_bucket": None,
            "staging_iam_role": None,
            "staging_region": None,
        }

    def test_dry_run_postgresql(self):
        runner = CliRunner()
        with patch("arrowjet.cli.cmd_export.resolve_cli_connection_params", return_value=self._mock_params()), \
             patch("arrowjet.cli.cmd_export.validate_connection_params", return_value=None), \
             patch("arrowjet.cli.cmd_export.print_connection_context"):
            result = runner.invoke(cli, [
                "export", "--query", "SELECT * FROM orders",
                "--to", "./orders.parquet", "--provider", "postgresql", "--dry-run",
            ])
        assert result.exit_code == 0
        assert "DRY RUN" in result.output
        assert "COPY" in result.output
        assert "SELECT * FROM orders" in result.output
        assert "Remove --dry-run to execute" in result.output

    def test_dry_run_mysql(self):
        runner = CliRunner()
        with patch("arrowjet.cli.cmd_export.resolve_cli_connection_params", return_value=self._mock_params("mysql")), \
             patch("arrowjet.cli.cmd_export.validate_connection_params", return_value=None), \
             patch("arrowjet.cli.cmd_export.print_connection_context"):
            result = runner.invoke(cli, [
                "export", "--query", "SELECT * FROM orders",
                "--to", "./orders.parquet", "--provider", "mysql", "--dry-run",
            ])
        assert result.exit_code == 0
        assert "DRY RUN" in result.output
        assert "cursor fetch" in result.output


class TestFromFileExport:

    def _mock_params(self):
        return {
            "host": "localhost",
            "database": "mydb",
            "auth_type": "password",
            "profile_name": "dev",
            "provider": "postgresql",
            "staging_bucket": None,
            "staging_iam_role": None,
            "staging_region": None,
        }

    def test_from_file_reads_query(self):
        runner = CliRunner()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("SELECT * FROM orders WHERE status = 'active'")
            f.flush()
            sql_file = f.name

        try:
            with patch("arrowjet.cli.cmd_export.resolve_cli_connection_params", return_value=self._mock_params()), \
                 patch("arrowjet.cli.cmd_export.validate_connection_params", return_value=None), \
                 patch("arrowjet.cli.cmd_export.print_connection_context"):
                result = runner.invoke(cli, [
                    "export", "--from-file", sql_file,
                    "--to", "./orders.parquet", "--provider", "postgresql", "--dry-run",
                ])
            assert result.exit_code == 0
            assert "SELECT * FROM orders WHERE status = 'active'" in result.output
        finally:
            os.unlink(sql_file)

    def test_from_file_and_query_mutually_exclusive(self):
        runner = CliRunner()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("SELECT 1")
            f.flush()
            sql_file = f.name

        try:
            with patch("arrowjet.cli.cmd_export.resolve_cli_connection_params", return_value=self._mock_params()), \
                 patch("arrowjet.cli.cmd_export.validate_connection_params", return_value=None), \
                 patch("arrowjet.cli.cmd_export.print_connection_context"):
                result = runner.invoke(cli, [
                    "export", "--query", "SELECT 1", "--from-file", sql_file,
                    "--to", "./out.parquet", "--provider", "postgresql",
                ])
            assert result.exit_code != 0
            assert "mutually exclusive" in result.output
        finally:
            os.unlink(sql_file)

    def test_empty_file_errors(self):
        runner = CliRunner()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("")
            f.flush()
            sql_file = f.name

        try:
            with patch("arrowjet.cli.cmd_export.resolve_cli_connection_params", return_value=self._mock_params()), \
                 patch("arrowjet.cli.cmd_export.validate_connection_params", return_value=None), \
                 patch("arrowjet.cli.cmd_export.print_connection_context"):
                result = runner.invoke(cli, [
                    "export", "--from-file", sql_file,
                    "--to", "./out.parquet", "--provider", "postgresql",
                ])
            assert result.exit_code != 0
            assert "empty" in result.output
        finally:
            os.unlink(sql_file)

    def test_no_query_no_file_errors(self):
        runner = CliRunner()
        with patch("arrowjet.cli.cmd_export.resolve_cli_connection_params", return_value=self._mock_params()), \
             patch("arrowjet.cli.cmd_export.validate_connection_params", return_value=None), \
             patch("arrowjet.cli.cmd_export.print_connection_context"):
            result = runner.invoke(cli, [
                "export", "--to", "./out.parquet", "--provider", "postgresql",
            ])
        assert result.exit_code != 0
        assert "--query or --from-file is required" in result.output


class TestS3RowCount:

    def test_count_returns_none_on_error(self):
        from arrowjet.cli.cmd_export import _count_s3_parquet_rows
        result = _count_s3_parquet_rows("s3://nonexistent-bucket/path/", "us-east-1")
        assert result is None
