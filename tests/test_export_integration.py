"""
Integration tests for export enhancements (dry-run, from-file, progress).

Parameterized across PostgreSQL, MySQL (dry-run + from-file).
Export-to-file tests use PostgreSQL (COPY is the most complete path).
Requires environment variables per provider (see .env.example).
"""

import os
import tempfile

import pytest
from click.testing import CliRunner

from arrowjet.cli.main import cli

PG_HOST = os.environ.get("PG_HOST")
PG_PASS = os.environ.get("PG_PASS")
MYSQL_HOST = os.environ.get("MYSQL_HOST")
MYSQL_PASS = os.environ.get("MYSQL_PASS")

requires_pg = pytest.mark.skipif(not PG_HOST or not PG_PASS, reason="PG_HOST and PG_PASS required")
requires_mysql = pytest.mark.skipif(not MYSQL_HOST or not MYSQL_PASS, reason="MYSQL_HOST and MYSQL_PASS required")


def _pg_args():
    return [
        "--provider", "postgresql",
        "--host", PG_HOST,
        "--database", os.environ.get("PG_DATABASE", "dev"),
        "--user", os.environ.get("PG_USER", "awsuser"),
        "--password", PG_PASS,
    ]


def _mysql_args():
    return [
        "--provider", "mysql",
        "--host", MYSQL_HOST,
        "--database", os.environ.get("MYSQL_DATABASE", "dev"),
        "--user", os.environ.get("MYSQL_USER", "awsuser"),
        "--password", MYSQL_PASS,
    ]


# --- Dry-run tests (parameterized) ---

_DRY_RUN_PROVIDERS = []
if PG_HOST and PG_PASS:
    _DRY_RUN_PROVIDERS.append(pytest.param("postgresql", _pg_args, "COPY", id="postgresql"))
if MYSQL_HOST and MYSQL_PASS:
    _DRY_RUN_PROVIDERS.append(pytest.param("mysql", _mysql_args, "cursor fetch", id="mysql"))


@pytest.mark.skipif(len(_DRY_RUN_PROVIDERS) == 0, reason="No database credentials available")
@pytest.mark.parametrize("provider_name,args_factory,expected_keyword", _DRY_RUN_PROVIDERS)
class TestDryRunIntegration:

    def test_dry_run_shows_sql(self, provider_name, args_factory, expected_keyword):
        runner = CliRunner()
        result = runner.invoke(cli, [
            "export", "--query", "SELECT 1 AS val",
            "--to", "./test_out.parquet",
            "--dry-run", *args_factory(),
        ])
        assert result.exit_code == 0
        assert "DRY RUN" in result.output
        assert expected_keyword in result.output

    def test_dry_run_does_not_create_file(self, provider_name, args_factory, expected_keyword):
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            dest = os.path.join(tmpdir, "should_not_exist.parquet")
            result = runner.invoke(cli, [
                "export", "--query", "SELECT 1 AS val",
                "--to", dest,
                "--dry-run", *args_factory(),
            ])
            assert result.exit_code == 0
            assert not os.path.exists(dest)


# --- From-file tests (PostgreSQL -- needs COPY for real export) ---

@requires_pg
class TestFromFileIntegration:

    def test_from_file_exports_data(self):
        runner = CliRunner()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("SELECT generate_series(1, 100) AS id")
            f.flush()
            sql_file = f.name

        with tempfile.TemporaryDirectory() as tmpdir:
            dest = os.path.join(tmpdir, "out.parquet")
            try:
                result = runner.invoke(cli, [
                    "export", "--from-file", sql_file,
                    "--to", dest, *_pg_args(),
                ])
                assert result.exit_code == 0
                assert "100" in result.output
                assert os.path.exists(dest)
            finally:
                os.unlink(sql_file)

    def test_from_file_dry_run(self):
        runner = CliRunner()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("SELECT 42 AS answer")
            f.flush()
            sql_file = f.name

        try:
            result = runner.invoke(cli, [
                "export", "--from-file", sql_file,
                "--to", "./out.parquet",
                "--dry-run", *_pg_args(),
            ])
            assert result.exit_code == 0
            assert "DRY RUN" in result.output
            assert "SELECT 42 AS answer" in result.output
        finally:
            os.unlink(sql_file)


# --- Export progress tests (parameterized) ---

_EXPORT_PROVIDERS = []
if PG_HOST and PG_PASS:
    _EXPORT_PROVIDERS.append(pytest.param(
        "postgresql", _pg_args, "SELECT generate_series(1, 50) AS id", "50",
        id="postgresql",
    ))
if MYSQL_HOST and MYSQL_PASS:
    _EXPORT_PROVIDERS.append(pytest.param(
        "mysql", _mysql_args, "SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3", "3",
        id="mysql",
    ))


@pytest.mark.skipif(len(_EXPORT_PROVIDERS) == 0, reason="No database credentials available")
@pytest.mark.parametrize("provider_name,args_factory,query,expected_count", _EXPORT_PROVIDERS)
class TestExportProgressIntegration:

    def test_export_shows_row_count(self, provider_name, args_factory, query, expected_count):
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            dest = os.path.join(tmpdir, "out.parquet")
            result = runner.invoke(cli, [
                "export", "--query", query,
                "--to", dest, *args_factory(),
            ])
            assert result.exit_code == 0
            assert expected_count in result.output
            assert os.path.exists(dest)


# --- Iceberg format export test ---

@requires_pg
class TestExportIcebergFormatIntegration:

    def test_export_iceberg_format_via_cli(self):
        """Test --format iceberg through the CLI export command."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            warehouse = os.path.join(tmpdir, "warehouse")
            result = runner.invoke(cli, [
                "export",
                "--query", "SELECT generate_series(1, 25) AS id",
                "--to", warehouse,
                "--format", "iceberg",
                "--iceberg-table", "cli_test.export_tbl",
                *_pg_args(),
            ])
            assert result.exit_code == 0
            assert "25" in result.output
            assert "Iceberg" in result.output or "iceberg" in result.output

    def test_export_iceberg_missing_table_name_errors(self):
        """--format iceberg without --iceberg-table should error."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            warehouse = os.path.join(tmpdir, "warehouse")
            result = runner.invoke(cli, [
                "export",
                "--query", "SELECT 1 AS id",
                "--to", warehouse,
                "--format", "iceberg",
                *_pg_args(),
            ])
            assert result.exit_code != 0
