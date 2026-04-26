"""
Unit tests for the CLI commands  - no AWS or Redshift calls required.
Tests option resolution, config loading, connection context, and error handling.
"""

import os
import pytest
from unittest.mock import patch, MagicMock
from click.testing import CliRunner

from arrowjet.cli.main import cli
from arrowjet.cli.config import resolve_option, get_profile, print_connection_context


# ── Config / option resolution ────────────────────────────────────────────────

class TestResolveOption:
    def test_cli_value_wins(self):
        assert resolve_option("cli_val", "key", "ENV_VAR", {"key": "profile_val"}) == "cli_val"

    def test_env_var_wins_over_profile(self, monkeypatch):
        monkeypatch.setenv("MY_VAR", "env_val")
        assert resolve_option(None, "key", "MY_VAR", {"key": "profile_val"}) == "env_val"

    def test_profile_used_when_no_cli_or_env(self):
        assert resolve_option(None, "key", "NONEXISTENT_VAR_XYZ", {"key": "profile_val"}) == "profile_val"

    def test_returns_none_when_nothing_set(self):
        assert resolve_option(None, "key", "NONEXISTENT_VAR_XYZ", {}) is None

    def test_empty_string_cli_value_not_used(self):
        # Empty string is falsy  - falls through to env/profile
        result = resolve_option("", "key", "NONEXISTENT_VAR_XYZ", {"key": "profile_val"})
        assert result == "profile_val"


class TestGetProfile:
    def test_returns_empty_dict_when_no_config(self, tmp_path, monkeypatch):
        monkeypatch.setattr("arrowjet.cli.config.CONFIG_FILE", tmp_path / "config.yaml")
        assert get_profile() == {}

    def test_returns_named_profile(self, tmp_path, monkeypatch):
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "profiles:\n  dev:\n    host: my-host\n    database: dev\n"
        )
        monkeypatch.setattr("arrowjet.cli.config.CONFIG_FILE", config_file)
        profile = get_profile("dev")
        assert profile["host"] == "my-host"

    def test_returns_default_profile(self, tmp_path, monkeypatch):
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "default_profile: prod\nprofiles:\n  prod:\n    host: prod-host\n"
        )
        monkeypatch.setattr("arrowjet.cli.config.CONFIG_FILE", config_file)
        profile = get_profile()
        assert profile["host"] == "prod-host"

    def test_returns_empty_for_missing_profile(self, tmp_path, monkeypatch):
        config_file = tmp_path / "config.yaml"
        config_file.write_text("profiles:\n  dev:\n    host: my-host\n")
        monkeypatch.setattr("arrowjet.cli.config.CONFIG_FILE", config_file)
        assert get_profile("nonexistent") == {}


class TestPrintConnectionContext:
    def test_shows_short_host_and_database(self, capsys):
        print_connection_context("my-cluster.region.redshift.amazonaws.com", "dev")
        captured = capsys.readouterr()
        assert "my-cluster" in captured.out
        assert "dev" in captured.out

    def test_shows_profile_name(self, capsys):
        print_connection_context("host.example.com", "mydb", profile_name="prod")
        captured = capsys.readouterr()
        assert "[prod]" in captured.out

    def test_shows_auth_type(self, capsys):
        print_connection_context("host.example.com", "mydb", auth_type="iam")
        captured = capsys.readouterr()
        assert "(iam)" in captured.out

    def test_hides_password_auth_label(self, capsys):
        print_connection_context("host.example.com", "mydb", auth_type="password")
        captured = capsys.readouterr()
        assert "(password)" not in captured.out

    def test_handles_host_without_dots(self, capsys):
        print_connection_context("localhost", "dev")
        captured = capsys.readouterr()
        assert "localhost" in captured.out


# ── CLI command tests (using Click test runner) ───────────────────────────────

class TestCliHelp:
    def test_main_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "arrowjet" in result.output

    def test_export_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["export", "--help"])
        assert result.exit_code == 0
        assert "--query" in result.output
        assert "--to" in result.output

    def test_import_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["import", "--help"])
        assert result.exit_code == 0
        assert "--from" in result.output
        assert "--to" in result.output

    def test_validate_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["validate", "--help"])
        assert result.exit_code == 0
        assert "--table" in result.output
        assert "--schema-name" in result.output
        assert "--profile" in result.output

    def test_preview_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["preview", "--help"])
        assert result.exit_code == 0
        assert "--file" in result.output

    def test_configure_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["configure", "--help"])
        assert result.exit_code == 0
        assert "--profile" in result.output

    def test_version(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        assert "0." in result.output


class TestExportErrors:
    def test_missing_host_shows_error(self, monkeypatch):
        # Clear all env vars and profile so no credentials are available
        for var in ["REDSHIFT_HOST", "REDSHIFT_PASS", "REDSHIFT_USER", "REDSHIFT_DATABASE",
                     "STAGING_BUCKET", "STAGING_IAM_ROLE", "STAGING_REGION", "REDSHIFT_AUTH_SECRET_ARN"]:
            monkeypatch.delenv(var, raising=False)
        runner = CliRunner()
        with patch("arrowjet.cli.config.get_profile", return_value={}):
            result = runner.invoke(cli, [
                "export", "--query", "SELECT 1", "--to", "/tmp/out.parquet",
            ])
        assert result.exit_code != 0 or "Error" in result.output

    def test_missing_query_required(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["export", "--to", "/tmp/out.parquet"])
        assert result.exit_code != 0
        assert "Missing option" in result.output or "required" in result.output.lower()

    def test_missing_to_required(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["export", "--query", "SELECT 1"])
        assert result.exit_code != 0


class TestImportErrors:
    def test_missing_from_required(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["import", "--to", "my_table"])
        assert result.exit_code != 0

    def test_missing_to_required(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["import", "--from", "s3://bucket/path/"])
        assert result.exit_code != 0

    def test_missing_iam_role_shows_error(self, monkeypatch):
        for var in ["REDSHIFT_HOST", "REDSHIFT_PASS", "REDSHIFT_USER", "STAGING_BUCKET",
                     "STAGING_IAM_ROLE", "STAGING_REGION", "REDSHIFT_AUTH_SECRET_ARN"]:
            monkeypatch.delenv(var, raising=False)
        runner = CliRunner()
        with patch("arrowjet.cli.config.get_profile", return_value={}):
            result = runner.invoke(cli, [
                "import",
                "--from", "s3://bucket/path/",
                "--to", "my_table",
                "--host", "my-host",
                "--password", "pass",
            ])
        assert "Error" in result.output or result.exit_code != 0

    def test_local_file_without_staging_bucket_shows_error(self, monkeypatch):
        for var in ["REDSHIFT_HOST", "REDSHIFT_PASS", "REDSHIFT_USER", "STAGING_BUCKET",
                     "STAGING_IAM_ROLE", "STAGING_REGION", "REDSHIFT_AUTH_SECRET_ARN"]:
            monkeypatch.delenv(var, raising=False)
        runner = CliRunner()
        with patch("arrowjet.cli.config.get_profile", return_value={}):
            result = runner.invoke(cli, [
                "import",
                "--from", "/tmp/data.parquet",
                "--to", "my_table",
                "--host", "my-host",
                "--password", "pass",
                "--iam-role", "arn:aws:iam::123:role/test",
            ])
        assert "staging" in result.output.lower() or result.exit_code != 0


class TestValidateErrors:
    def test_missing_table_required(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["validate"])
        assert result.exit_code != 0

    def test_missing_credentials_shows_error(self, monkeypatch):
        for var in ["REDSHIFT_HOST", "REDSHIFT_PASS", "REDSHIFT_USER",
                     "STAGING_BUCKET", "STAGING_IAM_ROLE", "STAGING_REGION",
                     "REDSHIFT_AUTH_SECRET_ARN"]:
            monkeypatch.delenv(var, raising=False)
        runner = CliRunner()
        with patch("arrowjet.cli.config.get_profile", return_value={}):
            result = runner.invoke(cli, [
                "validate", "--table", "my_table",
            ])
        assert "Error" in result.output or result.exit_code != 0


class TestPreviewLocalFile:
    def test_preview_local_parquet(self, tmp_path):
        import pyarrow as pa
        import pyarrow.parquet as pq

        # Create a small test Parquet file
        table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        parquet_file = tmp_path / "test.parquet"
        pq.write_table(table, str(parquet_file))

        runner = CliRunner()
        result = runner.invoke(cli, ["preview", "--file", str(parquet_file)])
        assert result.exit_code == 0
        assert "Rows: 3" in result.output
        assert "Columns: 2" in result.output
        assert "id" in result.output
        assert "name" in result.output

    def test_preview_schema_only(self, tmp_path):
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.table({"x": [1, 2], "y": [3.0, 4.0]})
        parquet_file = tmp_path / "test.parquet"
        pq.write_table(table, str(parquet_file))

        runner = CliRunner()
        result = runner.invoke(cli, ["preview", "--file", str(parquet_file), "--schema"])
        assert result.exit_code == 0
        assert "Schema:" in result.output
        # Sample should not appear when --schema flag is set
        assert "Sample" not in result.output

    def test_preview_nonexistent_file(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["preview", "--file", "/nonexistent/path.parquet"])
        assert result.exit_code != 0


# ── PostgreSQL CLI tests ──────────────────────────────────────────────────────

class TestExportProviderFlag:
    """Test that --provider flag is accepted and routes correctly."""

    def test_export_help_shows_provider_option(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["export", "--help"])
        assert "--provider" in result.output
        assert "postgresql" in result.output

    def test_export_postgresql_missing_host_shows_pg_error(self, monkeypatch):
        """PostgreSQL error message should mention PG_HOST, not REDSHIFT_HOST."""
        for var in ["PG_HOST", "PG_PASS", "PG_USER", "PG_DATABASE",
                     "REDSHIFT_HOST", "REDSHIFT_PASS", "STAGING_BUCKET",
                     "STAGING_IAM_ROLE", "STAGING_REGION", "REDSHIFT_AUTH_SECRET_ARN"]:
            monkeypatch.delenv(var, raising=False)
        runner = CliRunner()
        with patch("arrowjet.cli.config.get_profile", return_value={}):
            result = runner.invoke(cli, [
                "export", "--provider", "postgresql",
                "--query", "SELECT 1", "--to", "/tmp/out.parquet",
            ])
        assert result.exit_code != 0 or "Error" in result.output
        assert "PG_HOST" in result.output or "PostgreSQL" in result.output

    def test_export_postgresql_missing_password_shows_pg_error(self, monkeypatch):
        for var in ["PG_PASS", "REDSHIFT_PASS", "STAGING_BUCKET",
                     "STAGING_IAM_ROLE", "STAGING_REGION", "REDSHIFT_AUTH_SECRET_ARN"]:
            monkeypatch.delenv(var, raising=False)
        monkeypatch.setenv("PG_HOST", "my-pg-host")
        runner = CliRunner()
        with patch("arrowjet.cli.config.get_profile", return_value={}):
            result = runner.invoke(cli, [
                "export", "--provider", "postgresql",
                "--query", "SELECT 1", "--to", "/tmp/out.parquet",
            ])
        assert result.exit_code != 0 or "Error" in result.output
        assert "PG_PASS" in result.output or "Password" in result.output


class TestConfigResolvePostgreSQL:
    """Test that config resolution uses PG_* env vars for postgresql provider."""

    def test_pg_env_vars_used(self, monkeypatch):
        from arrowjet.cli.config import resolve_cli_connection_params
        monkeypatch.setenv("PG_HOST", "pg-host.example.com")
        monkeypatch.setenv("PG_PASS", "pgpass123")
        monkeypatch.setenv("PG_DATABASE", "mydb")
        monkeypatch.setenv("PG_USER", "pguser")
        monkeypatch.setenv("PG_PORT", "5433")
        with patch("arrowjet.cli.config.get_profile", return_value={}):
            params = resolve_cli_connection_params(
                profile=None, host=None, database=None, user=None, password=None,
                staging_bucket=None, iam_role=None, region=None,
                provider="postgresql",
            )
        assert params["provider"] == "postgresql"
        assert params["host"] == "pg-host.example.com"
        assert params["password"] == "pgpass123"
        assert params["database"] == "mydb"
        assert params["user"] == "pguser"
        assert params["port"] == 5433

    def test_redshift_env_vars_used_by_default(self, monkeypatch):
        from arrowjet.cli.config import resolve_cli_connection_params
        monkeypatch.setenv("REDSHIFT_HOST", "rs-host.example.com")
        monkeypatch.setenv("REDSHIFT_PASS", "rspass")
        with patch("arrowjet.cli.config.get_profile", return_value={}):
            params = resolve_cli_connection_params(
                profile=None, host=None, database=None, user=None, password=None,
                staging_bucket=None, iam_role=None, region=None,
            )
        assert params["provider"] == "redshift"
        assert params["host"] == "rs-host.example.com"

    def test_profile_provider_field(self, monkeypatch):
        """Provider can come from the profile config."""
        from arrowjet.cli.config import resolve_cli_connection_params
        monkeypatch.delenv("PG_HOST", raising=False)
        monkeypatch.delenv("PG_PASS", raising=False)
        profile = {"provider": "postgresql", "host": "aurora.example.com", "password": "pass"}
        with patch("arrowjet.cli.config.get_profile", return_value=profile):
            params = resolve_cli_connection_params(
                profile="my-aurora", host=None, database=None, user=None, password=None,
                staging_bucket=None, iam_role=None, region=None,
            )
        assert params["provider"] == "postgresql"
        assert params["host"] == "aurora.example.com"

    def test_cli_provider_overrides_profile(self, monkeypatch):
        """CLI --provider flag should override profile provider."""
        from arrowjet.cli.config import resolve_cli_connection_params
        profile = {"provider": "redshift", "host": "rs.example.com", "password": "pass"}
        monkeypatch.setenv("PG_HOST", "pg.example.com")
        monkeypatch.setenv("PG_PASS", "pgpass")
        with patch("arrowjet.cli.config.get_profile", return_value=profile):
            params = resolve_cli_connection_params(
                profile="my-profile", host=None, database=None, user=None, password=None,
                staging_bucket=None, iam_role=None, region=None,
                provider="postgresql",
            )
        assert params["provider"] == "postgresql"
        assert params["host"] == "pg.example.com"


class TestConfigResolveMysql:
    """Test that config resolution uses MYSQL_* env vars for mysql provider."""

    def test_mysql_env_vars_used(self, monkeypatch):
        from arrowjet.cli.config import resolve_cli_connection_params
        monkeypatch.setenv("MYSQL_HOST", "mysql-host.example.com")
        monkeypatch.setenv("MYSQL_PASS", "mysqlpass")
        monkeypatch.setenv("MYSQL_DATABASE", "mydb")
        monkeypatch.setenv("MYSQL_USER", "mysqluser")
        monkeypatch.setenv("MYSQL_PORT", "3307")
        with patch("arrowjet.cli.config.get_profile", return_value={}):
            params = resolve_cli_connection_params(
                profile=None, host=None, database=None, user=None, password=None,
                staging_bucket=None, iam_role=None, region=None,
                provider="mysql",
            )
        assert params["provider"] == "mysql"
        assert params["host"] == "mysql-host.example.com"
        assert params["password"] == "mysqlpass"
        assert params["port"] == 3307

    def test_export_help_shows_mysql(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["export", "--help"])
        assert "mysql" in result.output
