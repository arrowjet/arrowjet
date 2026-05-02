"""Unit tests for the arrowjet profiles command (10.9)."""

from unittest.mock import patch
from click.testing import CliRunner
from arrowjet.cli.main import cli


class TestProfilesCommand:

    def test_no_profiles(self):
        runner = CliRunner()
        with patch("arrowjet.cli.cmd_profiles.load_config", return_value={}):
            result = runner.invoke(cli, ["profiles"])
        assert result.exit_code == 0
        assert "No profiles configured" in result.output

    def test_single_profile(self):
        config = {
            "default_profile": "dev",
            "profiles": {
                "dev": {
                    "provider": "postgresql",
                    "host": "localhost",
                    "database": "mydb",
                    "auth": "password",
                }
            }
        }
        runner = CliRunner()
        with patch("arrowjet.cli.cmd_profiles.load_config", return_value=config):
            result = runner.invoke(cli, ["profiles"])
        assert result.exit_code == 0
        assert "dev (default)" in result.output
        assert "postgresql" in result.output
        assert "localhost" in result.output

    def test_multiple_profiles(self):
        config = {
            "default_profile": "dev",
            "profiles": {
                "dev": {
                    "provider": "postgresql",
                    "host": "localhost",
                    "database": "devdb",
                },
                "prod": {
                    "provider": "redshift",
                    "host": "prod-cluster.redshift.amazonaws.com",
                    "database": "prod",
                    "auth": "iam",
                }
            }
        }
        runner = CliRunner()
        with patch("arrowjet.cli.cmd_profiles.load_config", return_value=config):
            result = runner.invoke(cli, ["profiles"])
        assert result.exit_code == 0
        assert "dev (default)" in result.output
        assert "prod" in result.output
        assert "Profiles (2)" in result.output

    def test_verbose_shows_details(self):
        config = {
            "default_profile": "dev",
            "profiles": {
                "dev": {
                    "provider": "redshift",
                    "host": "cluster.redshift.amazonaws.com",
                    "database": "dev",
                    "user": "awsuser",
                    "staging_bucket": "my-bucket",
                    "staging_iam_role": "arn:aws:iam::123:role/Role",
                    "staging_region": "us-east-1",
                }
            }
        }
        runner = CliRunner()
        with patch("arrowjet.cli.cmd_profiles.load_config", return_value=config):
            result = runner.invoke(cli, ["profiles", "--verbose"])
        assert result.exit_code == 0
        assert "awsuser" in result.output
        assert "my-bucket" in result.output
        assert "us-east-1" in result.output
