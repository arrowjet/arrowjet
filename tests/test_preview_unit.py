"""Unit tests for preview command enhancements (10.14 truncate wide output)."""

from click.testing import CliRunner
from arrowjet.cli.main import cli


class TestPreviewTruncation:

    def test_help_shows_max_width(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["preview", "--help"])
        assert result.exit_code == 0
        assert "--max-width" in result.output
