"""
Arrowjet CLI entry point.

Usage:
    arrowjet configure
    arrowjet export --query "SELECT * FROM sales" --to s3://bucket/sales
    arrowjet preview --file s3://bucket/sales/*.parquet
    arrowjet validate --table sales --row-count
"""

import click
from arrowjet import __version__

from .cmd_configure import configure
from .cmd_diff import diff_cmd
from .cmd_export import export
from .cmd_import import import_cmd
from .cmd_preview import preview
from .cmd_profiles import profiles
from .cmd_validate import validate
from .cmd_transfer import transfer_cmd


@click.group()
@click.version_option(__version__, "--version", "-v")
def cli():
    """arrowjet: Fast bulk data movement for cloud databases."""
    pass


cli.add_command(configure)
cli.add_command(diff_cmd)
cli.add_command(export)
cli.add_command(import_cmd)
cli.add_command(preview)
cli.add_command(profiles)
cli.add_command(validate)
cli.add_command(transfer_cmd)

# Plugin CLI discovery: extensions can register additional commands
_cli_plugins = []


def register_cli_command(command):
    """
    Register an additional CLI command from a plugin.

    Called by extension packages (e.g., arrowjet-pro) to add commands
    like `arrowjet schema-check` or `arrowjet drift-report`.
    """
    _cli_plugins.append(command)
    cli.add_command(command)


def main():
    # Ensure plugin discovery runs before CLI
    import arrowjet  # noqa: F401 - triggers plugin auto-discovery
    cli()


if __name__ == "__main__":
    main()
