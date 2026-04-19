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
from .cmd_export import export
from .cmd_preview import preview
from .cmd_validate import validate


@click.group()
@click.version_option(__version__, "--version", "-v")
def cli():
    """arrowjet: Fast, simple bulk data movement for Redshift."""
    pass


cli.add_command(configure)
cli.add_command(export)
cli.add_command(preview)
cli.add_command(validate)


def main():
    cli()


if __name__ == "__main__":
    main()
