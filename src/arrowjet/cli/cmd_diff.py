"""
arrowjet diff -- compare data between two databases or tables.

Examples:
    arrowjet diff --from-profile pg --to-profile mysql --table orders --key id
    arrowjet diff --from-profile pg --to-profile pg --query "SELECT * FROM v1" --dest-table v2 --key id
"""

import sys
import click

from .config import (
    load_config,
    get_profile,
    resolve_option,
)


@click.command()
@click.option("--from-profile", required=True, help="Source connection profile")
@click.option("--to-profile", required=True, help="Destination connection profile")
@click.option("--table", default=None, help="Table name (used for both source and dest)")
@click.option("--query", default=None, help="Source query (overrides --table for source)")
@click.option("--dest-table", default=None, help="Destination table (overrides --table for dest)")
@click.option("--key", required=True, help="Key column(s), comma-separated")
@click.option("--include-rows", is_flag=True, help="Include actual row data in output")
@click.option("--verbose", is_flag=True, help="Show detailed output")
def diff_cmd(from_profile, to_profile, table, query, dest_table, key, include_rows, verbose):
    """Compare data between two databases or tables."""
    from arrowjet.diff import diff as arrowjet_diff, diff_tables

    # Resolve source and dest queries
    if not table and not query:
        click.echo("Error: --table or --query is required.", err=True)
        sys.exit(1)

    source_query = query or f"SELECT * FROM {table}"
    dest_tbl = dest_table or table

    if not dest_tbl:
        click.echo("Error: --dest-table is required when using --query.", err=True)
        sys.exit(1)

    key_columns = [k.strip() for k in key.split(",")]

    # Load profiles
    src_profile = get_profile(from_profile)
    dst_profile = get_profile(to_profile)

    if not src_profile:
        click.echo(f"Error: profile '{from_profile}' not found.", err=True)
        sys.exit(1)
    if not dst_profile:
        click.echo(f"Error: profile '{to_profile}' not found.", err=True)
        sys.exit(1)

    src_provider = src_profile.get("provider", "redshift")
    dst_provider = dst_profile.get("provider", "redshift")

    click.echo(f"Diff: {src_provider} ({from_profile}) -> {dst_provider} ({to_profile})")
    click.echo(f"Source: {source_query[:80]}")
    click.echo(f"Dest:   {dest_tbl}")
    click.echo(f"Key:    {', '.join(key_columns)}")
    click.echo()

    # Create engines and connections
    src_engine, src_conn = _make_engine_conn(src_profile, src_provider)
    dst_engine, dst_conn = _make_engine_conn(dst_profile, dst_provider)

    try:
        result = arrowjet_diff(
            source_engine=src_engine,
            source_conn=src_conn,
            dest_engine=dst_engine,
            dest_conn=dst_conn,
            query=source_query,
            dest_table=dest_tbl,
            key_columns=key_columns,
            include_rows=include_rows,
        )

        click.echo(result.summary())
        click.echo(f"\nCompleted in {result.total_time_s}s")

        if include_rows and verbose:
            if result.added_rows and result.added_rows.num_rows > 0:
                click.echo(f"\nAdded rows ({result.added_count}):")
                click.echo(result.added_rows.to_pandas().head(10).to_string(index=False))
            if result.removed_rows and result.removed_rows.num_rows > 0:
                click.echo(f"\nRemoved rows ({result.removed_count}):")
                click.echo(result.removed_rows.to_pandas().head(10).to_string(index=False))
            if result.changed_rows and result.changed_rows.num_rows > 0:
                click.echo(f"\nChanged rows ({result.changed_count}):")
                click.echo(result.changed_rows.to_pandas().head(10).to_string(index=False))

        # Exit code: 0 if no differences, 1 if differences found
        if result.has_differences:
            sys.exit(1)

    finally:
        src_conn.close()
        dst_conn.close()


def _make_engine_conn(profile, provider):
    """Create an Engine and connection from a profile dict."""
    from arrowjet.engine import Engine

    if provider == "postgresql":
        import psycopg2
        engine = Engine(provider="postgresql")
        conn = psycopg2.connect(
            host=profile["host"],
            port=int(profile.get("port", 5432)),
            dbname=profile.get("database", "dev"),
            user=profile.get("user", ""),
            password=profile.get("password", ""),
            connect_timeout=10,
        )
    elif provider == "mysql":
        import pymysql
        engine = Engine(provider="mysql")
        conn = pymysql.connect(
            host=profile["host"],
            port=int(profile.get("port", 3306)),
            database=profile.get("database", "dev"),
            user=profile.get("user", ""),
            password=profile.get("password", ""),
            connect_timeout=10,
            local_infile=True,
        )
    elif provider == "redshift":
        engine = Engine(
            provider="redshift",
            staging_bucket=profile.get("staging_bucket"),
            staging_iam_role=profile.get("staging_iam_role"),
            staging_region=profile.get("staging_region", "us-east-1"),
        )
        import redshift_connector
        conn = redshift_connector.connect(
            host=profile["host"],
            port=int(profile.get("port", 5439)),
            database=profile.get("database", "dev"),
            user=profile.get("user", ""),
            password=profile.get("password", ""),
        )
    else:
        raise ValueError(f"Unknown provider: {provider}")

    return engine, conn
