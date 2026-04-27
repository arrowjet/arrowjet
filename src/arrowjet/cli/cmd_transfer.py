"""
arrowjet transfer - move data between databases.

Uses arrowjet.transfer() under the hood: read from source via bulk path,
write to destination via bulk path, Arrow as the bridge.

Examples:
    arrowjet transfer \\
      --from-provider postgresql --from-host pg-host --from-password pgpass \\
      --to-provider mysql --to-host mysql-host --to-password mypass \\
      --query "SELECT * FROM orders" --to-table orders

    arrowjet transfer \\
      --from-profile my-postgres \\
      --to-profile my-mysql \\
      --query "SELECT * FROM orders" --to-table orders
"""

import sys
import time
import click

from .config import (
    resolve_cli_connection_params,
    validate_connection_params,
    print_connection_context,
    make_pg_connection,
    make_pg_engine,
    make_mysql_connection,
    make_mysql_engine,
    make_arrowjet_connection,
)


def _make_connection_and_engine(params):
    """Create a connection and engine for the given provider params."""
    provider = params.get("provider", "redshift")

    if provider == "postgresql":
        conn = make_pg_connection(params)
        engine = make_pg_engine()
    elif provider == "mysql":
        conn = make_mysql_connection(params)
        engine = make_mysql_engine()
    elif provider == "redshift":
        from arrowjet.engine import Engine
        if not params.get("staging_bucket") or not params.get("staging_iam_role"):
            click.echo("Error: staging_bucket and staging_iam_role required for Redshift.", err=True)
            raise SystemExit(1)
        engine = Engine(
            provider="redshift",
            staging_bucket=params["staging_bucket"],
            staging_iam_role=params["staging_iam_role"],
            staging_region=params.get("staging_region", "us-east-1"),
        )
        conn = make_arrowjet_connection(params)
    else:
        click.echo(f"Error: unknown provider '{provider}'", err=True)
        raise SystemExit(1)

    return conn, engine


@click.command("transfer")
@click.option("--query", required=True, help="SQL query to execute on the source")
@click.option("--to-table", required=True, help="Destination table name (must exist)")
# Source connection
@click.option("--from-provider", default=None, type=click.Choice(["redshift", "postgresql", "mysql"]),
              help="Source database provider")
@click.option("--from-profile", default=None, help="Source config profile name")
@click.option("--from-host", default=None, help="Source database host")
@click.option("--from-database", default=None, help="Source database name")
@click.option("--from-user", default=None, help="Source database user")
@click.option("--from-password", default=None, help="Source database password")
@click.option("--from-staging-bucket", default=None, help="Source S3 staging bucket (Redshift)")
@click.option("--from-iam-role", default=None, help="Source IAM role (Redshift)")
@click.option("--from-region", default=None, help="Source AWS region")
# Destination connection
@click.option("--to-provider", default=None, type=click.Choice(["redshift", "postgresql", "mysql"]),
              help="Destination database provider")
@click.option("--to-profile", default=None, help="Destination config profile name")
@click.option("--to-host", default=None, help="Destination database host")
@click.option("--to-database", default=None, help="Destination database name")
@click.option("--to-user", default=None, help="Destination database user")
@click.option("--to-password", default=None, help="Destination database password")
@click.option("--to-staging-bucket", default=None, help="Destination S3 staging bucket (Redshift)")
@click.option("--to-iam-role", default=None, help="Destination IAM role (Redshift)")
@click.option("--to-region", default=None, help="Destination AWS region")
def transfer_cmd(query, to_table,
                 from_provider, from_profile, from_host, from_database, from_user, from_password,
                 from_staging_bucket, from_iam_role, from_region,
                 to_provider, to_profile, to_host, to_database, to_user, to_password,
                 to_staging_bucket, to_iam_role, to_region):
    """Transfer data between databases."""

    # Resolve source params
    src_params = resolve_cli_connection_params(
        from_profile, from_host, from_database, from_user, from_password,
        from_staging_bucket, from_iam_role, from_region,
        provider=from_provider,
    )
    err = validate_connection_params(src_params)
    if err:
        click.echo(f"Source error: {err}", err=True)
        sys.exit(1)

    # Resolve destination params
    dst_params = resolve_cli_connection_params(
        to_profile, to_host, to_database, to_user, to_password,
        to_staging_bucket, to_iam_role, to_region,
        provider=to_provider,
    )
    err = validate_connection_params(dst_params)
    if err:
        click.echo(f"Destination error: {err}", err=True)
        sys.exit(1)

    src_provider = src_params.get("provider", "redshift")
    dst_provider = dst_params.get("provider", "redshift")

    click.echo(f"Source: {src_provider} ({src_params['host']})")
    click.echo(f"Destination: {dst_provider} ({dst_params['host']})")
    click.echo(f"Query: {query}")
    click.echo(f"Target table: {to_table}")
    click.echo()

    # Create connections and engines
    src_conn, src_engine = _make_connection_and_engine(src_params)
    dst_conn, dst_engine = _make_connection_and_engine(dst_params)

    # Execute transfer
    from arrowjet.transfer import transfer

    click.echo(f"Transferring {src_provider} -> {dst_provider}...")
    start = time.perf_counter()

    try:
        result = transfer(
            source_engine=src_engine,
            source_conn=src_conn,
            query=query,
            dest_engine=dst_engine,
            dest_conn=dst_conn,
            dest_table=to_table,
        )
    except Exception as e:
        click.echo(f"Transfer failed: {e}", err=True)
        sys.exit(1)
    finally:
        src_conn.close()
        dst_conn.close()

    elapsed = time.perf_counter() - start
    click.echo(
        f"Transferred {result.rows:,} rows in {elapsed:.2f}s "
        f"(read={result.read_time_s}s, write={result.write_time_s}s)"
    )
