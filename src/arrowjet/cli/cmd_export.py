"""
arrowjet export — export data from Redshift to S3 or local file.

S3 destination: UNLOAD runs directly to the destination — no roundtrip
through the client machine. Fast, efficient, data stays in AWS.

Local destination: UNLOAD → staging S3 → download → local file.

Examples:
    arrowjet export --query "SELECT * FROM sales" --to s3://bucket/sales/
    arrowjet export --query "SELECT * FROM sales" --to ./sales.parquet
    arrowjet export --query "SELECT * FROM sales" --to ./sales.csv --format csv
"""

import sys
import time
import click

from .config import (
    resolve_cli_connection_params,
    validate_connection_params,
    print_connection_context,
    make_arrowjet_connection,
    make_raw_connection,
)


@click.command()
@click.option("--query", required=True, help="SQL query to export")
@click.option("--to", "destination", required=True, help="Destination: s3://bucket/path or local file path")
@click.option("--format", "fmt", default="parquet", type=click.Choice(["parquet", "csv"]), help="Output format")
@click.option("--profile", default=None, help="Config profile name")
@click.option("--provider", default=None, type=click.Choice(["redshift", "postgresql", "mysql"]), help="Database provider")
@click.option("--host", default=None, help="Database host (overrides profile)")
@click.option("--database", default=None, help="Database (overrides profile)")
@click.option("--user", default=None, help="User (overrides profile)")
@click.option("--password", default=None, help="Password (overrides profile)")
@click.option("--auth-type", default=None, type=click.Choice(["password", "iam", "secrets_manager"]),
              help="Auth method (overrides profile)")
@click.option("--secret-arn", default=None, help="Secrets Manager ARN (overrides profile)")
@click.option("--staging-bucket", default=None, help="S3 staging bucket (overrides profile)")
@click.option("--iam-role", default=None, help="IAM role ARN (overrides profile)")
@click.option("--region", default=None, help="AWS region (overrides profile)")
@click.option("--verbose", is_flag=True, help="Show detailed output")
def export(query, destination, fmt, profile, provider, host, database, user, password,
           auth_type, secret_arn, staging_bucket, iam_role, region, verbose):
    """Export query results from a database to S3 or local file."""
    params = resolve_cli_connection_params(
        profile, host, database, user, password,
        staging_bucket, iam_role, region,
        auth_type=auth_type, secret_arn=secret_arn,
        provider=provider,
    )

    err = validate_connection_params(params)
    if err:
        click.echo(f"Error: {err}", err=True)
        sys.exit(1)

    if verbose:
        click.echo(f"Host: {params['host']}")
        click.echo(f"Database: {params['database']}")
        click.echo(f"Auth: {params['auth_type']}")
        click.echo(f"Query: {query}")
        click.echo(f"Destination: {destination}")
        click.echo(f"Format: {fmt}")

    is_s3 = destination.startswith("s3://")
    print_connection_context(params["host"], params["database"],
                             params["auth_type"], params["profile_name"])
    start = time.perf_counter()

    # --- PostgreSQL path: COPY protocol (no S3 staging needed) ---
    if params.get("provider") == "postgresql":
        from .config import make_pg_connection, make_pg_engine

        conn = make_pg_connection(params)
        engine = make_pg_engine()

        click.echo("Exporting via COPY TO STDOUT (PostgreSQL)...")
        result = engine.read_bulk(conn, query)
        table = result.table
        rows = result.rows
        conn.close()

        if fmt == "parquet":
            import pyarrow.parquet as pq
            pq.write_table(table, destination)
        elif fmt == "csv":
            table.to_pandas().to_csv(destination, index=False)

        elapsed = time.perf_counter() - start
        click.echo(f"Exported {rows:,} rows in {elapsed:.2f}s → {destination}")
        return

    # --- MySQL path: cursor fetch → Arrow (no S3 staging needed) ---
    if params.get("provider") == "mysql":
        from .config import make_mysql_connection, make_mysql_engine

        conn = make_mysql_connection(params)
        engine = make_mysql_engine()

        click.echo("Exporting via cursor fetch (MySQL)...")
        result = engine.read_bulk(conn, query)
        table = result.table
        rows = result.rows
        conn.close()

        if fmt == "parquet":
            import pyarrow.parquet as pq
            pq.write_table(table, destination)
        elif fmt == "csv":
            table.to_pandas().to_csv(destination, index=False)

        elapsed = time.perf_counter() - start
        click.echo(f"Exported {rows:,} rows in {elapsed:.2f}s → {destination}")
        return

    # --- Redshift path ---
    if is_s3 and params["staging_iam_role"]:
        # S3 destination: UNLOAD directly to the destination path.
        from arrowjet.providers.redshift import RedshiftProvider
        from arrowjet.staging.config import StagingConfig

        click.echo("Exporting via UNLOAD (direct to S3)...")

        conn = make_raw_connection(params)
        dest_path = destination.rstrip("/") + "/"

        config = StagingConfig(
            bucket=params["staging_bucket"] or dest_path.split("/")[2],
            iam_role=params["staging_iam_role"],
            region=params["staging_region"],
        )
        provider = RedshiftProvider(config)
        export_cmd = provider.build_export_sql(query=query, staging_path=dest_path)

        cursor = conn.cursor()
        try:
            cursor.execute(export_cmd.sql)
        except KeyboardInterrupt:
            click.echo("\nCancelling...", err=True)
            try:
                cursor.cancel()
            except Exception:
                pass
            conn.close()
            click.echo(
                f"Warning: UNLOAD may have written partial files to {destination}\n"
                f"Check and clean up manually if needed: aws s3 ls {destination}",
                err=True,
            )
            raise SystemExit(1)
        conn.close()

        elapsed = time.perf_counter() - start
        click.echo(f"Exported to {destination} in {elapsed:.2f}s")

    else:
        # Local destination: fetch → write locally
        if params["staging_bucket"] and params["staging_iam_role"]:
            conn = make_arrowjet_connection(params)
            click.echo("Exporting via UNLOAD (bulk mode)...")
            result = conn.read_bulk(query)
            table = result.table
            rows = result.rows
        else:
            conn = make_arrowjet_connection(params)
            click.echo("Exporting via direct fetch (safe mode)...")
            table = conn.fetch_arrow_table(query)
            rows = table.num_rows

        conn.close()

        if fmt == "parquet":
            import pyarrow.parquet as pq
            pq.write_table(table, destination)
        elif fmt == "csv":
            table.to_pandas().to_csv(destination, index=False)

        elapsed = time.perf_counter() - start
        click.echo(f"Exported {rows:,} rows in {elapsed:.2f}s → {destination}")
