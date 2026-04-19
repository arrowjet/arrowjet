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

import os
import sys
import time
import click

from .config import get_profile, resolve_option, print_connection_context


@click.command()
@click.option("--query", required=True, help="SQL query to export")
@click.option("--to", "destination", required=True, help="Destination: s3://bucket/path or local file path")
@click.option("--format", "fmt", default="parquet", type=click.Choice(["parquet", "csv"]), help="Output format")
@click.option("--profile", default=None, help="Config profile name")
@click.option("--host", default=None, help="Redshift host (overrides profile)")
@click.option("--database", default=None, help="Database (overrides profile)")
@click.option("--user", default=None, help="User (overrides profile)")
@click.option("--password", default=None, help="Password (overrides profile)")
@click.option("--staging-bucket", default=None, help="S3 staging bucket (overrides profile)")
@click.option("--iam-role", default=None, help="IAM role ARN (overrides profile)")
@click.option("--region", default=None, help="AWS region (overrides profile)")
@click.option("--verbose", is_flag=True, help="Show detailed output")
def export(query, destination, fmt, profile, host, database, user, password,
           staging_bucket, iam_role, region, verbose):
    """Export query results from Redshift to S3 or local file."""
    prof = get_profile(profile)

    host = resolve_option(host, "host", "REDSHIFT_HOST", prof)
    database = resolve_option(database, "database", "REDSHIFT_DATABASE", prof) or "dev"
    user = resolve_option(user, "user", "REDSHIFT_USER", prof) or "awsuser"
    password = resolve_option(password, "password", "REDSHIFT_PASS", prof)
    staging_bucket = resolve_option(staging_bucket, "staging_bucket", "STAGING_BUCKET", prof)
    iam_role = resolve_option(iam_role, "staging_iam_role", "STAGING_IAM_ROLE", prof)
    region = resolve_option(region, "staging_region", "STAGING_REGION", prof) or "us-east-1"

    if not host or not password:
        click.echo("Error: Redshift host and password required. Run 'arrowjet configure' or set env vars.", err=True)
        sys.exit(1)

    if verbose:
        click.echo(f"Host: {host}")
        click.echo(f"Database: {database}")
        click.echo(f"Query: {query}")
        click.echo(f"Destination: {destination}")
        click.echo(f"Format: {fmt}")

    is_s3 = destination.startswith("s3://")
    print_connection_context(host, database, profile)
    start = time.perf_counter()

    if is_s3 and iam_role:
        # S3 destination: UNLOAD directly to the destination path.
        # Data goes Redshift → S3 only. No roundtrip through the client.
        import redshift_connector
        from arrowjet.providers.redshift import RedshiftProvider
        from arrowjet.staging.config import StagingConfig

        click.echo("Exporting via UNLOAD (direct to S3)...")

        conn = redshift_connector.connect(
            host=host, port=5439, database=database,
            user=user, password=password,
        )
        conn.autocommit = True

        # Ensure destination ends with /
        dest_path = destination.rstrip("/") + "/"

        config = StagingConfig(
            bucket=staging_bucket or dest_path.split("/")[2],
            iam_role=iam_role,
            region=region,
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
        import arrowjet

        if staging_bucket and iam_role:
            conn = arrowjet.connect(
                host=host, database=database, user=user, password=password,
                staging_bucket=staging_bucket, staging_iam_role=iam_role,
                staging_region=region,
            )
            click.echo("Exporting via UNLOAD (bulk mode)...")
            result = conn.read_bulk(query)
            table = result.table
            rows = result.rows
        else:
            conn = arrowjet.connect(
                host=host, database=database, user=user, password=password,
            )
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
