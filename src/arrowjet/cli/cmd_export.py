"""
arrowjet export — export data from Redshift to S3 or local file.

Examples:
    arrowjet export --query "SELECT * FROM sales" --to s3://bucket/sales/
    arrowjet export --query "SELECT * FROM sales" --to ./sales.parquet
    arrowjet export --query "SELECT * FROM sales" --to ./sales.csv --format csv
"""

import os
import sys
import time
import click

from .config import get_profile, resolve_option


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

    import arrowjet as arrowjet

    is_s3 = destination.startswith("s3://")

    if is_s3 and staging_bucket and iam_role:
        # Bulk mode: UNLOAD → S3 → Parquet → write to destination
        conn = arrowjet.connect(
            host=host, database=database, user=user, password=password,
            staging_bucket=staging_bucket, staging_iam_role=iam_role,
            staging_region=region,
        )
    else:
        # Safe mode: direct fetch
        conn = arrowjet.connect(
            host=host, database=database, user=user, password=password,
        )

    try:
        start = time.perf_counter()

        if conn.has_bulk and is_s3:
            # Use bulk read for S3 destinations
            click.echo(f"Exporting via UNLOAD (bulk mode)...")
            result = conn.read_bulk(query)
            table = result.table
            rows = result.rows
        else:
            # Use safe mode
            click.echo(f"Exporting via direct fetch (safe mode)...")
            table = conn.fetch_arrow_table(query)
            rows = table.num_rows

        elapsed = time.perf_counter() - start

        # Write output
        if fmt == "parquet":
            import pyarrow.parquet as pq
            if is_s3:
                import pyarrow.fs as pafs
                s3fs = pafs.S3FileSystem(region=region)
                # Strip s3:// prefix for pyarrow
                s3_path = destination.replace("s3://", "")
                if not s3_path.endswith(".parquet"):
                    s3_path = s3_path.rstrip("/") + "/export.parquet"
                pq.write_table(table, s3_path, filesystem=s3fs)
            else:
                pq.write_table(table, destination)
        elif fmt == "csv":
            df = table.to_pandas()
            df.to_csv(destination, index=False)

        total = time.perf_counter() - start
        click.echo(f"Exported {rows:,} rows in {total:.2f}s → {destination}")

    finally:
        conn.close()
