"""
arrowjet import — load data into Redshift via COPY.

For S3 sources: COPY runs directly from S3 to Redshift — no client roundtrip.
For local files: upload to staging S3, then COPY into Redshift.

Examples:
    arrowjet import --from s3://bucket/data/ --to my_table
    arrowjet import --from ./data.parquet --to my_table
    arrowjet import --from ./data.parquet --to myschema.my_table
"""

import sys
import time
import click

from .config import get_profile, resolve_option, print_connection_context


@click.command(name="import")
@click.option("--from", "source", required=True, help="Source: s3://bucket/path or local file path")
@click.option("--to", "target_table", required=True, help="Target Redshift table (schema.table or table)")
@click.option("--profile", default=None, help="Config profile name")
@click.option("--host", default=None, help="Redshift host (overrides profile)")
@click.option("--database", default=None, help="Database (overrides profile)")
@click.option("--user", default=None, help="User (overrides profile)")
@click.option("--password", default=None, help="Password (overrides profile)")
@click.option("--staging-bucket", default=None, help="S3 staging bucket (required for local files)")
@click.option("--iam-role", default=None, help="IAM role ARN (overrides profile)")
@click.option("--region", default=None, help="AWS region (overrides profile)")
@click.option("--verbose", is_flag=True, help="Show detailed output")
def import_cmd(source, target_table, profile, host, database, user, password,
               staging_bucket, iam_role, region, verbose):
    """Load data into Redshift from S3 or a local Parquet/CSV file."""
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

    if not iam_role:
        click.echo("Error: IAM role ARN required for COPY. Set staging_iam_role in your profile.", err=True)
        sys.exit(1)

    print_connection_context(host, database, profile)

    is_s3 = source.startswith("s3://")
    start = time.perf_counter()

    if is_s3:
        # S3 source: COPY runs directly from S3 to Redshift — no client roundtrip
        import redshift_connector
        from arrowjet.providers.redshift import RedshiftProvider
        from arrowjet.staging.config import StagingConfig

        click.echo(f"Importing via COPY (direct from S3) → {target_table}...")

        conn = redshift_connector.connect(
            host=host, port=5439, database=database,
            user=user, password=password,
        )
        conn.autocommit = False

        config = StagingConfig(
            bucket=source.replace("s3://", "").split("/")[0],
            iam_role=iam_role,
            region=region,
        )
        provider = RedshiftProvider(config)
        import_cmd_obj = provider.build_import_sql(
            target_table=target_table,
            staging_path=source.rstrip("/") + "/",
        )

        if verbose:
            click.echo(f"SQL: {import_cmd_obj.sql}")

        cursor = conn.cursor()
        try:
            cursor.execute(import_cmd_obj.sql)
            conn.commit()
        except KeyboardInterrupt:
            click.echo("\nCancelling...", err=True)
            conn.rollback()
            conn.close()
            raise SystemExit(1)
        except Exception as e:
            conn.rollback()
            conn.close()
            click.echo(f"Error: COPY failed — {e}", err=True)
            sys.exit(1)

        conn.close()
        elapsed = time.perf_counter() - start
        click.echo(f"Imported to {target_table} in {elapsed:.2f}s")

    else:
        # Local file: upload to staging S3, then COPY
        if not staging_bucket:
            click.echo("Error: --staging-bucket required for local file imports.", err=True)
            sys.exit(1)

        import arrowjet
        import pyarrow.parquet as pq

        click.echo(f"Importing {source} → {target_table} via staging S3...")

        # Read local file
        if source.endswith(".parquet"):
            table = pq.read_table(source)
        elif source.endswith(".csv"):
            import pandas as pd
            import pyarrow as pa
            table = pa.Table.from_pandas(pd.read_csv(source))
        else:
            click.echo("Error: only .parquet and .csv files supported for local import.", err=True)
            sys.exit(1)

        conn = arrowjet.connect(
            host=host, database=database, user=user, password=password,
            staging_bucket=staging_bucket, staging_iam_role=iam_role,
            staging_region=region,
        )

        try:
            result = conn.write_bulk(table, target_table)
            elapsed = time.perf_counter() - start
            click.echo(f"Imported {result.rows:,} rows to {target_table} in {elapsed:.2f}s")
        finally:
            conn.close()
