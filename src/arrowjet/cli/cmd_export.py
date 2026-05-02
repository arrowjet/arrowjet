"""
arrowjet export  - export data from Redshift to S3 or local file.

S3 destination: UNLOAD runs directly to the destination  - no roundtrip
through the client machine. Fast, efficient, data stays in AWS.

Local destination: UNLOAD -> staging S3 -> download -> local file.

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
@click.option("--query", default=None, help="SQL query to export")
@click.option("--from-file", "from_file", default=None, type=click.Path(exists=True),
              help="Read SQL query from a file (mutually exclusive with --query)")
@click.option("--to", "destination", required=True, help="Destination: s3://bucket/path or local file path")
@click.option("--format", "fmt", default="parquet", type=click.Choice(["parquet", "csv", "iceberg"]), help="Output format")
@click.option("--iceberg-table", default=None, help="Iceberg table name (e.g., analytics.orders). Required when --format iceberg")
@click.option("--iceberg-mode", default="append", type=click.Choice(["append", "overwrite"]), help="Iceberg write mode")
@click.option("--iceberg-catalog", default="sql", type=click.Choice(["sql", "rest", "glue"]), help="Iceberg catalog type")
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
@click.option("--dry-run", is_flag=True, help="Show the SQL that would be executed without running it")
@click.option("--verbose", is_flag=True, help="Show detailed output")
def export(query, from_file, destination, fmt, iceberg_table, iceberg_mode, iceberg_catalog,
           profile, provider, host, database, user, password,
           auth_type, secret_arn, staging_bucket, iam_role, region, dry_run, verbose):
    """Export query results from a database to S3 or local file."""
    # Resolve query from --query or --from-file
    if from_file and query:
        click.echo("Error: --query and --from-file are mutually exclusive.", err=True)
        sys.exit(1)
    if from_file:
        from pathlib import Path
        query = Path(from_file).read_text().strip()
        if not query:
            click.echo(f"Error: file {from_file} is empty.", err=True)
            sys.exit(1)
        if verbose:
            click.echo(f"Query loaded from: {from_file}")
    if not query:
        click.echo("Error: --query or --from-file is required.", err=True)
        sys.exit(1)
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

    # --- Dry-run mode: show SQL without executing ---
    if dry_run:
        _handle_dry_run(query, destination, params, is_s3)
        return

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
        elif fmt == "iceberg":
            _write_iceberg(table, destination, iceberg_table, iceberg_mode, iceberg_catalog)

        elapsed = time.perf_counter() - start
        click.echo(f"Exported {rows:,} rows in {elapsed:.2f}s -> {destination}")
        return

    # --- MySQL path: cursor fetch -> Arrow (no S3 staging needed) ---
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
        elif fmt == "iceberg":
            _write_iceberg(table, destination, iceberg_table, iceberg_mode, iceberg_catalog)

        elapsed = time.perf_counter() - start
        click.echo(f"Exported {rows:,} rows in {elapsed:.2f}s -> {destination}")
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
            click.echo("  UNLOAD running...", nl=False)
            cursor.execute(export_cmd.sql)
            click.echo(" done.")
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

        # Get row count from S3 Parquet metadata
        row_count = _count_s3_parquet_rows(dest_path, params.get("staging_region", "us-east-1"))
        conn.close()

        elapsed = time.perf_counter() - start
        if row_count is not None:
            click.echo(f"Exported {row_count:,} rows to {destination} in {elapsed:.2f}s")
        else:
            click.echo(f"Exported to {destination} in {elapsed:.2f}s")

    else:
        # Local destination: fetch -> write locally
        if params["staging_bucket"] and params["staging_iam_role"]:
            conn = make_arrowjet_connection(params)
            click.echo("Exporting via UNLOAD (bulk mode)...")
            click.echo("  UNLOAD + download running...", nl=False)
            result = conn.read_bulk(query)
            click.echo(" done.")
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
        elif fmt == "iceberg":
            _write_iceberg(table, destination, iceberg_table, iceberg_mode, iceberg_catalog)

        elapsed = time.perf_counter() - start
        click.echo(f"Exported {rows:,} rows in {elapsed:.2f}s -> {destination}")


def _handle_dry_run(query, destination, params, is_s3):
    """Show what would be executed without actually running it."""
    provider_name = params.get("provider", "redshift")

    click.echo("DRY RUN - no data will be exported\n")
    click.echo(f"Provider:    {provider_name}")
    click.echo(f"Destination: {destination}")
    click.echo()

    if provider_name == "postgresql":
        copy_sql = f"COPY ({query}) TO STDOUT WITH (FORMAT csv, HEADER true)"
        click.echo("SQL (COPY TO STDOUT):")
        click.echo(f"  {copy_sql}")
    elif provider_name == "mysql":
        click.echo("SQL (cursor fetch):")
        click.echo(f"  {query}")
    elif is_s3 and params.get("staging_iam_role"):
        from arrowjet.providers.redshift import RedshiftProvider
        from arrowjet.staging.config import StagingConfig

        dest_path = destination.rstrip("/") + "/"
        config = StagingConfig(
            bucket=params.get("staging_bucket") or dest_path.split("/")[2],
            iam_role=params["staging_iam_role"],
            region=params.get("staging_region", "us-east-1"),
        )
        provider = RedshiftProvider(config)
        export_cmd = provider.build_export_sql(query=query, staging_path=dest_path)
        click.echo("SQL (UNLOAD direct to S3):")
        click.echo(f"  {export_cmd.sql}")
    else:
        click.echo("SQL (query):")
        click.echo(f"  {query}")

    click.echo()
    click.echo("Remove --dry-run to execute.")


def _count_s3_parquet_rows(s3_path, region="us-east-1"):
    """
    Count total rows across Parquet files at an S3 path using metadata only.

    Returns row count or None if unable to read metadata.
    """
    try:
        import pyarrow.parquet as pq
        import pyarrow.fs as pafs

        s3fs = pafs.S3FileSystem(region=region)
        s3_key = s3_path.replace("s3://", "")

        # List all parquet files
        selector = pafs.FileSelector(s3_key, recursive=False)
        file_infos = s3fs.get_file_info(selector)
        parquet_files = [f.path for f in file_infos
                         if f.type.name == "File" and f.path.endswith(".parquet")]

        if not parquet_files:
            return None

        total_rows = 0
        for pf in parquet_files:
            meta = pq.read_metadata(pf, filesystem=s3fs)
            total_rows += meta.num_rows

        return total_rows
    except Exception:
        return None


def _write_iceberg(table, warehouse, iceberg_table_name, mode, catalog_type):
    """Write an Arrow table to an Iceberg table via the arrowjet.iceberg module."""
    if not iceberg_table_name:
        click.echo(
            "Error: --iceberg-table is required when using --format iceberg.\n"
            "Example: --format iceberg --iceberg-table analytics.orders",
            err=True,
        )
        raise SystemExit(1)

    from arrowjet.iceberg import write_iceberg

    click.echo(f"Writing to Iceberg table '{iceberg_table_name}' ({mode})...")
    result = write_iceberg(
        table=table,
        table_name=iceberg_table_name,
        warehouse=warehouse,
        mode=mode,
        catalog_type=catalog_type,
    )
    click.echo(
        f"Iceberg write complete: {result.rows:,} rows to '{result.table_name}' "
        f"in {result.total_time_s}s"
    )
