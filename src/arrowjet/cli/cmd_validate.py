"""
arrowjet validate — validate a Redshift table (row count, schema, sample).

Examples:
    arrowjet validate --table sales --row-count
    arrowjet validate --table sales --schema
    arrowjet validate --table sales --sample
"""

import sys
import click

from .config import get_profile, resolve_option


@click.command()
@click.option("--table", required=True, help="Table name to validate")
@click.option("--schema-name", default="public", help="Schema name")
@click.option("--row-count", is_flag=True, help="Show row count")
@click.option("--schema", "show_schema", is_flag=True, help="Show table schema")
@click.option("--sample", is_flag=True, help="Show sample rows")
@click.option("--sample-rows", default=5, help="Number of sample rows")
@click.option("--profile", default=None, help="Config profile name")
@click.option("--host", default=None)
@click.option("--database", default=None)
@click.option("--user", default=None)
@click.option("--password", default=None)
def validate(table, schema_name, row_count, show_schema, sample, sample_rows,
             profile, host, database, user, password):
    """Validate a Redshift table (row count, schema, sample data)."""
    prof = get_profile(profile)

    host = resolve_option(host, "host", "REDSHIFT_HOST", prof)
    database = resolve_option(database, "database", "REDSHIFT_DATABASE", prof) or "dev"
    user = resolve_option(user, "user", "REDSHIFT_USER", prof) or "awsuser"
    password = resolve_option(password, "password", "REDSHIFT_PASS", prof)

    if not host or not password:
        click.echo("Error: Redshift host and password required.", err=True)
        sys.exit(1)

    # Default: show everything if no flags specified
    if not row_count and not show_schema and not sample:
        row_count = show_schema = sample = True

    import arrowjet as arrowjet
    conn = arrowjet.connect(host=host, database=database, user=user, password=password)

    try:
        full_table = f"{schema_name}.{table}"
        click.echo(f"Table: {full_table}")

        if row_count:
            df = conn.fetch_dataframe(f"SELECT COUNT(*) AS cnt FROM {full_table}")
            click.echo(f"Rows: {df['cnt'].iloc[0]:,}")

        if show_schema:
            cols = conn.get_columns(table, schema=schema_name)
            click.echo(f"\nSchema ({len(cols)} columns):")
            for c in cols:
                nullable = "NULL" if c["nullable"] else "NOT NULL"
                click.echo(f"  {c['name']}: {c['type']} {nullable}")

        if sample:
            click.echo(f"\nSample ({sample_rows} rows):")
            df = conn.fetch_dataframe(f"SELECT * FROM {full_table} LIMIT {sample_rows}")
            click.echo(df.to_string(index=False))

    finally:
        conn.close()
