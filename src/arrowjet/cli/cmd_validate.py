"""
arrowjet validate — validate a Redshift table (row count, schema, sample).

Examples:
    arrowjet validate --table sales --row-count
    arrowjet validate --table sales --schema
    arrowjet validate --table sales --sample
"""

import sys
import click

from .config import (
    resolve_cli_connection_params,
    validate_connection_params,
    print_connection_context,
    make_arrowjet_connection,
)


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
@click.option("--auth-type", default=None, type=click.Choice(["password", "iam", "secrets_manager"]),
              help="Auth method (overrides profile)")
@click.option("--secret-arn", default=None, help="Secrets Manager ARN (overrides profile)")
def validate(table, schema_name, row_count, show_schema, sample, sample_rows,
             profile, host, database, user, password, auth_type, secret_arn):
    """Validate a Redshift table (row count, schema, sample data)."""
    params = resolve_cli_connection_params(
        profile, host, database, user, password,
        staging_bucket=None, iam_role=None, region=None,
        auth_type=auth_type, secret_arn=secret_arn,
    )

    err = validate_connection_params(params)
    if err:
        click.echo(f"Error: {err}", err=True)
        sys.exit(1)

    # Default: show everything if no flags specified
    if not row_count and not show_schema and not sample:
        row_count = show_schema = sample = True

    conn = make_arrowjet_connection(params)

    try:
        print_connection_context(params["host"], params["database"],
                                 params["auth_type"], params["profile_name"])
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
