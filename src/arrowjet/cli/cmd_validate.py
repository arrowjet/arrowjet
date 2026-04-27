"""
arrowjet validate - validate a database table.

Core: row count, schema, sample data.
Pro (when installed): adds drift detection, schema change history, suggested fixes.

Examples:
    arrowjet validate --table sales --row-count
    arrowjet validate --table sales --schema
    arrowjet validate --table sales --sample
    arrowjet validate --table sales --schema --suggest-fix   # Pro: show fix SQL
    arrowjet validate --table sales --schema --apply-fix     # Pro: apply safe fixes
"""

import sys
import click

from .config import (
    resolve_cli_connection_params,
    validate_connection_params,
    print_connection_context,
    make_arrowjet_connection,
    make_pg_connection,
    make_mysql_connection,
)

# Validate hook registry - Pro registers enrichment callbacks here
_validate_hooks = []


def register_validate_hook(hook_fn):
    """
    Register a validation enrichment hook.

    Called by Pro to add drift detection, schema change history, etc.
    Hook signature: hook_fn(conn, table, schema_name, provider, flags)
    where flags is a dict with suggest_fix, apply_fix, etc.
    """
    _validate_hooks.append(hook_fn)


@click.command()
@click.option("--table", required=True, help="Table name to validate")
@click.option("--schema-name", default="public", help="Schema name")
@click.option("--row-count", is_flag=True, help="Show row count")
@click.option("--schema", "show_schema", is_flag=True, help="Show table schema")
@click.option("--sample", is_flag=True, help="Show sample rows")
@click.option("--sample-rows", default=5, help="Number of sample rows")
@click.option("--suggest-fix", is_flag=True, help="Show suggested schema fixes (Pro)")
@click.option("--apply-fix", is_flag=True, help="Apply safe schema fixes (Pro)")
@click.option("--provider", default=None, type=click.Choice(["redshift", "postgresql", "mysql"]),
              help="Database provider")
@click.option("--profile", default=None, help="Config profile name")
@click.option("--host", default=None)
@click.option("--database", default=None)
@click.option("--user", default=None)
@click.option("--password", default=None)
@click.option("--auth-type", default=None, type=click.Choice(["password", "iam", "secrets_manager"]),
              help="Auth method (overrides profile)")
@click.option("--secret-arn", default=None, help="Secrets Manager ARN (overrides profile)")
def validate(table, schema_name, row_count, show_schema, sample, sample_rows,
             suggest_fix, apply_fix, provider,
             profile, host, database, user, password, auth_type, secret_arn):
    """Validate a database table (row count, schema, sample data)."""
    params = resolve_cli_connection_params(
        profile, host, database, user, password,
        staging_bucket=None, iam_role=None, region=None,
        auth_type=auth_type, secret_arn=secret_arn,
        provider=provider,
    )

    err = validate_connection_params(params)
    if err:
        click.echo(f"Error: {err}", err=True)
        sys.exit(1)

    resolved_provider = params.get("provider", "redshift")

    # Default: show everything if no flags specified
    if not row_count and not show_schema and not sample and not suggest_fix and not apply_fix:
        row_count = show_schema = sample = True

    # Create connection based on provider
    if resolved_provider == "postgresql":
        conn = make_pg_connection(params)
    elif resolved_provider == "mysql":
        conn = make_mysql_connection(params)
    else:
        conn = make_arrowjet_connection(params)

    try:
        print_connection_context(params["host"], params["database"],
                                 params["auth_type"], params["profile_name"])
        full_table = f"{schema_name}.{table}" if resolved_provider != "mysql" else table
        click.echo(f"Table: {full_table}")

        cursor = conn.cursor()

        if row_count:
            cursor.execute(f"SELECT COUNT(*) AS cnt FROM {full_table}")
            count = cursor.fetchone()[0]
            click.echo(f"Rows: {count:,}")

        if show_schema:
            if resolved_provider == "postgresql":
                cursor.execute(
                    "SELECT column_name, data_type, is_nullable "
                    "FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s "
                    "ORDER BY ordinal_position",
                    (schema_name, table),
                )
            elif resolved_provider == "mysql":
                cursor.execute(
                    "SELECT column_name, data_type, is_nullable "
                    "FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s "
                    "ORDER BY ordinal_position",
                    (params["database"], table),
                )
            else:
                # Redshift - use arrowjet connection's get_columns
                cols = conn.get_columns(table, schema=schema_name)
                click.echo(f"\nSchema ({len(cols)} columns):")
                for c in cols:
                    nullable = "NULL" if c["nullable"] else "NOT NULL"
                    click.echo(f"  {c['name']}: {c['type']} {nullable}")
                cursor = None  # skip the generic path

            if cursor and cursor.description:
                rows = cursor.fetchall()
                click.echo(f"\nSchema ({len(rows)} columns):")
                for row in rows:
                    name, dtype, nullable = row[0], row[1], row[2]
                    null_str = "NULL" if nullable == "YES" else "NOT NULL"
                    click.echo(f"  {name}: {dtype} {null_str}")

        if sample:
            cursor = conn.cursor() if not cursor else cursor
            cursor.execute(f"SELECT * FROM {full_table} LIMIT {sample_rows}")
            if cursor.description:
                col_names = [d[0] for d in cursor.description]
                rows = cursor.fetchall()
                click.echo(f"\nSample ({len(rows)} rows):")
                import pandas as pd
                df = pd.DataFrame(rows, columns=col_names)
                click.echo(df.to_string(index=False))

        # Fire Pro hooks (drift detection, schema change, fixes)
        if _validate_hooks:
            flags = {
                "suggest_fix": suggest_fix,
                "apply_fix": apply_fix,
                "show_schema": show_schema,
            }
            for hook in _validate_hooks:
                try:
                    hook(conn, table, schema_name, resolved_provider, flags)
                except Exception as e:
                    click.echo(f"  Pro hook error: {e}", err=True)
        elif suggest_fix or apply_fix:
            click.echo("\n--suggest-fix and --apply-fix require arrowjet-pro.")
            click.echo("Install with: pip install arrowjet-pro")

    finally:
        conn.close()
