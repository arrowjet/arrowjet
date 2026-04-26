"""
CLI configuration — reads from ~/.arrowjet/config.yaml.

Stores connection profiles and defaults so users don't repeat
--host, --staging-bucket, etc. on every command.

Example ~/.arrowjet/config.yaml:
    default_profile: dev

    profiles:
      dev:
        host: my-cluster.region.redshift.amazonaws.com
        database: dev
        user: awsuser
        password: mypass
        staging_bucket: my-staging-bucket
        staging_iam_role: arn:aws:iam::123:role/RedshiftS3
        staging_region: us-east-1

      production:
        host: prod-cluster.region.redshift.amazonaws.com
        database: prod
        auth: iam
        user: etl_user
        staging_bucket: prod-staging
        staging_iam_role: arn:aws:iam::123:role/ProdRedshiftS3
        staging_region: us-east-1
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import yaml

CONFIG_DIR = Path.home() / ".arrowjet"
CONFIG_FILE = CONFIG_DIR / "config.yaml"


def load_config() -> dict:
    """Load config from ~/.arrowjet/config.yaml. Returns empty dict if not found."""
    if not CONFIG_FILE.exists():
        return {}
    with open(CONFIG_FILE) as f:
        return yaml.safe_load(f) or {}


def get_profile(profile_name: Optional[str] = None) -> dict:
    """Get a named profile, or the default profile."""
    cfg = load_config()
    profiles = cfg.get("profiles", {})

    if not profile_name:
        profile_name = cfg.get("default_profile", "default")

    return profiles.get(profile_name, {})


def save_config(config: dict) -> None:
    """Save config to ~/.arrowjet/config.yaml."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_FILE, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)


def resolve_option(cli_value, profile_key: str, env_var: str, profile: dict) -> Optional[str]:
    """Resolve an option: CLI flag > env var > profile config."""
    if cli_value:
        return cli_value
    env_val = os.environ.get(env_var)
    if env_val:
        return env_val
    return profile.get(profile_key)


def print_connection_context(host: str, database: str, auth_type: str = "password",
                             profile_name: Optional[str] = None) -> None:
    """Print the active connection context so users know which cluster they're talking to."""
    import click
    profile_label = f" [{profile_name}]" if profile_name else ""
    auth_label = f" ({auth_type})" if auth_type != "password" else ""
    short_host = host.split(".")[0] if host and "." in host else (host or "unknown")
    click.echo(f"Connected: {short_host} / {database}{auth_label}{profile_label}", err=False)


def resolve_cli_connection_params(profile, host, database, user, password,
                                  staging_bucket, iam_role, region, auth_type=None,
                                  secret_arn=None, provider=None) -> dict:
    """
    Resolve all connection parameters from CLI flags, env vars, and profile.

    Returns a dict with all parameters needed for arrowjet.connect() or
    resolve_credentials().
    """
    prof = get_profile(profile)

    # Determine provider: CLI flag > profile > default (redshift)
    resolved_provider = provider or prof.get("provider", "redshift")

    if resolved_provider == "postgresql":
        # PostgreSQL uses PG_* env vars
        resolved_host = resolve_option(host, "host", "PG_HOST", prof)
        resolved_database = resolve_option(database, "database", "PG_DATABASE", prof) or "dev"
        resolved_user = resolve_option(user, "user", "PG_USER", prof) or "postgres"
        resolved_password = resolve_option(password, "password", "PG_PASS", prof) or ""
        resolved_port = int(prof.get("port", os.environ.get("PG_PORT", "5432")))
    else:
        # Redshift uses REDSHIFT_* env vars
        resolved_host = resolve_option(host, "host", "REDSHIFT_HOST", prof)
        resolved_database = resolve_option(database, "database", "REDSHIFT_DATABASE", prof) or "dev"
        resolved_user = resolve_option(user, "user", "REDSHIFT_USER", prof) or "awsuser"
        resolved_password = resolve_option(password, "password", "REDSHIFT_PASS", prof) or ""
        resolved_port = int(prof.get("port", os.environ.get("REDSHIFT_PORT", "5439")))

    resolved_bucket = resolve_option(staging_bucket, "staging_bucket", "STAGING_BUCKET", prof)
    resolved_iam_role = resolve_option(iam_role, "staging_iam_role", "STAGING_IAM_ROLE", prof)
    resolved_region = resolve_option(region, "staging_region", "STAGING_REGION", prof) or "us-east-1"
    resolved_auth = auth_type or prof.get("auth", "password")
    resolved_secret_arn = resolve_option(secret_arn, "secret_arn", "REDSHIFT_AUTH_SECRET_ARN", prof)

    return {
        "provider": resolved_provider,
        "host": resolved_host,
        "port": resolved_port,
        "database": resolved_database,
        "user": resolved_user,
        "password": resolved_password,
        "auth_type": resolved_auth,
        "secret_arn": resolved_secret_arn,
        "staging_bucket": resolved_bucket,
        "staging_iam_role": resolved_iam_role,
        "staging_region": resolved_region,
        "profile": prof,
        "profile_name": profile,
    }


def validate_connection_params(params: dict) -> Optional[str]:
    """
    Validate that we have enough info to connect.
    Returns an error message string, or None if valid.
    """
    if not params["host"]:
        provider = params.get("provider", "redshift")
        if provider == "postgresql":
            return "PostgreSQL host required. Run 'arrowjet configure' or set PG_HOST."
        return "Redshift host required. Run 'arrowjet configure' or set REDSHIFT_HOST."

    auth = params["auth_type"]
    if auth == "password" and not params["password"]:
        provider = params.get("provider", "redshift")
        if provider == "postgresql":
            return "Password required. Use --password, set PG_PASS, or configure a profile."
        return "Password required for password auth. Use --password, set REDSHIFT_PASS, or switch to auth_type=iam."
    if auth == "secrets_manager" and not params["secret_arn"]:
        return "secret_arn required for Secrets Manager auth."

    return None


def make_arrowjet_connection(params: dict, need_staging: bool = False):
    """
    Create an arrowjet connection from resolved params.

    Uses arrowjet.connect() with auth_type — all auth flows go through
    the same path.
    """
    import arrowjet

    if need_staging and not (params["staging_bucket"] and params["staging_iam_role"]):
        import click
        click.echo("Error: staging_bucket and staging_iam_role required for bulk operations.", err=True)
        raise SystemExit(1)

    kwargs = dict(
        host=params["host"],
        database=params["database"],
        user=params["user"],
        password=params["password"],
        auth_type=params["auth_type"],
        aws_region=params["staging_region"],
    )

    if params["auth_type"] == "secrets_manager":
        kwargs["secret_arn"] = params["secret_arn"]
    if params["auth_type"] == "iam":
        kwargs["db_user"] = params["user"]

    if params["staging_bucket"] and params["staging_iam_role"]:
        kwargs["staging_bucket"] = params["staging_bucket"]
        kwargs["staging_iam_role"] = params["staging_iam_role"]
        kwargs["staging_region"] = params["staging_region"]

    return arrowjet.connect(**kwargs)


def make_raw_connection(params: dict):
    """
    Create a raw DBAPI connection (for S3-direct export/import where
    we don't need the full arrowjet connection, just cursor.execute()).

    Uses resolve_credentials for auth, then connects with redshift_connector.
    """
    from arrowjet.auth.redshift import resolve_credentials
    import redshift_connector

    creds = resolve_credentials(
        host=params["host"],
        port=5439,
        database=params["database"],
        user=params["user"],
        password=params["password"],
        auth_type=params["auth_type"],
        db_user=params["user"] if params["auth_type"] == "iam" else None,
        secret_arn=params.get("secret_arn"),
        region=params["staging_region"],
    )

    conn = redshift_connector.connect(**creds.as_kwargs())
    conn.autocommit = True
    return conn


def make_pg_connection(params: dict):
    """
    Create a psycopg2 connection for PostgreSQL/Aurora/RDS.
    """
    import psycopg2

    conn = psycopg2.connect(
        host=params["host"],
        port=params.get("port", 5432),
        dbname=params["database"],
        user=params["user"],
        password=params["password"],
        connect_timeout=10,
    )
    return conn


def make_pg_engine():
    """Create a PostgreSQL Engine instance."""
    from arrowjet.engine import Engine
    return Engine(provider="postgresql")
