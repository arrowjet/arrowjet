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


def print_connection_context(host: str, database: str, profile_name: Optional[str] = None) -> None:
    """Print the active connection context so users know which cluster they're talking to."""
    import click
    profile_label = f" [{profile_name}]" if profile_name else ""
    # Shorten host for readability: show first segment only
    short_host = host.split(".")[0] if host and "." in host else (host or "unknown")
    click.echo(f"Connected: {short_host} / {database}{profile_label}", err=False)


def make_arrowjet_connection(host: str, database: str, user: str, password: str,
                              profile: dict, staging_bucket: Optional[str] = None,
                              staging_iam_role: Optional[str] = None,
                              staging_region: str = "us-east-1"):
    """Create an arrowjet connection, handling both password and IAM auth."""
    import arrowjet

    auth = profile.get("auth", "password")

    if auth == "iam":
        # IAM auth: use redshift_connector with iam=True, no password
        import redshift_connector
        import boto3

        # Get temp credentials via IAM
        client = boto3.client("redshift", region_name=staging_region)
        creds = client.get_cluster_credentials(
            DbUser=user,
            DbName=database,
            ClusterIdentifier=host.split(".")[0],
            AutoCreate=False,
        )
        password = creds["DbPassword"]
        user = creds["DbUser"]

    if staging_bucket and staging_iam_role:
        return arrowjet.connect(
            host=host, database=database, user=user, password=password,
            staging_bucket=staging_bucket, staging_iam_role=staging_iam_role,
            staging_region=staging_region,
        )
    else:
        return arrowjet.connect(
            host=host, database=database, user=user, password=password,
        )
