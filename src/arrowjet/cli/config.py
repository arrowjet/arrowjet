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
