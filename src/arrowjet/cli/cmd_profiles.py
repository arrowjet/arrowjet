"""
arrowjet profiles - list configured connection profiles.

Examples:
    arrowjet profiles
    arrowjet profiles --verbose
"""

import click

from .config import load_config


@click.command()
@click.option("--verbose", is_flag=True, help="Show all profile details")
def profiles(verbose):
    """List configured connection profiles."""
    cfg = load_config()
    profile_list = cfg.get("profiles", {})
    default_name = cfg.get("default_profile", "default")

    if not profile_list:
        click.echo("No profiles configured.")
        click.echo("Run 'arrowjet configure' to create one.")
        return

    click.echo(f"Profiles ({len(profile_list)}):\n")

    for name, profile in profile_list.items():
        is_default = name == default_name
        marker = " (default)" if is_default else ""
        provider = profile.get("provider", "redshift")
        host = profile.get("host", "?")
        database = profile.get("database", "?")
        auth = profile.get("auth", profile.get("auth_type", "password"))

        click.echo(f"  {name}{marker}")
        click.echo(f"    provider: {provider}")
        click.echo(f"    host:     {host}")
        click.echo(f"    database: {database}")
        click.echo(f"    auth:     {auth}")

        if verbose:
            user = profile.get("user", "")
            if user:
                click.echo(f"    user:     {user}")
            bucket = profile.get("staging_bucket", "")
            if bucket:
                click.echo(f"    bucket:   {bucket}")
            iam_role = profile.get("staging_iam_role", "")
            if iam_role:
                click.echo(f"    iam_role: {iam_role}")
            region = profile.get("staging_region", profile.get("region", ""))
            if region:
                click.echo(f"    region:   {region}")

        click.echo()
