"""
arrowjet configure — interactive setup for ~/.arrowjet/config.yaml.
"""

import click
from .config import load_config, save_config, CONFIG_FILE


@click.command()
@click.option("--profile", default="default", help="Profile name")
def configure(profile):
    """Set up a connection profile (saved to ~/.arrowjet/config.yaml)."""
    click.echo(f"Configuring profile: {profile}")
    click.echo(f"Config file: {CONFIG_FILE}\n")

    host = click.prompt("Redshift host", default="")
    database = click.prompt("Database", default="dev")
    user = click.prompt("User", default="awsuser")
    auth_method = click.prompt("Auth method", type=click.Choice(["password", "iam"]), default="password")
    if auth_method == "password":
        password = click.prompt("Password", hide_input=True, default="")
    else:
        password = ""
        click.echo("IAM auth: uses your AWS credentials (~/.aws/credentials or instance role). No password needed.")
    staging_bucket = click.prompt("S3 staging bucket", default="")
    staging_iam_role = click.prompt("IAM role ARN", default="")
    staging_region = click.prompt("AWS region", default="us-east-1")

    cfg = load_config()
    if "profiles" not in cfg:
        cfg["profiles"] = {}

    cfg["profiles"][profile] = {
        "host": host,
        "database": database,
        "user": user,
        "auth": auth_method,
        "password": password,
        "staging_bucket": staging_bucket,
        "staging_iam_role": staging_iam_role,
        "staging_region": staging_region,
    }

    if "default_profile" not in cfg:
        cfg["default_profile"] = profile

    save_config(cfg)
    click.echo(f"\nProfile '{profile}' saved to {CONFIG_FILE}")

    # Validate the connection
    click.echo("\nTesting connection...")
    try:
        import arrowjet
        conn = arrowjet.connect(
            host=host, database=database, user=user, password=password,
        )
        conn.close()
        click.echo("Connection successful.")
    except Exception as e:
        click.echo(f"Warning: Could not connect — {e}", err=True)
        click.echo("Profile saved. Fix the connection details and run 'arrowjet configure' again.")
