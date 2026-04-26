"""
Redshift authentication  - IAM, Secrets Manager, and password.

Resolves Redshift credentials into a ResolvedCredentials object
that works with any DBAPI driver (redshift_connector, ADBC, psycopg2).

Usage:
    from arrowjet.auth.redshift import resolve_credentials

    # IAM auth (provisioned cluster)
    creds = resolve_credentials(
        host="my-cluster.xxx.us-east-1.redshift.amazonaws.com",
        auth_type="iam",
        db_user="etl_user",
    )

    # IAM auth (serverless)
    creds = resolve_credentials(
        host="my-wg.123.us-east-1.redshift-serverless.amazonaws.com",
        auth_type="iam",
    )

    # Secrets Manager
    creds = resolve_credentials(
        host="my-cluster.xxx.us-east-1.redshift.amazonaws.com",
        auth_type="secrets_manager",
        secret_arn="arn:aws:secretsmanager:us-east-1:123:secret:my-secret",
    )

    # Password (passthrough)
    creds = resolve_credentials(
        host="my-cluster.xxx.us-east-1.redshift.amazonaws.com",
        user="admin",
        password="secret",
    )

    # Use with any driver
    conn = redshift_connector.connect(**creds.as_kwargs())
    conn = adbc_driver_postgresql.dbapi.connect(creds.as_uri())
"""

from __future__ import annotations

import json
import logging
import re
from typing import List, Optional

from .base import ResolvedCredentials

logger = logging.getLogger(__name__)

# Default Redshift port
_DEFAULT_PORT = 5439
_DEFAULT_DATABASE = "dev"


def resolve_credentials(
    host: str,
    auth_type: str = "password",
    *,
    port: int = _DEFAULT_PORT,
    database: str = _DEFAULT_DATABASE,
    user: Optional[str] = None,
    password: Optional[str] = None,
    # IAM options
    db_user: Optional[str] = None,
    db_groups: Optional[List[str]] = None,
    auto_create: bool = False,
    duration_seconds: int = 900,
    # Secrets Manager options
    secret_arn: Optional[str] = None,
    # AWS options
    region: Optional[str] = None,
    profile: Optional[str] = None,
) -> ResolvedCredentials:
    """
    Resolve Redshift credentials for any auth method.

    Parameters
    ----------
    host : str
        Redshift cluster or serverless endpoint hostname.
    auth_type : str
        One of "password", "iam", or "secrets_manager". Default "password".
    port : int
        Redshift port. Default 5439.
    database : str
        Database name. Default "dev".
    user : str, optional
        Database user (required for password auth).
    password : str, optional
        Database password (required for password auth).
    db_user : str, optional
        Redshift database user for IAM auth. Falls back to `user` if not set.
    db_groups : list of str, optional
        Redshift database groups for IAM auth (provisioned only).
    auto_create : bool
        Auto-create database user on IAM auth (provisioned only).
    duration_seconds : int
        Lifetime of temporary IAM credentials (900-3600).
    secret_arn : str, optional
        Secrets Manager secret ARN (required for secrets_manager auth).
    region : str, optional
        AWS region. Inferred from endpoint if not provided.
    profile : str, optional
        AWS profile name for boto3 session.

    Returns
    -------
    ResolvedCredentials
        Credentials ready for any DBAPI driver.
    """
    auth_type = auth_type.lower().strip()

    if auth_type == "password":
        return _resolve_password(host, port, database, user, password)

    if auth_type == "iam":
        return _resolve_iam(
            host=host,
            port=port,
            database=database,
            db_user=db_user or user,
            db_groups=db_groups,
            auto_create=auto_create,
            duration_seconds=duration_seconds,
            region=region,
            profile=profile,
        )

    if auth_type == "secrets_manager":
        return _resolve_secrets_manager(
            host=host,
            port=port,
            database=database,
            secret_arn=secret_arn,
            region=region,
            profile=profile,
        )

    raise ValueError(
        f"Unknown auth_type: {auth_type!r}. "
        f"Expected one of: 'password', 'iam', 'secrets_manager'"
    )


# ---------------------------------------------------------------------------
# Password auth
# ---------------------------------------------------------------------------


def _resolve_password(
    host: str,
    port: int,
    database: str,
    user: Optional[str],
    password: Optional[str],
) -> ResolvedCredentials:
    """Passthrough  - just validate and wrap."""
    if not user:
        raise ValueError("user is required for password authentication")
    return ResolvedCredentials(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password or "",
    )


# ---------------------------------------------------------------------------
# IAM auth
# ---------------------------------------------------------------------------


def _resolve_iam(
    host: str,
    port: int,
    database: str,
    db_user: Optional[str],
    db_groups: Optional[List[str]],
    auto_create: bool,
    duration_seconds: int,
    region: Optional[str],
    profile: Optional[str],
) -> ResolvedCredentials:
    """Resolve IAM credentials for provisioned or serverless Redshift."""
    if not host:
        raise ValueError("host is required for IAM authentication")

    if not (900 <= duration_seconds <= 3600):
        raise ValueError(
            f"duration_seconds must be between 900 and 3600, got {duration_seconds}"
        )

    effective_region = region or infer_region(host)
    if not effective_region:
        raise ValueError(
            f"Cannot infer AWS region from host {host!r}. "
            f"Provide the 'region' parameter explicitly."
        )

    session = _get_boto3_session(region=effective_region, profile=profile)

    if is_serverless(host):
        if not db_user:
            raise ValueError(
                "db_user is required for Redshift Serverless IAM auth"
            )
        creds = _get_serverless_credentials(session, host, database)
    else:
        if not db_user:
            raise ValueError(
                "db_user (or user) is required for IAM authentication. "
                "Provide it via the 'db_user' or 'user' parameter."
            )
        creds = _get_provisioned_credentials(
            session, host, db_user, database,
            db_groups=db_groups,
            auto_create=auto_create,
            duration_seconds=duration_seconds,
        )

    logger.info("IAM credentials resolved for %s", host)

    return ResolvedCredentials(
        host=host,
        port=port,
        database=database,
        user=creds["user"],
        password=creds["password"],
    )


def _get_provisioned_credentials(
    session,
    host: str,
    db_user: str,
    database: str,
    *,
    db_groups: Optional[List[str]] = None,
    auto_create: bool = False,
    duration_seconds: int = 900,
) -> dict:
    """Call GetClusterCredentials for a provisioned Redshift cluster."""
    client = session.client("redshift")
    cluster_id = extract_cluster_identifier(host)

    kwargs: dict = {
        "ClusterIdentifier": cluster_id,
        "DbUser": db_user,
        "DbName": database,
        "DurationSeconds": duration_seconds,
        "AutoCreate": auto_create,
    }
    if db_groups:
        kwargs["DbGroups"] = db_groups

    response = client.get_cluster_credentials(**kwargs)
    return {
        "user": response["DbUser"],
        "password": response["DbPassword"],
    }


def _get_serverless_credentials(
    session,
    host: str,
    database: str,
) -> dict:
    """Call GetCredentials for a Redshift Serverless workgroup."""
    client = session.client("redshift-serverless")
    workgroup = extract_workgroup_name(host)

    response = client.get_credentials(
        workgroupName=workgroup,
        dbName=database,
    )
    return {
        "user": response["dbUser"],
        "password": response["dbPassword"],
    }


# ---------------------------------------------------------------------------
# Secrets Manager auth
# ---------------------------------------------------------------------------


def _resolve_secrets_manager(
    host: str,
    port: int,
    database: str,
    secret_arn: Optional[str],
    region: Optional[str],
    profile: Optional[str],
) -> ResolvedCredentials:
    """Resolve credentials from AWS Secrets Manager."""
    if not secret_arn:
        raise ValueError("secret_arn is required for Secrets Manager authentication")

    if not secret_arn.startswith("arn:aws:secretsmanager:"):
        raise ValueError(
            f"secret_arn must be a valid Secrets Manager ARN "
            f"(starting with 'arn:aws:secretsmanager:'), got {secret_arn!r}"
        )

    effective_region = region or infer_region(host)
    session = _get_boto3_session(region=effective_region, profile=profile)
    client = session.client("secretsmanager")

    response = client.get_secret_value(SecretId=secret_arn)
    secret = json.loads(response["SecretString"])

    # Redshift Secrets Manager secrets typically have:
    # username, password, host, port, dbname (or database), engine
    resolved_user = secret.get("username")
    resolved_password = secret.get("password", "")
    resolved_host = secret.get("host", host)
    resolved_port = int(secret.get("port", port))
    resolved_database = secret.get("dbname", secret.get("database", database))

    if not resolved_user:
        raise ValueError(
            "Secrets Manager secret does not contain a 'username' field"
        )

    logger.info("Secrets Manager credentials resolved (secret_arn=%s)", secret_arn)

    return ResolvedCredentials(
        host=resolved_host,
        port=resolved_port,
        database=resolved_database,
        user=resolved_user,
        password=resolved_password,
    )


# ---------------------------------------------------------------------------
# Host parsing utilities
# ---------------------------------------------------------------------------


def infer_region(host: str) -> Optional[str]:
    """
    Infer AWS region from a Redshift endpoint hostname.

    Provisioned: ``cluster-name.xxxx.region.redshift.amazonaws.com``
    Serverless:  ``workgroup-name.account-id.region.redshift-serverless.amazonaws.com``
    """
    match = re.search(
        r"\.([a-z]{2}-[a-z]+-\d+)\.redshift(?:-serverless)?\.amazonaws\.com", host
    )
    return match.group(1) if match else None


def is_serverless(host: str) -> bool:
    """Check if the host is a Redshift Serverless endpoint."""
    return "redshift-serverless" in host.lower()


def extract_cluster_identifier(host: str) -> str:
    """
    Extract the cluster identifier from a provisioned Redshift endpoint.

    ``my-cluster.xxxx.us-east-1.redshift.amazonaws.com`` -> ``my-cluster``
    """
    return host.split(".")[0]


def extract_workgroup_name(host: str) -> str:
    """
    Extract the workgroup name from a Redshift Serverless endpoint.

    ``my-workgroup.123456789.us-east-1.redshift-serverless.amazonaws.com``
    -> ``my-workgroup``
    """
    return host.split(".")[0]


# ---------------------------------------------------------------------------
# boto3 session helper
# ---------------------------------------------------------------------------


def _get_boto3_session(
    region: Optional[str] = None,
    profile: Optional[str] = None,
):
    """Create a boto3 session, raising a clear error if boto3 is missing."""
    try:
        import boto3
    except ImportError:
        raise ImportError(
            "boto3 is required for Redshift IAM and Secrets Manager authentication. "
            "Install it with: pip install boto3"
        ) from None

    kwargs: dict = {}
    if region:
        kwargs["region_name"] = region
    if profile:
        kwargs["profile_name"] = profile
    return boto3.Session(**kwargs)
