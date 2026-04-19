"""
Shared test configuration — reads credentials from environment variables.

Local: source .env or export vars manually
GitHub Actions: set as repository secrets

Required env vars for integration tests:
  REDSHIFT_HOST
  REDSHIFT_PORT (default: 5439)
  REDSHIFT_DATABASE (default: dev)
  REDSHIFT_USER (default: awsuser)
  REDSHIFT_PASS
  STAGING_BUCKET
  STAGING_IAM_ROLE
  STAGING_REGION (default: us-east-1)

AWS credentials: via environment (AWS_ACCESS_KEY_ID etc) or IAM role.
"""

import os
import pytest
import redshift_connector

from arrowjet.staging.config import StagingConfig, CleanupPolicy


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def _has_aws_creds() -> bool:
    try:
        import boto3
        boto3.client("sts").get_caller_identity()
        return True
    except Exception:
        return False


def _has_redshift() -> bool:
    host = _env("REDSHIFT_HOST")
    if not host:
        return False
    try:
        conn = redshift_connector.connect(
            host=host,
            port=int(_env("REDSHIFT_PORT", "5439")),
            database=_env("REDSHIFT_DATABASE", "dev"),
            user=_env("REDSHIFT_USER", "awsuser"),
            password=_env("REDSHIFT_PASS"),
        )
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        conn.close()
        return True
    except Exception:
        return False


# Markers for conditional test execution
requires_aws = pytest.mark.skipif(
    not _has_aws_creds(),
    reason="AWS credentials not available (set AWS_ACCESS_KEY_ID or use IAM role)",
)

requires_redshift = pytest.mark.skipif(
    not (_has_aws_creds() and _has_redshift()),
    reason="Redshift not available (set REDSHIFT_HOST, REDSHIFT_PASS env vars)",
)


@pytest.fixture
def redshift_conn():
    """Provides a Redshift connection. Auto-closes after test."""
    conn = redshift_connector.connect(
        host=_env("REDSHIFT_HOST"),
        port=int(_env("REDSHIFT_PORT", "5439")),
        database=_env("REDSHIFT_DATABASE", "dev"),
        user=_env("REDSHIFT_USER", "awsuser"),
        password=_env("REDSHIFT_PASS"),
    )
    yield conn
    conn.close()


@pytest.fixture
def staging_config():
    """Provides a StagingConfig from environment variables."""
    return StagingConfig(
        bucket=_env("STAGING_BUCKET"),
        iam_role=_env("STAGING_IAM_ROLE"),
        region=_env("STAGING_REGION", "us-east-1"),
        prefix="redshift-adbc-test",
        cleanup_policy=CleanupPolicy.ALWAYS,
    )


@pytest.fixture
def staging_manager(staging_config):
    """Provides a StagingManager from environment variables."""
    from arrowjet.staging.manager import StagingManager
    return StagingManager(config=staging_config, cluster_id="test", database="dev")
