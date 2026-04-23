"""
Integration tests for arrowjet.auth — real AWS + Redshift.

Parametrized across three axes:
  - auth_type: password, iam, secrets_manager
  - driver: redshift_connector, adbc
  - cluster: provisioned, serverless

Requires env vars: see .env.example
"""

import os
from typing import Optional

import pytest

import arrowjet
from arrowjet.auth.redshift import resolve_credentials


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def _has_aws() -> bool:
    try:
        import boto3
        boto3.client("sts").get_caller_identity()
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Connection helpers — one per driver
# ---------------------------------------------------------------------------


def _connect_redshift_connector(creds):
    import redshift_connector
    return redshift_connector.connect(**creds.as_kwargs())


def _connect_adbc(creds):
    import adbc_driver_postgresql.dbapi
    return adbc_driver_postgresql.dbapi.connect(creds.as_uri())


def _query_current_user(conn, driver: str) -> str:
    """Execute SELECT current_user and return the result, handling driver differences."""
    cursor = conn.cursor()
    cursor.execute("SELECT current_user")
    row = cursor.fetchone()
    # redshift_connector returns list, adbc returns tuple
    return row[0]


# ---------------------------------------------------------------------------
# Credential resolution helpers
# ---------------------------------------------------------------------------


def _resolve_for(
    auth_type: str,
    host: str,
    db_user: Optional[str] = None,
):
    """Resolve credentials for a given auth type and host."""
    kwargs = dict(
        host=host,
        port=int(_env("REDSHIFT_PORT", "5439")),
        database=_env("REDSHIFT_DATABASE", "dev"),
        auth_type=auth_type,
        region=_env("STAGING_REGION", "us-east-1"),
    )

    if auth_type == "password":
        kwargs["user"] = _env("REDSHIFT_USER", "awsuser")
        kwargs["password"] = _env("REDSHIFT_PASS")
    elif auth_type == "iam":
        kwargs["db_user"] = db_user or _env("REDSHIFT_USER", "awsuser")
    elif auth_type == "secrets_manager":
        kwargs["secret_arn"] = _env("REDSHIFT_AUTH_SECRET_ARN")

    return resolve_credentials(**kwargs)


# ---------------------------------------------------------------------------
# Skip conditions
# ---------------------------------------------------------------------------


_PROVISIONED_HOST = _env("REDSHIFT_HOST")
_SERVERLESS_HOST = _env("REDSHIFT_SERVERLESS_HOST")
_SECRET_ARN = _env("REDSHIFT_AUTH_SECRET_ARN")
_HAS_AWS = _has_aws()


def _can_run(auth_type: str, cluster: str) -> bool:
    """Check if a given (auth_type, cluster) combination can run."""
    if cluster == "provisioned" and not _PROVISIONED_HOST:
        return False
    if cluster == "serverless" and not _SERVERLESS_HOST:
        return False
    if auth_type in ("iam", "secrets_manager") and not _HAS_AWS:
        return False
    if auth_type == "secrets_manager" and not _SECRET_ARN:
        return False
    if auth_type == "password" and cluster == "serverless":
        # No password auth for serverless — IAM only
        return False
    return True


def _skip_reason(auth_type: str, cluster: str) -> str:
    if cluster == "provisioned" and not _PROVISIONED_HOST:
        return "REDSHIFT_HOST not set"
    if cluster == "serverless" and not _SERVERLESS_HOST:
        return "REDSHIFT_SERVERLESS_HOST not set"
    if not _HAS_AWS:
        return "AWS credentials not available"
    if auth_type == "secrets_manager" and not _SECRET_ARN:
        return "REDSHIFT_AUTH_SECRET_ARN not set"
    if auth_type == "password" and cluster == "serverless":
        return "Password auth not applicable for serverless"
    return ""


def _host_for(cluster: str) -> str:
    return _PROVISIONED_HOST if cluster == "provisioned" else _SERVERLESS_HOST


def _db_user_for(cluster: str) -> Optional[str]:
    """Serverless uses 'admin', provisioned uses env var."""
    return "admin" if cluster == "serverless" else None


# ---------------------------------------------------------------------------
# Parametrized: resolve_credentials → driver → SELECT current_user
# ---------------------------------------------------------------------------


_AUTH_TYPES = ["password", "iam", "secrets_manager"]
_DRIVERS = ["redshift_connector", "adbc"]
_CLUSTERS = ["provisioned", "serverless"]

_DRIVER_CONNECT = {
    "redshift_connector": _connect_redshift_connector,
    "adbc": _connect_adbc,
}


def _make_test_params():
    """Generate (auth_type, driver, cluster) combinations with skip marks."""
    params = []
    for auth_type in _AUTH_TYPES:
        for driver in _DRIVERS:
            for cluster in _CLUSTERS:
                test_id = f"{auth_type}-{driver}-{cluster}"
                if _can_run(auth_type, cluster):
                    params.append(pytest.param(auth_type, driver, cluster, id=test_id))
                else:
                    params.append(
                        pytest.param(
                            auth_type, driver, cluster, id=test_id,
                            marks=pytest.mark.skip(reason=_skip_reason(auth_type, cluster)),
                        )
                    )
    return params


class TestResolveAndConnect:
    """
    Core test: resolve credentials → connect with driver → query.

    Parametrized across auth_type × driver × cluster_type.
    """

    @pytest.mark.parametrize("auth_type,driver,cluster", _make_test_params())
    def test_resolve_connect_query(self, auth_type, driver, cluster):
        host = _host_for(cluster)
        creds = _resolve_for(auth_type, host, db_user=_db_user_for(cluster))

        connect_fn = _DRIVER_CONNECT[driver]
        conn = connect_fn(creds)
        try:
            user = _query_current_user(conn, driver)
            assert user, f"Expected a non-empty current_user, got {user!r}"
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# arrowjet.connect() with auth_type — unified path
# ---------------------------------------------------------------------------


class TestArrowjetConnect:
    """Test arrowjet.connect() with each auth_type × cluster combination."""

    @pytest.mark.parametrize("auth_type,cluster", [
        pytest.param("password", "provisioned", id="password-provisioned"),
        pytest.param(
            "iam", "provisioned", id="iam-provisioned",
            marks=pytest.mark.skipif(not _HAS_AWS, reason="No AWS creds"),
        ),
        pytest.param(
            "iam", "serverless", id="iam-serverless",
            marks=pytest.mark.skipif(
                not (_HAS_AWS and _SERVERLESS_HOST),
                reason="No AWS creds or REDSHIFT_SERVERLESS_HOST not set",
            ),
        ),
    ])
    def test_connect_and_query(self, auth_type, cluster):
        host = _host_for(cluster)
        kwargs = dict(
            host=host,
            database=_env("REDSHIFT_DATABASE", "dev"),
            port=int(_env("REDSHIFT_PORT", "5439")),
            auth_type=auth_type,
            staging_bucket=_env("STAGING_BUCKET"),
            staging_iam_role=_env("STAGING_IAM_ROLE"),
            staging_region=_env("STAGING_REGION", "us-east-1"),
        )

        if auth_type == "password":
            kwargs["user"] = _env("REDSHIFT_USER", "awsuser")
            kwargs["password"] = _env("REDSHIFT_PASS")
        elif auth_type == "iam":
            kwargs["db_user"] = _db_user_for(cluster) or _env("REDSHIFT_USER", "awsuser")
            kwargs["aws_region"] = _env("STAGING_REGION", "us-east-1")

        conn = arrowjet.connect(**kwargs)
        try:
            df = conn.fetch_dataframe("SELECT current_user AS u")
            assert len(df) == 1
            assert df.iloc[0]["u"]
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# BYOC Engine with auth — the full pipeline
# ---------------------------------------------------------------------------


class TestByocEngine:
    """Test resolve_credentials → driver → Engine.read_bulk()."""

    @pytest.mark.parametrize("driver,cluster", [
        pytest.param("redshift_connector", "provisioned", id="rs-provisioned"),
        pytest.param("adbc", "provisioned", id="adbc-provisioned"),
        pytest.param(
            "redshift_connector", "serverless", id="rs-serverless",
            marks=pytest.mark.skipif(not _SERVERLESS_HOST, reason="No serverless host"),
        ),
        pytest.param(
            "adbc", "serverless", id="adbc-serverless",
            marks=pytest.mark.skipif(not _SERVERLESS_HOST, reason="No serverless host"),
        ),
    ])
    def test_iam_engine_read_bulk(self, driver, cluster):
        if not _HAS_AWS:
            pytest.skip("No AWS creds")

        host = _host_for(cluster)
        creds = _resolve_for("iam", host, db_user=_db_user_for(cluster))

        connect_fn = _DRIVER_CONNECT[driver]
        conn = connect_fn(creds)

        # BYOC Engine needs autocommit for UNLOAD
        if driver == "redshift_connector":
            conn.autocommit = True
        # ADBC: autocommit is set via connect() kwarg — reconnect
        if driver == "adbc":
            conn.close()
            import adbc_driver_postgresql.dbapi
            conn = adbc_driver_postgresql.dbapi.connect(creds.as_uri(), autocommit=True)

        engine = arrowjet.Engine(
            staging_bucket=_env("STAGING_BUCKET"),
            staging_iam_role=_env("STAGING_IAM_ROLE"),
            staging_region=_env("STAGING_REGION", "us-east-1"),
        )

        try:
            result = engine.read_bulk(conn, "SELECT 1 AS test_col")
            assert result.rows == 1
        finally:
            conn.close()
