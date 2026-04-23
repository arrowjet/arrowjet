"""
Authentication utilities for Arrowjet.

Each database has its own auth module under arrowjet.auth.<database>.
The shared ResolvedCredentials class provides a uniform interface
that works with any DBAPI driver.

Usage:
    from arrowjet.auth.redshift import resolve_credentials

    creds = resolve_credentials(
        host="my-cluster.xxx.us-east-1.redshift.amazonaws.com",
        auth_type="iam",
        db_user="etl_user",
    )

    # Use with any driver
    conn = redshift_connector.connect(**creds.as_kwargs())
    conn = adbc_driver_postgresql.dbapi.connect(creds.as_uri())
"""

from .base import ResolvedCredentials

__all__ = ["ResolvedCredentials"]
