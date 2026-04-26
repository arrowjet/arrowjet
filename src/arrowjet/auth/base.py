"""
ResolvedCredentials  - the shared credential container.

Database-specific auth modules (redshift.py, snowflake.py, etc.)
resolve credentials into this object. It can render itself as
keyword arguments (for redshift_connector, psycopg2) or as a
PostgreSQL-style URI (for ADBC, SQLAlchemy).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote_plus, urlunparse


@dataclass(frozen=True)
class ResolvedCredentials:
    """
    Database credentials resolved from any auth method.

    Provides two output formats:
      - as_kwargs() -> dict for keyword-based drivers (redshift_connector, psycopg2)
      - as_uri()    -> PostgreSQL URI for URI-based drivers (ADBC, SQLAlchemy)

    Attributes:
        host: Database hostname
        port: Database port
        database: Database name
        user: Resolved username (may include IAM prefix like "IAM:user")
        password: Resolved password (may be temporary)
        ssl: Whether SSL is required
        extra: Additional driver-specific options
    """

    host: str
    port: int
    database: str
    user: str
    password: str = ""
    ssl: bool = True
    extra: Optional[dict] = None

    def as_kwargs(self) -> dict:
        """
        Return credentials as keyword arguments.

        Compatible with redshift_connector.connect(), psycopg2.connect(),
        and similar keyword-based connection functions.

        .. warning::
            The returned dict contains the plaintext password.
            Do not log, serialize, or persist this dict.
        """
        result = {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "password": self.password,
        }
        if self.ssl:
            result["ssl"] = True
        if self.extra:
            result.update(self.extra)
        return result

    def as_uri(self, scheme: str = "postgresql") -> str:
        """
        Return credentials as a connection URI.

        Compatible with ADBC, SQLAlchemy, and other URI-based drivers.
        Special characters in user/password are percent-encoded.

        Args:
            scheme: URI scheme. Default "postgresql".
                    Use "redshift+arrowjet" for SQLAlchemy dialect.
        """
        userinfo = quote_plus(self.user)
        if self.password:
            userinfo += ":" + quote_plus(self.password)

        netloc = f"{userinfo}@{self.host}:{self.port}"

        # Build query string from ssl + extra
        params = {}
        if self.ssl:
            params["sslmode"] = "require"
        if self.extra:
            params.update({k: str(v) for k, v in self.extra.items()})

        query = "&".join(f"{k}={quote_plus(str(v))}" for k, v in params.items())

        return urlunparse((scheme, netloc, f"/{self.database}", "", query, ""))

    def __repr__(self) -> str:
        return (
            f"ResolvedCredentials(host={self.host!r}, port={self.port}, "
            f"database={self.database!r}, user={self.user!r}, "
            f"password={'***' if self.password else '(empty)'})"
        )

    def __str__(self) -> str:
        return self.__repr__()
