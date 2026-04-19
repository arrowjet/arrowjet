"""
SQLAlchemy dialect for Redshift via redshift_connector.

Safe mode only. Metadata always reflects safe-mode capabilities.
This is a compatibility layer for tools (dbt, Superset) that use SQLAlchemy.

Registers as: redshift+arrowjet://
Uses redshift_connector as the DBAPI (DBAPI 2.0 compliant, PG wire protocol).

Why not ADBC as DBAPI?
  - No official ADBC SQLAlchemy dialect exists (as of 2026)
  - SQLAlchemy's built-in PG dialect assumes psycopg2 and runs hooks
    (register_uuid etc.) that fail on non-psycopg2 connections
  - ADBC's columnar advantage (1.6x at 10M rows) doesn't justify the
    integration complexity for safe-mode queries

Why not sqlalchemy-redshift?
  - sqlalchemy-redshift only supports SQLAlchemy 1.x
  - AWS has SQLAlchemy 2.x support in progress but not yet released
  - This dialect provides SQLAlchemy 2.x compatibility now

When AWS ships their SA 2.x dialect, evaluate whether to adopt it or
keep this one. The user-facing URL (redshift+arrowjet://) can be
remapped without breaking user code.
"""

from __future__ import annotations

import logging

from sqlalchemy import types as sa_types
from sqlalchemy.engine import default

import redshift_connector

logger = logging.getLogger(__name__)


class RedshiftArrowjetDialect(default.DefaultDialect):
    """
    SQLAlchemy dialect for Redshift using redshift_connector as DBAPI.

    Safe mode only — no UNLOAD, no COPY, no auto-routing.
    Bulk performance is accessed through the explicit Arrowjet API.
    """

    name = "redshift"
    driver = "arrowjet"
    supports_alter = True
    supports_statement_cache = True
    implicit_returning = False  # Redshift doesn't support RETURNING
    postfetch_lastrowid = False

    # Parameter style
    paramstyle = "format"  # redshift_connector uses %s

    @classmethod
    def dbapi(cls):
        return redshift_connector

    @classmethod
    def import_dbapi(cls):
        return redshift_connector

    def create_connect_args(self, url):
        """Convert SQLAlchemy URL to redshift_connector kwargs."""
        kwargs = {
            "host": url.host or "localhost",
            "port": url.port or 5439,
            "database": url.database or "dev",
            "user": url.username or "awsuser",
            "password": url.password or "",
        }
        return [], kwargs

    def do_ping(self, dbapi_connection):
        try:
            cursor = dbapi_connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            return True
        except Exception:
            return False


    # ── Reflection / Metadata ─────────────────────────────

    def has_table(self, connection, table_name, schema=None, **kwargs):
        schema = schema or "public"
        cursor = connection.connection.dbapi_connection.cursor()
        cursor.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s",
            (schema, table_name),
        )
        return cursor.fetchone() is not None

    def get_schema_names(self, connection, **kwargs):
        cursor = connection.connection.dbapi_connection.cursor()
        cursor.execute(
            "SELECT nspname FROM pg_namespace "
            "WHERE nspname NOT LIKE 'pg_%%' "
            "ORDER BY nspname"
        )
        return [row[0] for row in cursor.fetchall()]

    def get_table_names(self, connection, schema=None, **kwargs):
        schema = schema or "public"
        cursor = connection.connection.dbapi_connection.cursor()
        cursor.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = %s AND table_type = 'BASE TABLE' "
            "ORDER BY table_name",
            (schema,),
        )
        return [row[0] for row in cursor.fetchall()]

    def get_view_names(self, connection, schema=None, **kwargs):
        schema = schema or "public"
        cursor = connection.connection.dbapi_connection.cursor()
        cursor.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = %s AND table_type = 'VIEW' "
            "ORDER BY table_name",
            (schema,),
        )
        return [row[0] for row in cursor.fetchall()]

    def get_columns(self, connection, table_name, schema=None, **kwargs):
        schema = schema or "public"
        cursor = connection.connection.dbapi_connection.cursor()
        cursor.execute(
            "SELECT column_name, data_type, is_nullable, column_default, "
            "character_maximum_length, numeric_precision, numeric_scale "
            "FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s "
            "ORDER BY ordinal_position",
            (schema, table_name),
        )
        columns = []
        for row in cursor.fetchall():
            col_name, data_type, nullable, col_default, char_len, num_prec, num_scale = row
            sa_type = self._map_redshift_type(data_type, char_len, num_prec, num_scale)
            columns.append({
                "name": col_name,
                "type": sa_type,
                "nullable": nullable == "YES",
                "default": col_default,
            })
        return columns

    def get_pk_constraint(self, connection, table_name, schema=None, **kwargs):
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_indexes(self, connection, table_name, schema=None, **kwargs):
        return []

    # ── Type mapping ──────────────────────────────────────

    def _map_redshift_type(self, type_name, char_len=None, num_prec=None, num_scale=None):
        type_name = type_name.lower()
        type_map = {
            "integer": sa_types.INTEGER,
            "bigint": sa_types.BIGINT,
            "smallint": sa_types.SMALLINT,
            "real": sa_types.FLOAT,
            "double precision": sa_types.FLOAT,
            "boolean": sa_types.BOOLEAN,
            "date": sa_types.DATE,
            "timestamp without time zone": sa_types.TIMESTAMP,
            "timestamp with time zone": sa_types.TIMESTAMP,
            "time without time zone": sa_types.TIME,
            "text": sa_types.TEXT,
            "super": sa_types.TEXT,
        }
        if type_name in type_map:
            t = type_map[type_name]
            return t() if isinstance(t, type) else t
        if "character varying" in type_name or "varchar" in type_name:
            return sa_types.VARCHAR(length=char_len)
        if "character" in type_name or "char" in type_name:
            return sa_types.CHAR(length=char_len)
        if "numeric" in type_name or "decimal" in type_name:
            return sa_types.NUMERIC(precision=num_prec, scale=num_scale)
        return sa_types.TEXT()
