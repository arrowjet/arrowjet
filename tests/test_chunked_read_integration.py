"""
Integration tests for chunked read_bulk_iter against real databases.

Parameterized across PostgreSQL, MySQL, and Redshift.
Requires environment variables per provider (see .env.example).
"""

import os

import pytest
import pyarrow as pa

from arrowjet.engine import Engine

# --- Connection helpers ---

PG_HOST = os.environ.get("PG_HOST")
PG_PASS = os.environ.get("PG_PASS")
MYSQL_HOST = os.environ.get("MYSQL_HOST")
MYSQL_PASS = os.environ.get("MYSQL_PASS")
RS_HOST = os.environ.get("REDSHIFT_HOST")
RS_PASS = os.environ.get("REDSHIFT_PASS")
STAGING_BUCKET = os.environ.get("STAGING_BUCKET")
STAGING_IAM_ROLE = os.environ.get("STAGING_IAM_ROLE")
STAGING_REGION = os.environ.get("STAGING_REGION", "us-east-1")


def _pg_conn():
    import psycopg2
    return psycopg2.connect(
        host=PG_HOST, port=int(os.environ.get("PG_PORT", "5432")),
        dbname=os.environ.get("PG_DATABASE", "dev"),
        user=os.environ.get("PG_USER", "awsuser"), password=PG_PASS,
        connect_timeout=10,
    )


def _mysql_conn():
    import pymysql
    return pymysql.connect(
        host=MYSQL_HOST, port=int(os.environ.get("MYSQL_PORT", "3306")),
        database=os.environ.get("MYSQL_DATABASE", "dev"),
        user=os.environ.get("MYSQL_USER", "awsuser"), password=MYSQL_PASS,
        connect_timeout=10, local_infile=True,
    )


def _rs_conn():
    import redshift_connector
    return redshift_connector.connect(
        host=RS_HOST, port=int(os.environ.get("REDSHIFT_PORT", "5439")),
        database=os.environ.get("REDSHIFT_DATABASE", "dev"),
        user=os.environ.get("REDSHIFT_USER", "awsuser"), password=RS_PASS,
    )


def _pg_engine():
    return Engine(provider="postgresql")


def _mysql_engine():
    return Engine(provider="mysql")


def _rs_engine():
    return Engine(
        provider="redshift",
        staging_bucket=STAGING_BUCKET,
        staging_iam_role=STAGING_IAM_ROLE,
        staging_region=STAGING_REGION,
    )


# --- Parameterized fixtures ---

# Each entry: (provider_name, has_creds, conn_factory, engine_factory, generate_series_query)
_PROVIDERS = []

if PG_HOST and PG_PASS:
    _PROVIDERS.append(pytest.param(
        "postgresql", _pg_conn, _pg_engine, "SELECT generate_series(1, {n}) AS id",
        id="postgresql",
    ))

if MYSQL_HOST and MYSQL_PASS:
    # MySQL doesn't have generate_series; use a CTE or inline values
    _PROVIDERS.append(pytest.param(
        "mysql", _mysql_conn, _mysql_engine,
        "SELECT @row := @row + 1 AS id FROM "
        "(SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 "
        "UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a, "
        "(SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 "
        "UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b, "
        "(SELECT @row := 0) r LIMIT {n}",
        id="mysql",
    ))

if RS_HOST and RS_PASS and STAGING_BUCKET and STAGING_IAM_ROLE:
    # Redshift doesn't support generate_series as standalone.
    # Use a system table with LIMIT instead.
    _PROVIDERS.append(pytest.param(
        "redshift", _rs_conn, _rs_engine,
        "SELECT ROW_NUMBER() OVER () AS id FROM stv_blocklist LIMIT {n}",
        id="redshift",
    ))


skip_no_providers = pytest.mark.skipif(len(_PROVIDERS) == 0, reason="No database credentials available")


@skip_no_providers
@pytest.mark.parametrize("provider_name,conn_factory,engine_factory,query_tpl", _PROVIDERS)
class TestChunkedReadIntegration:

    def test_iter_returns_all_rows(self, provider_name, conn_factory, engine_factory, query_tpl):
        engine = engine_factory()
        conn = conn_factory()
        try:
            query = query_tpl.format(n=100)
            batches = list(engine.read_bulk_iter(conn, query, batch_size=25))
            total_rows = sum(b.num_rows for b in batches)
            assert total_rows == 100
        finally:
            conn.close()

    def test_iter_small_batch(self, provider_name, conn_factory, engine_factory, query_tpl):
        engine = engine_factory()
        conn = conn_factory()
        try:
            query = query_tpl.format(n=10)
            batches = list(engine.read_bulk_iter(conn, query, batch_size=3))
            total_rows = sum(b.num_rows for b in batches)
            assert total_rows == 10
            # PostgreSQL/MySQL chunk by batch_size so expect multiple batches.
            # Redshift chunks by Parquet file -- small results may be 1 file/batch.
            if provider_name in ("postgresql", "mysql"):
                assert len(batches) >= 2
            else:
                assert len(batches) >= 1
        finally:
            conn.close()

    def test_iter_batches_are_record_batches(self, provider_name, conn_factory, engine_factory, query_tpl):
        engine = engine_factory()
        conn = conn_factory()
        try:
            query = query_tpl.format(n=5)
            batches = list(engine.read_bulk_iter(conn, query, batch_size=2))
            for batch in batches:
                assert isinstance(batch, pa.RecordBatch)
        finally:
            conn.close()

    def test_iter_can_reconstruct_table(self, provider_name, conn_factory, engine_factory, query_tpl):
        engine = engine_factory()
        conn = conn_factory()
        try:
            query = query_tpl.format(n=50)
            batches = list(engine.read_bulk_iter(conn, query, batch_size=15))
            table = pa.Table.from_batches(batches)
            assert table.num_rows == 50
        finally:
            conn.close()
