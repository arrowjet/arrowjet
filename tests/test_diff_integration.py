"""
Integration tests for data diff engine (M23).

Tests diff across real databases. Parameterized where possible.
Requires PG_HOST/PG_PASS and optionally MYSQL_HOST/MYSQL_PASS.
"""

import os

import pytest
import pyarrow as pa

from arrowjet.engine import Engine
from arrowjet.diff import diff_tables, diff

PG_HOST = os.environ.get("PG_HOST")
PG_PASS = os.environ.get("PG_PASS")
MYSQL_HOST = os.environ.get("MYSQL_HOST")
MYSQL_PASS = os.environ.get("MYSQL_PASS")

requires_pg = pytest.mark.skipif(not PG_HOST or not PG_PASS, reason="PG_HOST and PG_PASS required")
requires_mysql = pytest.mark.skipif(not MYSQL_HOST or not MYSQL_PASS, reason="MYSQL_HOST and MYSQL_PASS required")
requires_pg_and_mysql = pytest.mark.skipif(
    not (PG_HOST and PG_PASS and MYSQL_HOST and MYSQL_PASS),
    reason="Both PG and MySQL credentials required",
)


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


@requires_pg
class TestDiffSameDatabase:
    """Diff two queries within the same PostgreSQL database."""

    def test_identical_queries(self):
        engine = Engine(provider="postgresql")
        conn = _pg_conn()
        try:
            src = engine.read_bulk(conn, "SELECT generate_series(1, 50) AS id")
            dst = engine.read_bulk(conn, "SELECT generate_series(1, 50) AS id")
            result = diff_tables(src.table, dst.table, key_columns=["id"])
            assert not result.has_differences
            assert result.unchanged_count == 50
        finally:
            conn.close()

    def test_source_has_extra_rows(self):
        engine = Engine(provider="postgresql")
        conn = _pg_conn()
        try:
            src = engine.read_bulk(conn, "SELECT generate_series(1, 100) AS id")
            dst = engine.read_bulk(conn, "SELECT generate_series(1, 80) AS id")
            result = diff_tables(src.table, dst.table, key_columns=["id"])
            assert result.added_count == 20
            assert result.removed_count == 0
            assert result.unchanged_count == 80
        finally:
            conn.close()

    def test_dest_has_extra_rows(self):
        engine = Engine(provider="postgresql")
        conn = _pg_conn()
        try:
            src = engine.read_bulk(conn, "SELECT generate_series(1, 30) AS id")
            dst = engine.read_bulk(conn, "SELECT generate_series(1, 50) AS id")
            result = diff_tables(src.table, dst.table, key_columns=["id"])
            assert result.added_count == 0
            assert result.removed_count == 20
            assert result.unchanged_count == 30
        finally:
            conn.close()

    def test_diff_with_value_changes(self):
        engine = Engine(provider="postgresql")
        conn = _pg_conn()
        try:
            src = engine.read_bulk(
                conn,
                "SELECT id, CASE WHEN id <= 5 THEN 'old' ELSE 'new' END AS status "
                "FROM generate_series(1, 10) AS id"
            )
            dst = engine.read_bulk(
                conn,
                "SELECT id, 'old' AS status FROM generate_series(1, 10) AS id"
            )
            result = diff_tables(src.table, dst.table, key_columns=["id"])
            assert result.changed_count == 5  # ids 6-10 changed from 'old' to 'new'
            assert result.unchanged_count == 5
            assert "status" in result.changed_columns
        finally:
            conn.close()


@requires_pg_and_mysql
class TestDiffCrossDatabase:
    """Diff between PostgreSQL and MySQL."""

    def test_cross_db_diff_identical(self):
        pg_engine = Engine(provider="postgresql")
        mysql_engine = Engine(provider="mysql")
        pg_conn = _pg_conn()
        mysql_conn = _mysql_conn()

        try:
            # Write same data to both
            data = pa.table({"id": [1, 2, 3], "val": ["a", "b", "c"]})

            # Create tables
            pg_cur = pg_conn.cursor()
            pg_cur.execute("DROP TABLE IF EXISTS diff_test_src")
            pg_cur.execute("CREATE TABLE diff_test_src (id BIGINT, val VARCHAR(10))")
            pg_conn.commit()
            pg_engine.write_bulk(pg_conn, data, "diff_test_src")
            pg_conn.commit()

            mysql_cur = mysql_conn.cursor()
            mysql_cur.execute("DROP TABLE IF EXISTS diff_test_dst")
            mysql_cur.execute("CREATE TABLE diff_test_dst (id BIGINT, val VARCHAR(10))")
            mysql_conn.commit()
            mysql_engine.write_bulk(mysql_conn, data, "diff_test_dst")
            mysql_conn.commit()

            # Diff
            result = diff(
                source_engine=pg_engine, source_conn=pg_conn,
                dest_engine=mysql_engine, dest_conn=mysql_conn,
                query="SELECT * FROM diff_test_src",
                dest_table="diff_test_dst",
                key_columns=["id"],
            )
            assert not result.has_differences
            assert result.unchanged_count == 3

            # Cleanup
            pg_cur.execute("DROP TABLE IF EXISTS diff_test_src")
            pg_conn.commit()
            mysql_cur.execute("DROP TABLE IF EXISTS diff_test_dst")
            mysql_conn.commit()
        finally:
            pg_conn.close()
            mysql_conn.close()

    def test_cross_db_diff_with_differences(self):
        pg_engine = Engine(provider="postgresql")
        mysql_engine = Engine(provider="mysql")
        pg_conn = _pg_conn()
        mysql_conn = _mysql_conn()

        try:
            # Source has id 1,2,3 with val a,b,c
            src_data = pa.table({"id": [1, 2, 3], "val": ["a", "b_new", "c"]})
            # Dest has id 1,2,4 with val a,b_old,d
            dst_data = pa.table({"id": [1, 2, 4], "val": ["a", "b_old", "d"]})

            pg_cur = pg_conn.cursor()
            pg_cur.execute("DROP TABLE IF EXISTS diff_test_src2")
            pg_cur.execute("CREATE TABLE diff_test_src2 (id BIGINT, val VARCHAR(10))")
            pg_conn.commit()
            pg_engine.write_bulk(pg_conn, src_data, "diff_test_src2")
            pg_conn.commit()

            mysql_cur = mysql_conn.cursor()
            mysql_cur.execute("DROP TABLE IF EXISTS diff_test_dst2")
            mysql_cur.execute("CREATE TABLE diff_test_dst2 (id BIGINT, val VARCHAR(10))")
            mysql_conn.commit()
            mysql_engine.write_bulk(mysql_conn, dst_data, "diff_test_dst2")
            mysql_conn.commit()

            result = diff(
                source_engine=pg_engine, source_conn=pg_conn,
                dest_engine=mysql_engine, dest_conn=mysql_conn,
                query="SELECT * FROM diff_test_src2",
                dest_table="diff_test_dst2",
                key_columns=["id"],
            )
            assert result.added_count == 1      # id=3
            assert result.removed_count == 1    # id=4
            assert result.changed_count == 1    # id=2
            assert result.unchanged_count == 1  # id=1

            # Cleanup
            pg_cur.execute("DROP TABLE IF EXISTS diff_test_src2")
            pg_conn.commit()
            mysql_cur.execute("DROP TABLE IF EXISTS diff_test_dst2")
            mysql_conn.commit()
        finally:
            pg_conn.close()
            mysql_conn.close()
