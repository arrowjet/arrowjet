"""
Integration tests for cross-database transfer against real databases.

Requires: PG_HOST, PG_PASS, MYSQL_HOST, MYSQL_PASS
"""

import os
import time
import pytest
import pyarrow as pa
import pandas as pd

from arrowjet.engine import Engine
from arrowjet.transfer import transfer, TransferResult

PG_HOST = os.environ.get("PG_HOST")
PG_PASS = os.environ.get("PG_PASS")
MYSQL_HOST = os.environ.get("MYSQL_HOST")
MYSQL_PASS = os.environ.get("MYSQL_PASS")

requires_both = pytest.mark.skipif(
    not (PG_HOST and PG_PASS and MYSQL_HOST and MYSQL_PASS),
    reason="PG_HOST, PG_PASS, MYSQL_HOST, MYSQL_PASS all required",
)


def get_pg_conn():
    import psycopg2
    return psycopg2.connect(
        host=PG_HOST, port=int(os.environ.get("PG_PORT", "5432")),
        dbname=os.environ.get("PG_DATABASE", "dev"),
        user=os.environ.get("PG_USER", "awsuser"),
        password=PG_PASS, connect_timeout=10,
    )


def get_mysql_conn():
    import pymysql
    return pymysql.connect(
        host=MYSQL_HOST, port=int(os.environ.get("MYSQL_PORT", "3306")),
        database=os.environ.get("MYSQL_DATABASE", "dev"),
        user=os.environ.get("MYSQL_USER", "awsuser"),
        password=MYSQL_PASS, connect_timeout=10, local_infile=True,
    )


@requires_both
class TestPostgreSQLToMySQL:
    """Transfer data from PostgreSQL to MySQL."""

    @pytest.fixture
    def pg_conn(self):
        conn = get_pg_conn()
        yield conn
        conn.close()

    @pytest.fixture
    def mysql_conn(self):
        conn = get_mysql_conn()
        yield conn
        conn.close()

    @pytest.fixture
    def pg_source_table(self, pg_conn):
        name = f"xfer_pg_src_{int(time.time())}"
        cursor = pg_conn.cursor()
        cursor.execute(f"CREATE TABLE {name} (id BIGINT, name VARCHAR(50), score DOUBLE PRECISION)")
        for i in range(0, 500, 100):
            values = ",".join(f"({j}, 'user_{j}', {j * 1.5})" for j in range(i, min(i + 100, 500)))
            cursor.execute(f"INSERT INTO {name} VALUES {values}")
        pg_conn.commit()
        yield name
        cursor.execute(f"DROP TABLE IF EXISTS {name}")
        pg_conn.commit()

    @pytest.fixture
    def mysql_dest_table(self, mysql_conn):
        name = f"xfer_mysql_dst_{int(time.time())}"
        cursor = mysql_conn.cursor()
        cursor.execute(f"CREATE TABLE {name} (id BIGINT, name VARCHAR(50), score DOUBLE)")
        mysql_conn.commit()
        yield name
        cursor.execute(f"DROP TABLE IF EXISTS {name}")
        mysql_conn.commit()

    def test_pg_to_mysql_transfer(self, pg_conn, mysql_conn, pg_source_table, mysql_dest_table):
        pg_engine = Engine(provider="postgresql")
        mysql_engine = Engine(provider="mysql")

        result = transfer(
            source_engine=pg_engine, source_conn=pg_conn,
            query=f"SELECT * FROM {pg_source_table}",
            dest_engine=mysql_engine, dest_conn=mysql_conn,
            dest_table=mysql_dest_table,
        )

        assert isinstance(result, TransferResult)
        assert result.rows == 500
        assert result.source_provider == "postgresql"
        assert result.dest_provider == "mysql"

        # Verify in MySQL
        cursor = mysql_conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {mysql_dest_table}")
        assert cursor.fetchone()[0] == 500


@requires_both
class TestMySQLToPostgreSQL:
    """Transfer data from MySQL to PostgreSQL."""

    @pytest.fixture
    def pg_conn(self):
        conn = get_pg_conn()
        yield conn
        conn.close()

    @pytest.fixture
    def mysql_conn(self):
        conn = get_mysql_conn()
        yield conn
        conn.close()

    @pytest.fixture
    def mysql_source_table(self, mysql_conn):
        name = f"xfer_mysql_src_{int(time.time())}"
        cursor = mysql_conn.cursor()
        cursor.execute(f"CREATE TABLE {name} (id BIGINT, name VARCHAR(50), score DOUBLE)")
        for i in range(0, 300, 100):
            values = ",".join(f"({j}, 'item_{j}', {j * 2.0})" for j in range(i, min(i + 100, 300)))
            cursor.execute(f"INSERT INTO {name} VALUES {values}")
        mysql_conn.commit()
        yield name
        cursor.execute(f"DROP TABLE IF EXISTS {name}")
        mysql_conn.commit()

    @pytest.fixture
    def pg_dest_table(self, pg_conn):
        name = f"xfer_pg_dst_{int(time.time())}"
        cursor = pg_conn.cursor()
        cursor.execute(f"CREATE TABLE {name} (id BIGINT, name VARCHAR(50), score DOUBLE PRECISION)")
        pg_conn.commit()
        yield name
        cursor.execute(f"DROP TABLE IF EXISTS {name}")
        pg_conn.commit()

    def test_mysql_to_pg_transfer(self, pg_conn, mysql_conn, mysql_source_table, pg_dest_table):
        mysql_engine = Engine(provider="mysql")
        pg_engine = Engine(provider="postgresql")

        result = transfer(
            source_engine=mysql_engine, source_conn=mysql_conn,
            query=f"SELECT * FROM {mysql_source_table}",
            dest_engine=pg_engine, dest_conn=pg_conn,
            dest_table=pg_dest_table,
        )

        assert result.rows == 300
        assert result.source_provider == "mysql"
        assert result.dest_provider == "postgresql"

        # Verify in PostgreSQL
        cursor = pg_conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {pg_dest_table}")
        assert cursor.fetchone()[0] == 300

    def test_transfer_data_integrity(self, pg_conn, mysql_conn, mysql_source_table, pg_dest_table):
        """Verify actual data values survive the transfer."""
        mysql_engine = Engine(provider="mysql")
        pg_engine = Engine(provider="postgresql")

        transfer(
            source_engine=mysql_engine, source_conn=mysql_conn,
            query=f"SELECT * FROM {mysql_source_table} WHERE id < 3 ORDER BY id",
            dest_engine=pg_engine, dest_conn=pg_conn,
            dest_table=pg_dest_table,
        )

        cursor = pg_conn.cursor()
        cursor.execute(f"SELECT id, name, score FROM {pg_dest_table} ORDER BY id")
        rows = cursor.fetchall()

        assert rows[0] == (0, "item_0", 0.0)
        assert rows[1] == (1, "item_1", 2.0)
        assert rows[2] == (2, "item_2", 4.0)
