"""
Integration tests for SQLAlchemy dialect  - requires real Redshift.
Uses redshift_connector as DBAPI.
"""

import os
import pytest
from sqlalchemy import create_engine, text, inspect

pytestmark = pytest.mark.skipif(
    not os.environ.get("REDSHIFT_HOST"),
    reason="REDSHIFT_HOST not set",
)


def _engine():
    host = os.environ["REDSHIFT_HOST"]
    pwd = os.environ["REDSHIFT_PASS"]

    # Register our dialect
    from sqlalchemy.dialects import registry
    registry.register("redshift.arrowjet", "arrowjet.sqlalchemy.dialect", "RedshiftArrowjetDialect")

    return create_engine(f"redshift+arrowjet://awsuser:{pwd}@{host}:5439/dev")


class TestConnection:
    def test_execute(self):
        engine = _engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 AS test"))
            assert result.fetchone()[0] == 1

    def test_multiple_rows(self):
        engine = _engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3"))
            assert len(result.fetchall()) == 3


class TestMetadata:
    def test_get_table_names(self):
        engine = _engine()
        insp = inspect(engine)
        tables = insp.get_table_names(schema="public")
        assert isinstance(tables, list)
        assert len(tables) > 0

    def test_get_columns(self):
        engine = _engine()
        insp = inspect(engine)
        tables = insp.get_table_names(schema="public")
        if tables:
            cols = insp.get_columns(tables[0], schema="public")
            assert isinstance(cols, list)
            if cols:
                assert "name" in cols[0]
                assert "type" in cols[0]

    def test_has_table(self):
        engine = _engine()
        insp = inspect(engine)
        tables = insp.get_table_names(schema="public")
        if tables:
            assert insp.has_table(tables[0], schema="public")
        assert not insp.has_table("nonexistent_xyz_123", schema="public")


class TestCRUD:
    def _cleanup(self, engine, name):
        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {name}"))
            conn.commit()

    def test_create_insert_select(self):
        engine = _engine()
        self._cleanup(engine, "sa_test_crud")
        try:
            with engine.connect() as conn:
                conn.execute(text("CREATE TABLE sa_test_crud (id INT, name VARCHAR(64))"))
                conn.commit()
                conn.execute(text("INSERT INTO sa_test_crud VALUES (1, 'alice'), (2, 'bob')"))
                conn.commit()
                rows = conn.execute(text("SELECT * FROM sa_test_crud ORDER BY id")).fetchall()
                assert len(rows) == 2
                assert rows[0][1] == "alice"
        finally:
            self._cleanup(engine, "sa_test_crud")

    def test_transaction_commit(self):
        engine = _engine()
        self._cleanup(engine, "sa_test_tx")
        try:
            with engine.connect() as conn:
                conn.execute(text("CREATE TABLE sa_test_tx (id INT)"))
                conn.commit()
            with engine.begin() as conn:
                conn.execute(text("INSERT INTO sa_test_tx VALUES (1)"))
            with engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM sa_test_tx"))
                assert result.fetchone()[0] == 1
        finally:
            self._cleanup(engine, "sa_test_tx")

    def test_pandas_read_sql(self):
        import pandas as pd
        engine = _engine()
        df = pd.read_sql("SELECT 1 AS id, 'test' AS name", engine)
        assert len(df) == 1
        assert "id" in df.columns
