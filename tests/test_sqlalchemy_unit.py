"""
Unit tests for SQLAlchemy dialect  - type mapping and dialect properties.
No AWS calls required.
"""

import pytest
from sqlalchemy import types as sa_types
from arrowjet.sqlalchemy.dialect import RedshiftArrowjetDialect


class TestDialectProperties:
    def test_name(self):
        d = RedshiftArrowjetDialect()
        assert d.name == "redshift"

    def test_driver(self):
        assert RedshiftArrowjetDialect.driver == "arrowjet"

    def test_dbapi(self):
        dbapi = RedshiftArrowjetDialect.dbapi()
        assert hasattr(dbapi, "connect")

    def test_supports_statement_cache(self):
        d = RedshiftArrowjetDialect()
        assert d._supports_statement_cache is True


class TestTypeMapping:
    def _map(self, type_name, **kwargs):
        d = RedshiftArrowjetDialect()
        return d._map_redshift_type(type_name, **kwargs)

    def test_integer(self):
        assert isinstance(self._map("integer"), sa_types.INTEGER)

    def test_bigint(self):
        assert isinstance(self._map("bigint"), sa_types.BIGINT)

    def test_smallint(self):
        assert isinstance(self._map("smallint"), sa_types.SMALLINT)

    def test_real(self):
        assert isinstance(self._map("real"), sa_types.FLOAT)

    def test_double_precision(self):
        assert isinstance(self._map("double precision"), sa_types.FLOAT)

    def test_boolean(self):
        assert isinstance(self._map("boolean"), sa_types.BOOLEAN)

    def test_date(self):
        assert isinstance(self._map("date"), sa_types.DATE)

    def test_timestamp(self):
        assert isinstance(self._map("timestamp without time zone"), sa_types.TIMESTAMP)

    def test_timestamp_tz(self):
        t = self._map("timestamp with time zone")
        assert isinstance(t, sa_types.TIMESTAMP)

    def test_varchar(self):
        t = self._map("character varying", char_len=256)
        assert isinstance(t, sa_types.VARCHAR)

    def test_char(self):
        t = self._map("character", char_len=10)
        assert isinstance(t, sa_types.CHAR)

    def test_numeric(self):
        t = self._map("numeric", num_prec=18, num_scale=2)
        assert isinstance(t, sa_types.NUMERIC)

    def test_text(self):
        assert isinstance(self._map("text"), sa_types.TEXT)

    def test_super(self):
        assert isinstance(self._map("super"), sa_types.TEXT)

    def test_unknown_fallback(self):
        assert isinstance(self._map("geometry"), sa_types.TEXT)


class TestCreateConnectArgs:
    def test_basic_url(self):
        from sqlalchemy.engine import make_url
        d = RedshiftArrowjetDialect()
        url = make_url("redshift+arrowjet://awsuser:pass@myhost:5439/dev")
        args, kwargs = d.create_connect_args(url)
        assert kwargs["host"] == "myhost"
        assert kwargs["port"] == 5439
        assert kwargs["database"] == "dev"
        assert kwargs["user"] == "awsuser"
        assert kwargs["password"] == "pass"

    def test_defaults(self):
        from sqlalchemy.engine import make_url
        d = RedshiftArrowjetDialect()
        url = make_url("redshift+arrowjet://")
        args, kwargs = d.create_connect_args(url)
        assert kwargs["host"] == "localhost"
        assert kwargs["port"] == 5439
