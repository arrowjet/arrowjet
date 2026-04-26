"""
Unit tests for the BYOC Engine  - no AWS calls required.
Tests both Redshift and PostgreSQL providers via unified Engine(provider=...).
"""

from unittest.mock import MagicMock, patch
import pyarrow as pa
import pandas as pd
import pytest

from arrowjet.engine import Engine


# --- Helpers ---

def _make_redshift_engine(**overrides):
    """Create a Redshift Engine with StagingManager mocked out."""
    defaults = dict(
        provider="redshift",
        staging_bucket="test-bucket",
        staging_iam_role="arn:aws:iam::123:role/test",
        staging_region="us-east-1",
    )
    defaults.update(overrides)
    with patch("arrowjet.staging.manager.StagingManager") as mock_sm:
        mock_config = MagicMock(bucket="test-bucket", region="us-east-1")
        mock_sm.return_value.config = mock_config
        engine = Engine(**defaults)
    engine._bulk_reader = MagicMock()
    engine._bulk_writer = MagicMock()
    return engine


def _make_pg_engine():
    """Create a PostgreSQL Engine."""
    return Engine(provider="postgresql")


def _mock_read_result():
    from arrowjet.bulk.reader import ReadResult
    return ReadResult(
        table=pa.table({"id": [1, 2, 3]}),
        rows=3, files_count=1, bytes_staged=100,
        unload_time_s=1.0, download_time_s=0.5, total_time_s=1.5,
        s3_path="s3://test-bucket/prefix/",
    )


def _mock_write_result():
    from arrowjet.bulk.writer import WriteResult
    return WriteResult(
        rows=3, bytes_staged=100,
        compression="snappy",
        upload_time_s=0.5, copy_time_s=0.3, total_time_s=0.8,
        s3_path="s3://test-bucket/prefix/",
        target_table="target_table",
    )


# --- Engine creation and provider selection ---

class TestEngineProviderSelection:
    def test_postgresql_provider(self):
        engine = Engine(provider="postgresql")
        assert engine.provider == "postgresql"
        assert "postgresql" in repr(engine)

    def test_redshift_provider(self):
        engine = _make_redshift_engine()
        assert engine.provider == "redshift"
        assert "redshift" in repr(engine)

    def test_unknown_provider_raises(self):
        with pytest.raises(ValueError, match="Unknown provider"):
            Engine(provider="oracle")

    def test_no_provider_with_staging_defaults_to_redshift(self):
        """Backward compat: Engine(staging_bucket=...) infers redshift."""
        engine = _make_redshift_engine(provider=None)
        assert engine.provider == "redshift"

    def test_no_provider_no_staging_raises(self):
        with pytest.raises(ValueError, match="provider is required"):
            Engine()

    def test_redshift_without_staging_raises(self):
        with pytest.raises(ValueError, match="staging_bucket.*required"):
            Engine(provider="redshift")

    def test_postgresql_ignores_staging_params(self):
        """PostgreSQL engine should work even if staging params are passed."""
        engine = Engine(provider="postgresql", staging_bucket="ignored")
        assert engine.provider == "postgresql"


# --- Redshift Engine config ---

class TestRedshiftEngineConfig:
    def test_staging_manager_receives_correct_config(self):
        from arrowjet.staging.config import CleanupPolicy, EncryptionMode
        with patch("arrowjet.staging.manager.StagingManager") as mock_sm:
            mock_sm.return_value = MagicMock()
            Engine(
                provider="redshift",
                staging_bucket="my-bucket",
                staging_iam_role="arn:aws:iam::123:role/test",
                staging_region="us-east-1",
                staging_prefix="my-prefix",
                staging_cleanup="always",
                staging_encryption="sse_s3",
            )
        config_arg = mock_sm.call_args[1]["config"]
        assert config_arg.bucket == "my-bucket"
        assert config_arg.prefix == "my-prefix"
        assert config_arg.cleanup_policy == CleanupPolicy.ALWAYS
        assert config_arg.encryption == EncryptionMode.SSE_S3

    def test_default_cleanup_is_on_success(self):
        from arrowjet.staging.config import CleanupPolicy
        with patch("arrowjet.staging.manager.StagingManager") as mock_sm:
            mock_sm.return_value = MagicMock()
            Engine(provider="redshift", staging_bucket="b", staging_iam_role="r", staging_region="us-east-1")
        config_arg = mock_sm.call_args[1]["config"]
        assert config_arg.cleanup_policy == CleanupPolicy.ON_SUCCESS

    def test_default_encryption_is_none(self):
        from arrowjet.staging.config import EncryptionMode
        with patch("arrowjet.staging.manager.StagingManager") as mock_sm:
            mock_sm.return_value = MagicMock()
            Engine(provider="redshift", staging_bucket="b", staging_iam_role="r", staging_region="us-east-1")
        config_arg = mock_sm.call_args[1]["config"]
        assert config_arg.encryption == EncryptionMode.NONE

    def test_repr_contains_bucket(self):
        engine = _make_redshift_engine()
        assert "test-bucket" in repr(engine)


# --- Read delegation (parameterized) ---

class TestRedshiftReadBulk:
    def test_read_delegates_to_reader(self):
        engine = _make_redshift_engine()
        mock_conn = MagicMock()
        expected = _mock_read_result()
        engine._bulk_reader.read.return_value = expected

        result = engine.read_bulk(mock_conn, "SELECT * FROM t")

        engine._bulk_reader.read.assert_called_once_with(
            mock_conn, "SELECT * FROM t",
            autocommit=True, explicit_mode=True,
        )
        assert result is expected

    def test_read_passes_kwargs(self):
        engine = _make_redshift_engine()
        mock_conn = MagicMock()
        expected = _mock_read_result()
        engine._bulk_reader.read.return_value = expected

        engine.read_bulk(mock_conn, "SELECT 1", parallel=False)

        engine._bulk_reader.read.assert_called_once_with(
            mock_conn, "SELECT 1",
            autocommit=True, explicit_mode=True, parallel=False,
        )

    def test_read_accepts_any_dbapi_conn(self):
        engine = _make_redshift_engine()
        expected = _mock_read_result()
        engine._bulk_reader.read.return_value = expected
        for name in ["redshift_connector", "psycopg2", "adbc"]:
            mock_conn = MagicMock(name=name)
            result = engine.read_bulk(mock_conn, "SELECT 1")
            assert result.rows == 3


class TestPostgreSQLReadBulk:
    def test_read_calls_copy_protocol(self):
        engine = _make_pg_engine()
        mock_conn = MagicMock()

        # Mock copy_expert to return CSV data
        def fake_copy_out(sql, buf):
            buf.write(b"id\n1\n2\n3\n")
        mock_conn.cursor.return_value.copy_expert.side_effect = fake_copy_out

        result = engine.read_bulk(mock_conn, "SELECT id FROM t")
        assert result.rows == 3

    def test_read_empty_result(self):
        engine = _make_pg_engine()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.copy_expert.side_effect = lambda sql, buf: None

        result = engine.read_bulk(mock_conn, "SELECT * FROM empty")
        assert result.rows == 0


# --- Write delegation (parameterized) ---

class TestRedshiftWriteBulk:
    def test_write_delegates_to_writer(self):
        engine = _make_redshift_engine()
        mock_conn = MagicMock()
        table = pa.table({"id": [1, 2, 3]})
        expected = _mock_write_result()
        engine._bulk_writer.write.return_value = expected

        result = engine.write_bulk(mock_conn, table, "target_table")

        engine._bulk_writer.write.assert_called_once_with(mock_conn, table, "target_table")
        assert result is expected

    def test_write_dataframe_converts_to_arrow(self):
        engine = _make_redshift_engine()
        mock_conn = MagicMock()
        df = pd.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})
        expected = _mock_write_result()
        engine._bulk_writer.write.return_value = expected

        result = engine.write_dataframe(mock_conn, df, "target_table")

        call_args = engine._bulk_writer.write.call_args
        assert isinstance(call_args[0][1], pa.Table)
        assert call_args[0][1].num_rows == 3
        assert result is expected


class TestPostgreSQLWriteBulk:
    def test_write_calls_copy_protocol(self):
        engine = _make_pg_engine()
        mock_conn = MagicMock()
        table = pa.table({"id": pa.array([1, 2], type=pa.int64())})

        result = engine.write_bulk(mock_conn, table, "test_table")

        cursor = mock_conn.cursor.return_value
        assert cursor.copy_expert.called
        assert result.rows == 2

    def test_write_dataframe(self):
        engine = _make_pg_engine()
        mock_conn = MagicMock()
        df = pd.DataFrame({"id": [1, 2, 3]})

        result = engine.write_dataframe(mock_conn, df, "test_table")
        assert result.rows == 3


# --- Connection ownership ---

class TestEngineDoesNotOwnConnection:
    def test_redshift_does_not_close_conn(self):
        engine = _make_redshift_engine()
        mock_conn = MagicMock()
        expected = _mock_read_result()
        engine._bulk_reader.read.return_value = expected

        engine.read_bulk(mock_conn, "SELECT 1")
        mock_conn.close.assert_not_called()

    def test_postgresql_does_not_close_conn(self):
        engine = _make_pg_engine()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.copy_expert.side_effect = lambda sql, buf: buf.write(b"id\n1\n")

        engine.read_bulk(mock_conn, "SELECT 1")
        mock_conn.close.assert_not_called()

    def test_multiple_conns_same_engine(self):
        engine = _make_redshift_engine()
        expected = _mock_read_result()
        engine._bulk_reader.read.return_value = expected

        conn1, conn2 = MagicMock(name="conn1"), MagicMock(name="conn2")
        r1 = engine.read_bulk(conn1, "SELECT 1")
        r2 = engine.read_bulk(conn2, "SELECT 2")
        assert r1.rows == r2.rows == 3


# --- Public API ---

class TestEnginePublicAPI:
    def test_engine_exported_from_arrowjet(self):
        import arrowjet
        assert hasattr(arrowjet, "Engine")

    def test_engine_in_all(self):
        import arrowjet
        assert "Engine" in arrowjet.__all__

    def test_postgresql_engine_alias_works(self):
        import arrowjet
        engine = arrowjet.PostgreSQLEngine()
        assert engine.provider == "postgresql"

    def test_provider_property(self):
        pg = _make_pg_engine()
        rs = _make_redshift_engine()
        assert pg.provider == "postgresql"
        assert rs.provider == "redshift"
