"""
Unit tests for the BYOC Engine — no AWS calls required.
Tests that Engine accepts any DBAPI connection and delegates correctly.
"""

from unittest.mock import MagicMock, patch
import pyarrow as pa
import pandas as pd
import pytest


def _make_engine(**overrides):
    """Create an Engine with StagingManager validation mocked out."""
    from arrowjet.engine import Engine
    defaults = dict(
        staging_bucket="test-bucket",
        staging_iam_role="arn:aws:iam::123:role/test",
        staging_region="us-east-1",
    )
    defaults.update(overrides)
    with patch("arrowjet.engine.StagingManager") as mock_sm:
        mock_sm.return_value = MagicMock()
        engine = Engine(**defaults)
    return engine


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


class TestEngineInit:
    def test_repr(self):
        engine = _make_engine()
        assert "Engine(" in repr(engine)

    def test_staging_manager_receives_correct_config(self):
        from arrowjet.staging.config import CleanupPolicy, EncryptionMode
        from arrowjet.engine import Engine
        with patch("arrowjet.engine.StagingManager") as mock_sm:
            mock_sm.return_value = MagicMock()
            Engine(
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
        from arrowjet.engine import Engine
        with patch("arrowjet.engine.StagingManager") as mock_sm:
            mock_sm.return_value = MagicMock()
            Engine(staging_bucket="b", staging_iam_role="r", staging_region="us-east-1")
        config_arg = mock_sm.call_args[1]["config"]
        assert config_arg.cleanup_policy == CleanupPolicy.ON_SUCCESS

    def test_default_encryption_is_none(self):
        from arrowjet.staging.config import EncryptionMode
        from arrowjet.engine import Engine
        with patch("arrowjet.engine.StagingManager") as mock_sm:
            mock_sm.return_value = MagicMock()
            Engine(staging_bucket="b", staging_iam_role="r", staging_region="us-east-1")
        config_arg = mock_sm.call_args[1]["config"]
        assert config_arg.encryption == EncryptionMode.NONE


class TestEngineReadBulk:
    def test_read_bulk_delegates_to_reader(self):
        engine = _make_engine()
        mock_conn = MagicMock()
        expected = _mock_read_result()

        with patch.object(engine._bulk_reader, "read", return_value=expected) as mock_read:
            result = engine.read_bulk(mock_conn, "SELECT * FROM t")

        mock_read.assert_called_once_with(
            mock_conn, "SELECT * FROM t",
            autocommit=True, explicit_mode=True,
        )
        assert result is expected

    def test_read_bulk_passes_kwargs(self):
        engine = _make_engine()
        mock_conn = MagicMock()
        expected = _mock_read_result()

        with patch.object(engine._bulk_reader, "read", return_value=expected) as mock_read:
            engine.read_bulk(mock_conn, "SELECT 1", parallel=False)

        mock_read.assert_called_once_with(
            mock_conn, "SELECT 1",
            autocommit=True, explicit_mode=True, parallel=False,
        )

    def test_read_bulk_accepts_any_dbapi_conn(self):
        """Engine only needs conn.cursor() — any DBAPI connection works."""
        engine = _make_engine()
        expected = _mock_read_result()
        for driver_name in ["redshift_connector", "psycopg2", "adbc"]:
            mock_conn = MagicMock(name=driver_name)
            with patch.object(engine._bulk_reader, "read", return_value=expected):
                result = engine.read_bulk(mock_conn, "SELECT 1")
            assert result.rows == 3


class TestEngineWriteBulk:
    def test_write_bulk_delegates_to_writer(self):
        engine = _make_engine()
        mock_conn = MagicMock()
        table = pa.table({"id": [1, 2, 3]})
        expected = _mock_write_result()

        with patch.object(engine._bulk_writer, "write", return_value=expected) as mock_write:
            result = engine.write_bulk(mock_conn, table, "target_table")

        mock_write.assert_called_once_with(mock_conn, table, "target_table")
        assert result is expected

    def test_write_dataframe_converts_to_arrow(self):
        engine = _make_engine()
        mock_conn = MagicMock()
        df = pd.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})
        expected = _mock_write_result()

        with patch.object(engine._bulk_writer, "write", return_value=expected) as mock_write:
            result = engine.write_dataframe(mock_conn, df, "target_table")

        call_args = mock_write.call_args
        assert isinstance(call_args[0][1], pa.Table)
        assert call_args[0][1].num_rows == 3
        assert result is expected


class TestEngineDoesNotOwnConnection:
    def test_engine_does_not_close_conn(self):
        """Engine must not close the user's connection."""
        engine = _make_engine()
        mock_conn = MagicMock()
        expected = _mock_read_result()

        with patch.object(engine._bulk_reader, "read", return_value=expected):
            engine.read_bulk(mock_conn, "SELECT 1")

        mock_conn.close.assert_not_called()

    def test_multiple_conns_same_engine(self):
        """Same engine can be used with different connections."""
        engine = _make_engine()
        conn1, conn2 = MagicMock(name="conn1"), MagicMock(name="conn2")
        expected = _mock_read_result()

        with patch.object(engine._bulk_reader, "read", return_value=expected):
            r1 = engine.read_bulk(conn1, "SELECT 1")
            r2 = engine.read_bulk(conn2, "SELECT 2")

        assert r1.rows == r2.rows == 3


class TestEnginePublicAPI:
    def test_engine_exported_from_arrowjet(self):
        import arrowjet
        assert hasattr(arrowjet, "Engine")
        assert arrowjet.Engine is not None

    def test_engine_in_all(self):
        import arrowjet
        assert "Engine" in arrowjet.__all__
