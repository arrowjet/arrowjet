"""
Unit tests for the provider abstraction layer.

Tests BulkProvider interface, RedshiftProvider implementation,
and that BulkReader/BulkWriter are provider-agnostic.
"""

import pytest
from unittest.mock import MagicMock
from arrowjet.providers.base import BulkProvider, ExportCommand, ImportCommand
from arrowjet.providers.redshift import RedshiftProvider, _wrap_if_limit
from arrowjet.staging.config import StagingConfig, EncryptionMode


def _make_config(**overrides):
    defaults = dict(
        bucket="test-bucket",
        iam_role="arn:aws:iam::123:role/test",
        region="us-east-1",
    )
    defaults.update(overrides)
    return StagingConfig(**defaults)


class TestBulkProviderInterface:
    def test_redshift_is_bulk_provider(self):
        provider = RedshiftProvider(_make_config())
        assert isinstance(provider, BulkProvider)

    def test_cannot_instantiate_abstract(self):
        with pytest.raises(TypeError):
            BulkProvider()

    def test_export_command_is_dataclass(self):
        cmd = ExportCommand(sql="SELECT 1", staging_path="s3://b/p/", format="parquet")
        assert cmd.sql == "SELECT 1"
        assert cmd.format == "parquet"

    def test_import_command_is_dataclass(self):
        cmd = ImportCommand(sql="COPY t FROM ...", staging_path="s3://b/p/", format="parquet")
        assert cmd.sql == "COPY t FROM ..."


class TestRedshiftProvider:
    def _provider(self, **config_overrides):
        return RedshiftProvider(_make_config(**config_overrides))

    # ── Export (UNLOAD) ──────────────────────────────────────

    def test_export_returns_export_command(self):
        p = self._provider()
        cmd = p.build_export_sql("SELECT * FROM t", "s3://b/p/")
        assert isinstance(cmd, ExportCommand)
        assert cmd.format == "parquet"
        assert cmd.staging_path == "s3://b/p/"

    def test_export_sql_contains_unload(self):
        p = self._provider()
        cmd = p.build_export_sql("SELECT * FROM t", "s3://b/p/")
        assert "UNLOAD" in cmd.sql

    def test_export_sql_contains_query(self):
        p = self._provider()
        cmd = p.build_export_sql("SELECT * FROM t", "s3://b/p/")
        assert "SELECT * FROM t" in cmd.sql

    def test_export_sql_contains_iam_role(self):
        p = self._provider()
        cmd = p.build_export_sql("SELECT 1", "s3://b/p/")
        assert "arn:aws:iam::123:role/test" in cmd.sql

    def test_export_parallel_on_by_default(self):
        p = self._provider()
        cmd = p.build_export_sql("SELECT 1", "s3://b/p/")
        assert "PARALLEL ON" in cmd.sql

    def test_export_parallel_off(self):
        p = self._provider()
        cmd = p.build_export_sql("SELECT 1", "s3://b/p/", parallel=False)
        assert "PARALLEL OFF" in cmd.sql

    def test_export_kms_encryption(self):
        p = self._provider(
            encryption=EncryptionMode.SSE_KMS,
            kms_key_id="arn:aws:kms:us-east-1:123:key/abc",
        )
        cmd = p.build_export_sql("SELECT 1", "s3://b/p/")
        assert "ENCRYPTED" in cmd.sql
        assert "KMS_KEY_ID" in cmd.sql

    def test_export_no_encryption_by_default(self):
        p = self._provider()
        cmd = p.build_export_sql("SELECT 1", "s3://b/p/")
        assert "ENCRYPTED" not in cmd.sql

    def test_export_wraps_limit(self):
        p = self._provider()
        cmd = p.build_export_sql("SELECT * FROM t LIMIT 100", "s3://b/p/")
        assert "SELECT * FROM (SELECT * FROM t LIMIT 100)" in cmd.sql

    def test_export_dollar_quoting(self):
        p = self._provider()
        cmd = p.build_export_sql("SELECT 'hello' FROM t", "s3://b/p/")
        assert "$$" in cmd.sql

    # ── Import (COPY) ────────────────────────────────────────

    def test_import_returns_import_command(self):
        p = self._provider()
        cmd = p.build_import_sql("my_table", "s3://b/p/")
        assert isinstance(cmd, ImportCommand)
        assert cmd.format == "parquet"

    def test_import_sql_contains_copy(self):
        p = self._provider()
        cmd = p.build_import_sql("my_table", "s3://b/p/")
        assert "COPY my_table" in cmd.sql

    def test_import_sql_contains_staging_path(self):
        p = self._provider()
        cmd = p.build_import_sql("my_table", "s3://b/p/")
        assert "s3://b/p/" in cmd.sql

    def test_import_sql_contains_iam_role(self):
        p = self._provider()
        cmd = p.build_import_sql("my_table", "s3://b/p/")
        assert "arn:aws:iam::123:role/test" in cmd.sql

    def test_import_kms_encryption(self):
        p = self._provider(
            encryption=EncryptionMode.SSE_KMS,
            kms_key_id="arn:aws:kms:us-east-1:123:key/abc",
        )
        cmd = p.build_import_sql("my_table", "s3://b/p/")
        assert "ENCRYPTED" in cmd.sql

    # ── Provider metadata ────────────────────────────────────

    def test_staging_format_is_parquet(self):
        assert RedshiftProvider(_make_config()).staging_format == "parquet"

    def test_staging_backend_is_s3(self):
        assert RedshiftProvider(_make_config()).staging_backend == "s3"

    def test_name_is_redshift(self):
        assert RedshiftProvider(_make_config()).name == "redshift"


class TestLimitWrapping:
    """UNLOAD doesn't support LIMIT directly — provider should auto-wrap."""

    def test_limit_gets_wrapped(self):
        assert _wrap_if_limit("SELECT * FROM t LIMIT 100") == \
               "SELECT * FROM (SELECT * FROM t LIMIT 100)"

    def test_no_limit_unchanged(self):
        q = "SELECT * FROM t WHERE id > 5"
        assert _wrap_if_limit(q) == q

    def test_offset_gets_wrapped(self):
        result = _wrap_if_limit("SELECT * FROM t LIMIT 100 OFFSET 50")
        assert result.startswith("SELECT * FROM (")

    def test_offset_only_gets_wrapped(self):
        result = _wrap_if_limit("SELECT * FROM t OFFSET 50")
        assert result.startswith("SELECT * FROM (")

    def test_limit_case_insensitive(self):
        result = _wrap_if_limit("SELECT * FROM t limit 50")
        assert result.startswith("SELECT * FROM (")

    def test_already_subquery_no_double_wrap(self):
        q = "SELECT * FROM (SELECT * FROM t LIMIT 100)"
        assert _wrap_if_limit(q) == q


class TestProviderAgnosticEngine:
    """BulkReader and BulkWriter should work with any BulkProvider."""

    def test_bulk_reader_accepts_custom_provider(self):
        """BulkReader can be initialized with a custom provider."""
        from unittest.mock import patch, MagicMock
        from arrowjet.bulk.reader import BulkReader

        mock_provider = MagicMock(spec=BulkProvider)
        mock_staging = MagicMock()
        mock_staging.config = _make_config()

        reader = BulkReader(mock_staging, provider=mock_provider)
        assert reader._provider is mock_provider

    def test_bulk_writer_accepts_custom_provider(self):
        """BulkWriter can be initialized with a custom provider."""
        from arrowjet.bulk.writer import BulkWriter

        mock_provider = MagicMock(spec=BulkProvider)
        mock_staging = MagicMock()
        mock_staging.config = _make_config()

        writer = BulkWriter(mock_staging, provider=mock_provider)
        assert writer._provider is mock_provider

    def test_bulk_reader_defaults_to_redshift_provider(self):
        """Without explicit provider, BulkReader uses RedshiftProvider."""
        from arrowjet.bulk.reader import BulkReader

        mock_staging = MagicMock()
        mock_staging.config = _make_config()

        reader = BulkReader(mock_staging)
        assert isinstance(reader._provider, RedshiftProvider)

    def test_bulk_writer_defaults_to_redshift_provider(self):
        """Without explicit provider, BulkWriter uses RedshiftProvider."""
        from arrowjet.bulk.writer import BulkWriter

        mock_staging = MagicMock()
        mock_staging.config = _make_config()

        writer = BulkWriter(mock_staging)
        assert isinstance(writer._provider, RedshiftProvider)
