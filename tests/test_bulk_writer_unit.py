"""
Unit tests for the bulk write engine  - no AWS calls required.
Tests COPY command generation and WriteResult.
"""

import pytest
from arrowjet.staging.config import StagingConfig, EncryptionMode
from arrowjet.bulk.copy_builder import CopyCommandBuilder
from arrowjet.bulk.writer import WriteResult


class TestCopyCommandBuilder:
    def _make_config(self, **overrides):
        defaults = dict(
            bucket="test-bucket",
            iam_role="arn:aws:iam::123:role/test",
            region="us-east-1",
        )
        defaults.update(overrides)
        return StagingConfig(**defaults)

    def test_basic_copy(self):
        cfg = self._make_config()
        builder = CopyCommandBuilder(cfg)
        sql = builder.build("my_table", "s3://bucket/prefix/")
        assert "COPY my_table" in sql
        assert "FROM 's3://bucket/prefix/'" in sql
        assert "IAM_ROLE 'arn:aws:iam::123:role/test'" in sql
        assert "FORMAT PARQUET" in sql

    def test_manifest_option(self):
        cfg = self._make_config()
        builder = CopyCommandBuilder(cfg)
        sql = builder.build("t", "s3://b/manifest.json", manifest=True)
        assert "MANIFEST" in sql

    def test_no_manifest_by_default(self):
        cfg = self._make_config()
        builder = CopyCommandBuilder(cfg)
        sql = builder.build("t", "s3://b/prefix/")
        assert "MANIFEST" not in sql

    def test_build_from_path(self):
        cfg = self._make_config()
        builder = CopyCommandBuilder(cfg)
        sql = builder.build_from_path("t", "my-bucket", "staging/op123/")
        assert "FROM 's3://my-bucket/staging/op123/'" in sql

    def test_kms_encryption(self):
        cfg = self._make_config(
            encryption=EncryptionMode.SSE_KMS,
            kms_key_id="arn:aws:kms:us-east-1:123:key/abc",
        )
        builder = CopyCommandBuilder(cfg)
        sql = builder.build("t", "s3://b/p/")
        assert "ENCRYPTED" in sql

    def test_no_encryption_by_default(self):
        cfg = self._make_config()
        builder = CopyCommandBuilder(cfg)
        sql = builder.build("t", "s3://b/p/")
        assert "ENCRYPTED" not in sql


class TestWriteResult:
    def test_repr(self):
        r = WriteResult(
            rows=1000, bytes_staged=5000, compression="snappy",
            upload_time_s=1.5, copy_time_s=3.0, total_time_s=4.5,
            s3_path="s3://b/p/", target_table="my_table",
        )
        assert "1,000" in repr(r)
        assert "my_table" in repr(r)

    def test_fields(self):
        r = WriteResult(
            rows=500, bytes_staged=2000, compression="zstd",
            upload_time_s=0.5, copy_time_s=1.0, total_time_s=1.5,
            s3_path="s3://b/p/", target_table="t",
        )
        assert r.rows == 500
        assert r.compression == "zstd"
        assert r.total_time_s == 1.5
