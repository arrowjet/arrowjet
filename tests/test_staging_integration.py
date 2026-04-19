"""
Integration tests for the staging subsystem — requires real AWS credentials and S3 bucket.

Tests the full cycle: validate → upload → list → download → cleanup.
Run with: PYTHONPATH=src pytest tests/test_staging_integration.py -v

Requires env vars: STAGING_BUCKET, STAGING_IAM_ROLE, STAGING_REGION
"""

import os
import pytest
import pyarrow as pa
import numpy as np

from arrowjet.staging.config import (
    StagingConfig, CleanupPolicy, EncryptionMode, QueueBehavior,
)
from arrowjet.staging.manager import (
    StagingManager, StagingValidationError, StagingConcurrencyError,
)
from arrowjet.staging.upload import StagingUploadError
from arrowjet.staging.lifecycle import OperationState

pytestmark = pytest.mark.skipif(
    not os.environ.get("STAGING_BUCKET"),
    reason="STAGING_BUCKET not set — integration tests require env vars from .env",
)

BUCKET = os.environ.get("STAGING_BUCKET", "")
REGION = os.environ.get("STAGING_REGION", "us-east-1")
IAM_ROLE = os.environ.get("STAGING_IAM_ROLE", "")
PREFIX = "redshift-adbc-staging-test"


def _make_config(**overrides):
    defaults = dict(
        bucket=BUCKET, iam_role=IAM_ROLE, region=REGION,
        prefix=PREFIX, cleanup_policy=CleanupPolicy.ALWAYS,
    )
    defaults.update(overrides)
    return StagingConfig(**defaults)


def _make_arrow_table(rows=1000):
    return pa.table({
        "id": pa.array(np.arange(rows), type=pa.int64()),
        "value": pa.array(np.random.random(rows), type=pa.float64()),
        "name": pa.array([f"row_{i}" for i in range(rows)], type=pa.string()),
    })


class TestStagingValidation:
    def test_valid_bucket(self):
        cfg = _make_config()
        mgr = StagingManager(config=cfg, cluster_id="test", database="dev")
        assert mgr.config.bucket == BUCKET

    def test_invalid_bucket_fails_fast(self):
        cfg = _make_config(bucket="nonexistent-bucket-xyz-12345")
        with pytest.raises(StagingValidationError, match="Cannot access"):
            StagingManager(config=cfg)

    def test_cross_region_rejected(self):
        cfg = _make_config(region="eu-west-1", disallow_cross_region=True)
        with pytest.raises(StagingValidationError, match="Cross-region"):
            StagingManager(config=cfg)


class TestUploadDownloadCycle:
    def test_full_cycle(self):
        cfg = _make_config()
        mgr = StagingManager(config=cfg, cluster_id="test", database="dev")
        table = _make_arrow_table(rows=5000)

        op = mgr.begin_operation("write")
        try:
            op.transition(OperationState.FILES_STAGING)
            result = mgr.uploader.upload_parquet(table, op.path)
            assert result.size_bytes > 0

            files = mgr.downloader.list_parquet_files(op.path)
            assert len(files) == 1

            downloaded = mgr.downloader.read_parquet(op.path)
            assert downloaded.num_rows == 5000
            assert downloaded.column("id").to_pylist() == list(range(5000))

            op.transition(OperationState.COMMAND_SUBMITTED)
            op.transition(OperationState.COMPLETED)
        except Exception as e:
            op.fail(str(e))
            raise
        finally:
            mgr.complete_operation(op)

        remaining = mgr.downloader.list_parquet_files(op.path)
        assert len(remaining) == 0

    def test_multiple_concurrent_operations(self):
        cfg = _make_config(max_concurrent_ops=2)
        mgr = StagingManager(config=cfg, cluster_id="test", database="dev")
        table = _make_arrow_table(rows=100)

        op1 = mgr.begin_operation("write")
        op2 = mgr.begin_operation("write")
        assert op1.path.key_prefix != op2.path.key_prefix

        try:
            op1.transition(OperationState.FILES_STAGING)
            mgr.uploader.upload_parquet(table, op1.path)
            op2.transition(OperationState.FILES_STAGING)
            mgr.uploader.upload_parquet(table, op2.path)

            assert len(mgr.downloader.list_parquet_files(op1.path)) == 1
            assert len(mgr.downloader.list_parquet_files(op2.path)) == 1

            op1.transition(OperationState.COMMAND_SUBMITTED)
            op1.transition(OperationState.COMPLETED)
            op2.transition(OperationState.COMMAND_SUBMITTED)
            op2.transition(OperationState.COMPLETED)
        except Exception as e:
            op1.fail(str(e))
            op2.fail(str(e))
            raise
        finally:
            mgr.complete_operation(op1)
            mgr.complete_operation(op2)


class TestCleanupPolicies:
    def test_on_success_cleans_on_success(self):
        cfg = _make_config(cleanup_policy=CleanupPolicy.ON_SUCCESS)
        mgr = StagingManager(config=cfg, cluster_id="test", database="dev")
        table = _make_arrow_table(rows=100)

        op = mgr.begin_operation("write")
        try:
            op.transition(OperationState.FILES_STAGING)
            mgr.uploader.upload_parquet(table, op.path)
            op.transition(OperationState.COMMAND_SUBMITTED)
            op.transition(OperationState.COMPLETED)
        finally:
            mgr.complete_operation(op)
        assert len(mgr.downloader.list_parquet_files(op.path)) == 0

    def test_on_success_preserves_on_failure(self):
        cfg = _make_config(cleanup_policy=CleanupPolicy.ON_SUCCESS)
        mgr = StagingManager(config=cfg, cluster_id="test", database="dev")
        table = _make_arrow_table(rows=100)

        op = mgr.begin_operation("write")
        try:
            op.transition(OperationState.FILES_STAGING)
            mgr.uploader.upload_parquet(table, op.path)
            op.fail("simulated failure")
        finally:
            mgr.complete_operation(op)

        assert len(mgr.downloader.list_parquet_files(op.path)) == 1
        mgr.cleanup_manager.force_cleanup(op.path)

    def test_never_policy_preserves_files(self):
        cfg = _make_config(cleanup_policy=CleanupPolicy.NEVER)
        mgr = StagingManager(config=cfg, cluster_id="test", database="dev")
        table = _make_arrow_table(rows=100)

        op = mgr.begin_operation("write")
        try:
            op.transition(OperationState.FILES_STAGING)
            mgr.uploader.upload_parquet(table, op.path)
            op.transition(OperationState.COMMAND_SUBMITTED)
            op.transition(OperationState.COMPLETED)
        finally:
            mgr.complete_operation(op)

        assert len(mgr.downloader.list_parquet_files(op.path)) == 1
        mgr.cleanup_manager.force_cleanup(op.path)


class TestEncryption:
    def test_sse_s3_upload(self):
        cfg = _make_config(encryption=EncryptionMode.SSE_S3)
        mgr = StagingManager(config=cfg, cluster_id="test", database="dev")
        table = _make_arrow_table(rows=100)

        op = mgr.begin_operation("write")
        try:
            op.transition(OperationState.FILES_STAGING)
            result = mgr.uploader.upload_parquet(table, op.path)
            assert result.size_bytes > 0
            downloaded = mgr.downloader.read_parquet(op.path)
            assert downloaded.num_rows == 100
            op.transition(OperationState.COMMAND_SUBMITTED)
            op.transition(OperationState.COMPLETED)
        except Exception as e:
            op.fail(str(e))
            raise
        finally:
            mgr.complete_operation(op)


class TestSafetyAndLimits:
    def test_max_staging_bytes_exceeded(self):
        cfg = _make_config(max_staging_bytes=100)
        mgr = StagingManager(config=cfg, cluster_id="test", database="dev")
        table = _make_arrow_table(rows=1000)

        op = mgr.begin_operation("write")
        try:
            op.transition(OperationState.FILES_STAGING)
            with pytest.raises(StagingUploadError, match="exceeds"):
                mgr.uploader.upload_parquet(table, op.path)
            op.fail("size exceeded")
        finally:
            mgr.complete_operation(op)

    def test_concurrency_reject_behavior(self):
        cfg = _make_config(max_concurrent_ops=1, queue_behavior=QueueBehavior.REJECT)
        mgr = StagingManager(config=cfg, cluster_id="test", database="dev")

        op1 = mgr.begin_operation("write")
        try:
            with pytest.raises(StagingConcurrencyError, match="Max concurrent"):
                mgr.begin_operation("write")
        finally:
            op1.fail("test done")
            mgr.complete_operation(op1)

    def test_download_empty_prefix(self):
        cfg = _make_config()
        mgr = StagingManager(config=cfg, cluster_id="test", database="dev")

        op = mgr.begin_operation("read")
        try:
            files = mgr.downloader.list_parquet_files(op.path)
            assert len(files) == 0
            op.fail("no files")
        finally:
            mgr.complete_operation(op)

    def test_orphan_detection(self):
        cfg = _make_config(cleanup_policy=CleanupPolicy.NEVER)
        mgr = StagingManager(config=cfg, cluster_id="test", database="dev")
        table = _make_arrow_table(rows=100)

        op = mgr.begin_operation("write")
        op.transition(OperationState.FILES_STAGING)
        mgr.uploader.upload_parquet(table, op.path)
        op.transition(OperationState.COMMAND_SUBMITTED)
        op.transition(OperationState.COMPLETED)
        mgr.complete_operation(op)

        assert len(mgr.downloader.list_parquet_files(op.path)) == 1
        mgr.cleanup_manager.force_cleanup(op.path)
        assert len(mgr.downloader.list_parquet_files(op.path)) == 0

    def test_cleanup_after_partial_upload(self):
        cfg = _make_config()
        mgr = StagingManager(config=cfg, cluster_id="test", database="dev")

        op = mgr.begin_operation("write")
        try:
            op.transition(OperationState.FILES_STAGING)
            small_table = _make_arrow_table(rows=10)
            mgr.uploader.upload_parquet(small_table, op.path, filename="part1.parquet")
            assert len(mgr.downloader.list_parquet_files(op.path)) == 1
            op.fail("simulated partial failure")
        finally:
            mgr.complete_operation(op)
        assert len(mgr.downloader.list_parquet_files(op.path)) == 0
