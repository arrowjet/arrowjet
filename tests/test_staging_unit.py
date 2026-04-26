"""
Unit tests for the staging subsystem  - no AWS calls required.
Tests config validation, namespace generation, lifecycle state machine.
"""

import pytest
from arrowjet.staging.config import (
    StagingConfig, CleanupPolicy, EncryptionMode, QueueBehavior,
)
from arrowjet.staging.namespace import StagingNamespace, OperationPath
from arrowjet.staging.lifecycle import (
    StagingOperation, OperationState, InvalidTransitionError,
)


# ── Config Tests ──────────────────────────────────────────────

class TestStagingConfig:
    def test_valid_config(self):
        cfg = StagingConfig(
            bucket="test-bucket",
            iam_role="arn:aws:iam::123:role/test",
            region="us-east-1",
        )
        assert cfg.bucket == "test-bucket"
        assert cfg.cleanup_policy == CleanupPolicy.ON_SUCCESS
        assert cfg.max_concurrent_ops == 4

    def test_missing_bucket_raises(self):
        with pytest.raises(ValueError, match="bucket"):
            StagingConfig(bucket="", iam_role="arn:test", region="us-east-1")

    def test_missing_iam_role_raises(self):
        with pytest.raises(ValueError, match="IAM role"):
            StagingConfig(bucket="b", iam_role="", region="us-east-1")

    def test_missing_region_raises(self):
        with pytest.raises(ValueError, match="region"):
            StagingConfig(bucket="b", iam_role="arn:test", region="")

    def test_kms_requires_key_id(self):
        with pytest.raises(ValueError, match="kms_key_id"):
            StagingConfig(
                bucket="b", iam_role="arn:test", region="us-east-1",
                encryption=EncryptionMode.SSE_KMS,
            )

    def test_kms_with_key_id(self):
        cfg = StagingConfig(
            bucket="b", iam_role="arn:test", region="us-east-1",
            encryption=EncryptionMode.SSE_KMS,
            kms_key_id="arn:aws:kms:us-east-1:123:key/abc",
        )
        headers = cfg.encryption_headers()
        assert headers["ServerSideEncryption"] == "aws:kms"
        assert "abc" in headers["SSEKMSKeyId"]

    def test_sse_s3_headers(self):
        cfg = StagingConfig(
            bucket="b", iam_role="arn:test", region="us-east-1",
            encryption=EncryptionMode.SSE_S3,
        )
        assert cfg.encryption_headers() == {"ServerSideEncryption": "AES256"}

    def test_no_encryption_headers(self):
        cfg = StagingConfig(
            bucket="b", iam_role="arn:test", region="us-east-1",
        )
        assert cfg.encryption_headers() == {}

    def test_s3_client_kwargs(self):
        cfg = StagingConfig(
            bucket="b", iam_role="arn:test", region="us-west-2",
            s3_endpoint_url="https://vpce-123.s3.us-west-2.vpce.amazonaws.com",
        )
        kwargs = cfg.s3_client_kwargs()
        assert kwargs["region_name"] == "us-west-2"
        assert "vpce" in kwargs["endpoint_url"]

    def test_max_concurrent_ops_validation(self):
        with pytest.raises(ValueError, match="max_concurrent_ops"):
            StagingConfig(
                bucket="b", iam_role="arn:test", region="us-east-1",
                max_concurrent_ops=0,
            )


# ── Namespace Tests ───────────────────────────────────────────

class TestStagingNamespace:
    def _make_config(self):
        return StagingConfig(
            bucket="test-bucket",
            iam_role="arn:aws:iam::123:role/test",
            region="us-east-1",
            prefix="staging",
        )

    def test_unique_paths(self):
        ns = StagingNamespace(config=self._make_config(), cluster_id="cl1", database="dev")
        p1 = ns.new_operation()
        p2 = ns.new_operation()
        assert p1.key_prefix != p2.key_prefix
        assert p1.stmt_id != p2.stmt_id

    def test_path_contains_cluster_and_db(self):
        ns = StagingNamespace(config=self._make_config(), cluster_id="mycluster", database="mydb")
        p = ns.new_operation()
        assert "mycluster" in p.key_prefix
        assert "mydb" in p.key_prefix

    def test_path_contains_connection_id(self):
        ns = StagingNamespace(config=self._make_config())
        p = ns.new_operation()
        assert ns.connection_id in p.key_prefix

    def test_s3_uri(self):
        ns = StagingNamespace(config=self._make_config())
        p = ns.new_operation()
        assert p.s3_uri.startswith("s3://test-bucket/staging/")

    def test_file_key(self):
        ns = StagingNamespace(config=self._make_config())
        p = ns.new_operation()
        key = p.file_key("data.parquet")
        assert key.endswith("data.parquet")
        assert key.startswith("staging/")

    def test_retry_increments_attempt(self):
        ns = StagingNamespace(config=self._make_config())
        p1 = ns.new_operation(attempt=1)
        assert p1.attempt == 1
        p2 = p1.retry()
        assert p2.attempt == 2
        assert p2.stmt_id == p1.stmt_id
        assert p2.key_prefix != p1.key_prefix

    def test_different_connections_different_paths(self):
        cfg = self._make_config()
        ns1 = StagingNamespace(config=cfg, cluster_id="cl1", database="dev")
        ns2 = StagingNamespace(config=cfg, cluster_id="cl1", database="dev")
        assert ns1.connection_id != ns2.connection_id
        p1 = ns1.new_operation()
        p2 = ns2.new_operation()
        assert p1.key_prefix != p2.key_prefix


# ── Lifecycle Tests ───────────────────────────────────────────

class TestStagingLifecycle:
    def _make_op(self):
        cfg = StagingConfig(bucket="b", iam_role="arn:test", region="us-east-1")
        ns = StagingNamespace(config=cfg)
        path = ns.new_operation()
        return StagingOperation(path=path, operation_type="write")

    def test_initial_state(self):
        op = self._make_op()
        assert op.state == OperationState.PLANNED

    def test_happy_path(self):
        op = self._make_op()
        op.transition(OperationState.FILES_STAGING)
        op.transition(OperationState.COMMAND_SUBMITTED)
        op.transition(OperationState.COMPLETED)
        op.transition(OperationState.CLEANUP_DONE)
        assert op.is_terminal
        assert op.duration_s is not None

    def test_invalid_transition_raises(self):
        op = self._make_op()
        with pytest.raises(InvalidTransitionError):
            op.transition(OperationState.COMPLETED)  # can't skip to completed

    def test_fail_from_any_active_state(self):
        for start_state in [OperationState.FILES_STAGING, OperationState.COMMAND_SUBMITTED]:
            op = self._make_op()
            op.transition(OperationState.FILES_STAGING)
            if start_state == OperationState.COMMAND_SUBMITTED:
                op.transition(OperationState.COMMAND_SUBMITTED)
            op.fail("something broke")
            assert op.state == OperationState.FAILED
            assert op.error == "something broke"

    def test_cleanup_after_failure(self):
        op = self._make_op()
        op.transition(OperationState.FILES_STAGING)
        op.fail("error")
        assert op.needs_cleanup
        op.transition(OperationState.CLEANUP_DONE)
        assert op.is_terminal

    def test_cleanup_failed(self):
        op = self._make_op()
        op.transition(OperationState.FILES_STAGING)
        op.transition(OperationState.COMMAND_SUBMITTED)
        op.transition(OperationState.COMPLETED)
        op.transition(OperationState.CLEANUP_FAILED)
        assert op.is_terminal

    def test_summary(self):
        op = self._make_op()
        op.files_count = 4
        op.bytes_staged = 1024
        op.rows_affected = 1000
        s = op.summary()
        assert s["type"] == "write"
        assert s["files"] == 4
        assert s["bytes_staged"] == 1024
        assert s["rows"] == 1000

    def test_terminal_states_cannot_transition(self):
        op = self._make_op()
        op.transition(OperationState.FILES_STAGING)
        op.transition(OperationState.COMMAND_SUBMITTED)
        op.transition(OperationState.COMPLETED)
        op.transition(OperationState.CLEANUP_DONE)
        with pytest.raises(InvalidTransitionError):
            op.transition(OperationState.PLANNED)
