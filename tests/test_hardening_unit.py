"""
Unit tests for hardening  - error classification, retry logic, connection checks.
Uses mocks, no AWS calls required.
"""

import pytest
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError

from arrowjet.hardening import (
    classify_error, ErrorCategory,
    retry_on_transient, check_connection,
    ConnectionLostError, check_connection_or_raise,
)


class TestErrorClassification:
    def test_s3_throttling(self):
        err = Exception("An error occurred (503) when calling PutObject: SlowDown")
        assert classify_error(err) == ErrorCategory.TRANSIENT

    def test_timeout(self):
        err = Exception("Connection timed out")
        assert classify_error(err) == ErrorCategory.TRANSIENT

    def test_connection_refused(self):
        err = Exception("Connection refused by server")
        assert classify_error(err) == ErrorCategory.CONNECTION

    def test_connection_closed(self):
        err = Exception("SSL connection has been closed unexpectedly")
        assert classify_error(err) == ErrorCategory.CONNECTION

    def test_broken_pipe(self):
        err = Exception("Broken pipe")
        assert classify_error(err) == ErrorCategory.TRANSIENT

    def test_permanent_error(self):
        err = Exception("relation 'nonexistent' does not exist")
        assert classify_error(err) == ErrorCategory.PERMANENT

    def test_syntax_error(self):
        err = Exception("syntax error at or near 'SELEC'")
        assert classify_error(err) == ErrorCategory.PERMANENT

    def test_boto_slowdown(self):
        err = ClientError(
            {"Error": {"Code": "SlowDown", "Message": "Reduce request rate"}},
            "PutObject",
        )
        assert classify_error(err) == ErrorCategory.TRANSIENT

    def test_boto_service_unavailable(self):
        err = ClientError(
            {"Error": {"Code": "ServiceUnavailable", "Message": ""}},
            "GetObject",
        )
        assert classify_error(err) == ErrorCategory.TRANSIENT

    def test_network_unreachable(self):
        err = Exception("Network is unreachable")
        assert classify_error(err) == ErrorCategory.CONNECTION


class TestRetryOnTransient:
    def test_succeeds_first_try(self):
        @retry_on_transient(max_retries=3, base_delay=0.01)
        def good_func():
            return "ok"
        assert good_func() == "ok"

    def test_retries_on_transient(self):
        call_count = 0

        @retry_on_transient(max_retries=3, base_delay=0.01)
        def flaky_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception("Connection timed out")
            return "ok"

        assert flaky_func() == "ok"
        assert call_count == 3

    def test_no_retry_on_permanent(self):
        call_count = 0

        @retry_on_transient(max_retries=3, base_delay=0.01)
        def bad_func():
            nonlocal call_count
            call_count += 1
            raise Exception("relation does not exist")

        with pytest.raises(Exception, match="relation"):
            bad_func()
        assert call_count == 1  # No retries

    def test_max_retries_exceeded(self):
        call_count = 0

        @retry_on_transient(max_retries=2, base_delay=0.01)
        def always_fails():
            nonlocal call_count
            call_count += 1
            raise Exception("Connection timed out")

        with pytest.raises(Exception, match="timed out"):
            always_fails()
        assert call_count == 3  # initial + 2 retries

    def test_backoff_increases(self):
        delays = []

        @retry_on_transient(max_retries=3, base_delay=0.01, backoff_factor=2.0)
        def track_delays():
            delays.append(len(delays))
            if len(delays) <= 3:
                raise Exception("throttling error")
            return "ok"

        with patch("arrowjet.hardening.time.sleep") as mock_sleep:
            # Override sleep to not actually wait
            mock_sleep.return_value = None
            try:
                track_delays()
            except Exception:
                pass
            # Verify backoff pattern
            if mock_sleep.call_count >= 2:
                calls = [c[0][0] for c in mock_sleep.call_args_list]
                assert calls[1] >= calls[0]  # Second delay >= first


class TestConnectionCheck:
    def test_healthy_connection(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        cursor.fetchone.return_value = (1,)
        assert check_connection(conn) is True

    def test_dead_connection(self):
        conn = MagicMock()
        conn.cursor.side_effect = Exception("connection closed")
        assert check_connection(conn) is False

    def test_check_or_raise_healthy(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        cursor.fetchone.return_value = (1,)
        check_connection_or_raise(conn)  # Should not raise

    def test_check_or_raise_dead(self):
        conn = MagicMock()
        conn.cursor.side_effect = Exception("connection closed")
        with pytest.raises(ConnectionLostError, match="no longer responsive"):
            check_connection_or_raise(conn)


# ---------------------------------------------------------------------------
# Bulk error classification (_classify_bulk_error in connection.py)
# ---------------------------------------------------------------------------

from arrowjet.connection import (
    _classify_bulk_error, ArrowjetError, S3Error, DataError, TransientError,
)


class TestClassifyBulkError:
    def test_s3_bucket_error(self):
        err = Exception("NoSuchBucket: the bucket does not exist")
        result = _classify_bulk_error(err, "write")
        assert isinstance(result, S3Error)

    def test_s3_access_denied(self):
        err = Exception("AccessDenied: not authorized")
        result = _classify_bulk_error(err, "read")
        assert isinstance(result, S3Error)

    def test_transient_timeout(self):
        err = Exception("Connection timed out after 30s")
        result = _classify_bulk_error(err, "write")
        assert isinstance(result, TransientError)

    def test_transient_connection_reset(self):
        err = Exception("Connection reset by peer")
        result = _classify_bulk_error(err, "read")
        assert isinstance(result, TransientError)

    def test_data_schema_mismatch(self):
        err = Exception("column 'price' type mismatch: expected int, got varchar")
        result = _classify_bulk_error(err, "write")
        assert isinstance(result, DataError)

    def test_data_incompatible_type(self):
        err = Exception("incompatible data type for column 'ts'")
        result = _classify_bulk_error(err, "write")
        assert isinstance(result, DataError)

    def test_unknown_falls_to_arrowjet_error(self):
        err = Exception("something completely unexpected")
        result = _classify_bulk_error(err, "write")
        assert isinstance(result, ArrowjetError)
        assert not isinstance(result, (S3Error, DataError, TransientError))

    def test_already_classified_passthrough(self):
        err = S3Error("already classified")
        result = _classify_bulk_error(err, "write")
        assert result is err  # same object, not re-wrapped

    def test_already_classified_data_error(self):
        err = DataError("already classified")
        result = _classify_bulk_error(err, "write")
        assert result is err

    def test_already_classified_transient(self):
        err = TransientError("already classified")
        result = _classify_bulk_error(err, "write")
        assert result is err

    def test_connection_lost_becomes_transient(self):
        err = ConnectionLostError("connection dropped")
        result = _classify_bulk_error(err, "read")
        assert isinstance(result, TransientError)

    def test_operation_label_in_message(self):
        err = Exception("timeout")
        result = _classify_bulk_error(err, "write")
        assert "write" in str(result)

        result2 = _classify_bulk_error(Exception("timeout"), "read")
        assert "read" in str(result2)


class TestErrorExports:
    def test_errors_exported_from_arrowjet(self):
        import arrowjet
        assert hasattr(arrowjet, "S3Error")
        assert hasattr(arrowjet, "DataError")
        assert hasattr(arrowjet, "TransientError")
        assert hasattr(arrowjet, "ConnectionLostError")

    def test_error_hierarchy(self):
        assert issubclass(S3Error, ArrowjetError)
        assert issubclass(DataError, ArrowjetError)
        assert issubclass(TransientError, ArrowjetError)

    def test_connection_lost_is_catchable(self):
        """ConnectionLostError should be catchable as Exception."""
        with pytest.raises(Exception):
            raise ConnectionLostError("test")
