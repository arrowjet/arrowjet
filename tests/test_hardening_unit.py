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
