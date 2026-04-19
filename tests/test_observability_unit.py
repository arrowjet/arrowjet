"""
Unit tests for observability — metrics, tracing hooks, cost logging.
No AWS calls required.
"""

import pytest
from arrowjet.observability import (
    ConnectionMetrics, BulkOperationEvent, CostLogger, CostLogLevel,
)


class TestConnectionMetrics:
    def test_initial_state(self):
        m = ConnectionMetrics()
        assert m.ops_bulk_read == 0
        assert m.ops_bulk_write == 0
        assert m.bytes_staged_total == 0

    def test_record_bulk_read(self):
        m = ConnectionMetrics()
        m.record_bulk_read(1000, s3_requests=5)
        assert m.ops_bulk_read == 1
        assert m.bytes_staged_total == 1000
        assert m.s3_requests_total == 5

    def test_record_bulk_write(self):
        m = ConnectionMetrics()
        m.record_bulk_write(2000, s3_requests=3)
        assert m.ops_bulk_write == 1
        assert m.bytes_staged_total == 2000

    def test_cumulative(self):
        m = ConnectionMetrics()
        m.record_bulk_read(100)
        m.record_bulk_read(200)
        m.record_bulk_write(300)
        assert m.ops_bulk_read == 2
        assert m.ops_bulk_write == 1
        assert m.bytes_staged_total == 600

    def test_record_error(self):
        m = ConnectionMetrics()
        m.record_error()
        m.record_error()
        assert m.errors_total == 2

    def test_to_dict(self):
        m = ConnectionMetrics()
        m.record_bulk_read(100)
        d = m.to_dict()
        assert d["ops_bulk_read"] == 1
        assert d["bytes_staged_total"] == 100
        assert isinstance(d, dict)

    def test_repr(self):
        m = ConnectionMetrics()
        m.record_bulk_read(1000)
        assert "1" in repr(m)
        assert "1,000" in repr(m)


class TestBulkOperationEvent:
    def test_success_event(self):
        e = BulkOperationEvent(
            operation_type="read", success=True, rows=1000,
            bytes_staged=5000, files_count=4, s3_requests=9,
            duration_s=2.5, path_chosen="unload", s3_path="s3://b/p/",
        )
        assert e.success
        assert e.error is None

    def test_failure_event(self):
        e = BulkOperationEvent(
            operation_type="write", success=False, rows=0,
            bytes_staged=0, files_count=0, s3_requests=0,
            duration_s=0.1, path_chosen="copy", s3_path="",
            error="COPY failed",
        )
        assert not e.success
        assert e.error == "COPY failed"

    def test_to_dict(self):
        e = BulkOperationEvent(
            operation_type="read", success=True, rows=100,
            bytes_staged=500, files_count=1, s3_requests=3,
            duration_s=1.0, path_chosen="unload", s3_path="s3://b/p/",
        )
        d = e.to_dict()
        assert d["operation_type"] == "read"
        assert d["rows"] == 100


class TestCostLogger:
    def test_off_does_nothing(self, caplog):
        cl = CostLogger(CostLogLevel.OFF)
        cl.log_operation("read", 100, 500, 1, 1.0, "s3://b/p/")
        assert len(caplog.records) == 0

    def test_summary_logs(self, caplog):
        import logging
        with caplog.at_level(logging.INFO):
            cl = CostLogger(CostLogLevel.SUMMARY)
            cl.log_operation("read", 100, 500, 1, 1.0, "s3://b/p/")
        assert any("Arrowjet" in r.message for r in caplog.records)

    def test_verbose_logs_details(self, caplog):
        import logging
        with caplog.at_level(logging.INFO):
            cl = CostLogger(CostLogLevel.VERBOSE)
            cl.log_operation("read", 1000, 5000, 4, 2.5, "s3://b/p/")
        messages = " ".join(r.message for r in caplog.records)
        assert "PUT" in messages
        assert "GET" in messages

    def test_level_setter(self):
        cl = CostLogger(CostLogLevel.OFF)
        assert cl.level == CostLogLevel.OFF
        cl.level = CostLogLevel.VERBOSE
        assert cl.level == CostLogLevel.VERBOSE
