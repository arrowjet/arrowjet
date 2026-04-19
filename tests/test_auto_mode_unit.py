"""
Unit tests for auto mode — routing logic, EXPLAIN parsing, configuration.
Uses mocks, no AWS calls required.
"""

import pytest
from unittest.mock import MagicMock

from arrowjet.auto_mode import (
    AutoRouter, AutoModeConfig, ReadMode, RoutingDecision, RoutingResult,
)


class TestRoutingDecision:
    def test_result_bool_bulk(self):
        r = RoutingResult(RoutingDecision.BULK, "test")
        assert bool(r) is True

    def test_result_bool_direct(self):
        r = RoutingResult(RoutingDecision.DIRECT, "test")
        assert bool(r) is False


class TestAutoRouterPhase1:
    """Phase 1: conservative — requires explicit hint."""

    def _make_router(self, **overrides):
        defaults = dict(require_hint=True, use_explain=False)
        defaults.update(overrides)
        return AutoRouter(AutoModeConfig(**defaults))

    def test_no_hint_routes_direct(self):
        router = self._make_router()
        conn = MagicMock()
        result = router.route(conn, "SELECT * FROM t", autocommit=True)
        assert result.decision == RoutingDecision.DIRECT
        assert "hint" in result.reason.lower()

    def test_hint_routes_bulk(self):
        router = self._make_router()
        conn = MagicMock()
        result = router.route(conn, "SELECT * FROM t", autocommit=True, bulk_hint=True)
        assert result.decision == RoutingDecision.BULK

    def test_ineligible_query_routes_direct(self):
        router = self._make_router()
        conn = MagicMock()
        # INSERT is not eligible for UNLOAD
        result = router.route(conn, "INSERT INTO t SELECT 1", autocommit=True, bulk_hint=True)
        assert result.decision == RoutingDecision.DIRECT
        assert "eligible" in result.reason.lower() or "SELECT" in result.reason

    def test_no_autocommit_routes_direct(self):
        router = self._make_router()
        conn = MagicMock()
        result = router.route(conn, "SELECT * FROM t", autocommit=False)
        assert result.decision == RoutingDecision.DIRECT

    def test_temp_table_routes_direct(self):
        router = self._make_router()
        conn = MagicMock()
        result = router.route(conn, "SELECT * FROM #temp_data", autocommit=True, bulk_hint=True)
        assert result.decision == RoutingDecision.DIRECT


class TestAutoRouterPhase2:
    """Phase 2: EXPLAIN-based heuristic routing."""

    def _make_router(self, **overrides):
        defaults = dict(
            require_hint=False, use_explain=True,
            threshold_rows=100_000, threshold_bytes=50_000_000,
        )
        defaults.update(overrides)
        return AutoRouter(AutoModeConfig(**defaults))

    def _mock_conn_explain(self, plan_text):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        cursor.fetchall.return_value = [(plan_text,)]
        return conn

    def test_large_query_routes_bulk(self):
        router = self._make_router()
        conn = self._mock_conn_explain(
            "XN Seq Scan on events  (cost=0.00..1000.00 rows=500000 width=200)"
        )
        result = router.route(conn, "SELECT * FROM events", autocommit=True)
        assert result.decision == RoutingDecision.BULK
        assert result.estimated_rows == 500000

    def test_small_query_routes_direct(self):
        router = self._make_router()
        conn = self._mock_conn_explain(
            "XN Seq Scan on users  (cost=0.00..10.00 rows=100 width=50)"
        )
        result = router.route(conn, "SELECT * FROM users", autocommit=True)
        assert result.decision == RoutingDecision.DIRECT
        assert result.estimated_rows == 100

    def test_threshold_boundary_rows(self):
        router = self._make_router(threshold_rows=1000)
        conn = self._mock_conn_explain(
            "XN Seq Scan on t  (cost=0.00..10.00 rows=1000 width=10)"
        )
        result = router.route(conn, "SELECT * FROM t", autocommit=True)
        assert result.decision == RoutingDecision.BULK

    def test_threshold_boundary_bytes(self):
        router = self._make_router(threshold_rows=1_000_000, threshold_bytes=1000)
        conn = self._mock_conn_explain(
            "XN Seq Scan on t  (cost=0.00..10.00 rows=100 width=20)"
        )
        # 100 rows * 20 width = 2000 bytes > 1000 threshold
        result = router.route(conn, "SELECT * FROM t", autocommit=True)
        assert result.decision == RoutingDecision.BULK

    def test_explain_failure_falls_back_to_direct(self):
        router = self._make_router()
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        cursor.execute.side_effect = Exception("EXPLAIN failed")
        result = router.route(conn, "SELECT * FROM t", autocommit=True)
        assert result.decision == RoutingDecision.DIRECT

    def test_explain_no_rows_falls_back(self):
        router = self._make_router()
        conn = self._mock_conn_explain("Some plan without rows or width info")
        result = router.route(conn, "SELECT * FROM t", autocommit=True)
        assert result.decision == RoutingDecision.DIRECT


class TestAutoModeConfig:
    def test_defaults(self):
        cfg = AutoModeConfig()
        assert cfg.threshold_rows == 100_000
        assert cfg.threshold_bytes == 50_000_000
        assert cfg.require_hint is True

    def test_custom(self):
        cfg = AutoModeConfig(threshold_rows=500, threshold_bytes=1000, require_hint=False)
        assert cfg.threshold_rows == 500
        assert cfg.require_hint is False


class TestReadMode:
    def test_values(self):
        assert ReadMode.DIRECT.value == "direct"
        assert ReadMode.UNLOAD.value == "unload"
        assert ReadMode.AUTO.value == "auto"
