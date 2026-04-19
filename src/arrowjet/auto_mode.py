"""
Auto Mode — smart routing between direct fetch and bulk (UNLOAD) paths.

Phase 1: Conservative — only routes to bulk with explicit hints
Phase 2: Heuristic — uses EXPLAIN to estimate result size

The router never silently changes semantics. If there's any ambiguity
about whether bulk routing is safe, it falls back to direct execution.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from .bulk.eligibility import check_read_eligibility, EligibilityResult

logger = logging.getLogger(__name__)


class ReadMode(Enum):
    DIRECT = "direct"     # Always use PG wire protocol
    UNLOAD = "unload"     # Always use UNLOAD (explicit bulk)
    AUTO = "auto"         # Router decides per-query


class RoutingDecision(Enum):
    DIRECT = "direct"
    BULK = "bulk"


@dataclass
class RoutingResult:
    """Result of a routing decision with explanation."""
    decision: RoutingDecision
    reason: str
    estimated_rows: Optional[int] = None
    estimated_bytes: Optional[int] = None

    def __bool__(self):
        return self.decision == RoutingDecision.BULK


@dataclass
class AutoModeConfig:
    """Configuration for auto mode routing."""
    threshold_rows: int = 100_000
    threshold_bytes: int = 50_000_000  # 50 MB
    use_explain: bool = True
    # Phase 1: only route to bulk with explicit hint
    require_hint: bool = True


class AutoRouter:
    """
    Decides whether a query should use direct fetch or UNLOAD bulk path.

    Usage:
        router = AutoRouter(config)
        result = router.route(conn, query, autocommit=True)
        if result.decision == RoutingDecision.BULK:
            # use read_bulk
        else:
            # use direct fetch
    """

    def __init__(self, config: Optional[AutoModeConfig] = None):
        self._config = config or AutoModeConfig()

    @property
    def config(self) -> AutoModeConfig:
        return self._config

    def route(
        self,
        conn,
        query: str,
        autocommit: bool = True,
        bulk_hint: bool = False,
    ) -> RoutingResult:
        """
        Decide routing for a query.

        Args:
            conn: database connection (for EXPLAIN)
            query: SQL query
            autocommit: whether connection is in autocommit mode
            bulk_hint: explicit user hint to prefer bulk

        Returns:
            RoutingResult with decision and explanation
        """
        # Step 1: Check eligibility (safety rules)
        eligibility = check_read_eligibility(
            query=query,
            autocommit=autocommit,
            explicit_mode=False,
            staging_valid=True,
        )
        if not eligibility:
            return RoutingResult(
                RoutingDecision.DIRECT,
                f"Not eligible for bulk: {eligibility.reason}",
            )

        # Step 2: Phase 1 — require explicit hint
        if self._config.require_hint and not bulk_hint:
            return RoutingResult(
                RoutingDecision.DIRECT,
                "Auto mode requires bulk_hint=True (Phase 1 conservative mode)",
            )

        # Step 3: If hint provided, route to bulk
        if bulk_hint:
            return RoutingResult(
                RoutingDecision.BULK,
                "User provided bulk_hint=True",
            )

        # Step 4: Phase 2 — EXPLAIN-based estimation
        if self._config.use_explain:
            estimate = self._estimate_size(conn, query)
            if estimate:
                rows, width = estimate
                est_bytes = rows * width
                if rows >= self._config.threshold_rows or est_bytes >= self._config.threshold_bytes:
                    return RoutingResult(
                        RoutingDecision.BULK,
                        f"EXPLAIN estimate: {rows:,} rows, ~{est_bytes:,} bytes "
                        f"(thresholds: {self._config.threshold_rows:,} rows, "
                        f"{self._config.threshold_bytes:,} bytes)",
                        estimated_rows=rows,
                        estimated_bytes=est_bytes,
                    )
                else:
                    return RoutingResult(
                        RoutingDecision.DIRECT,
                        f"EXPLAIN estimate below thresholds: {rows:,} rows, ~{est_bytes:,} bytes",
                        estimated_rows=rows,
                        estimated_bytes=est_bytes,
                    )

        # Default: direct
        return RoutingResult(
            RoutingDecision.DIRECT,
            "No hint, no EXPLAIN estimate — defaulting to direct",
        )

    def _estimate_size(self, conn, query: str) -> Optional[tuple[int, int]]:
        """
        Run EXPLAIN on a query and extract estimated rows and width.

        Returns (estimated_rows, estimated_width_bytes) or None if EXPLAIN fails.
        """
        try:
            cursor = conn.cursor()
            cursor.execute(f"EXPLAIN {query}")
            plan = "\n".join(str(row[0]) if isinstance(row, tuple) else str(row)
                            for row in cursor.fetchall())

            rows = self._extract_rows(plan)
            width = self._extract_width(plan)

            if rows is not None and width is not None:
                logger.debug(
                    "EXPLAIN estimate for query: rows=%d, width=%d",
                    rows, width,
                )
                return (rows, width)

            return None
        except Exception as e:
            logger.debug("EXPLAIN failed (falling back to direct): %s", e)
            return None

    def _extract_rows(self, plan: str) -> Optional[int]:
        """Extract estimated rows from EXPLAIN output."""
        # Redshift EXPLAIN format: rows=NNN
        match = re.search(r'rows=(\d+)', plan)
        if match:
            return int(match.group(1))
        return None

    def _extract_width(self, plan: str) -> Optional[int]:
        """Extract estimated width from EXPLAIN output."""
        # Redshift EXPLAIN format: width=NNN
        match = re.search(r'width=(\d+)', plan)
        if match:
            return int(match.group(1))
        return None
