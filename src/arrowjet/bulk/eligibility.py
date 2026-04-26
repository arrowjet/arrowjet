"""
Execution eligibility checker  - determines when UNLOAD routing is safe.

UNLOAD has different semantics than direct fetch:
  - Runs outside the current transaction
  - May not see uncommitted state
  - Cannot reference session-local temp tables
  - Always materializes the full result

This module defines the rules for when auto-routing to UNLOAD is allowed.
"""

from __future__ import annotations

import re
import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

# Patterns that suggest temp table usage
_TEMP_TABLE_PATTERN = re.compile(
    r'\b(#\w+|temp_\w+)\b', re.IGNORECASE
)

# Only SELECT statements can be UNLOADed
_SELECT_PATTERN = re.compile(
    r'^\s*(\(?\s*SELECT|WITH\s)', re.IGNORECASE
)

# DML/DDL keywords that disqualify a query even inside a CTE
_DML_DDL_PATTERN = re.compile(
    r'\b(INSERT\s+INTO|UPDATE\s|DELETE\s+FROM|CREATE\s|DROP\s|ALTER\s|TRUNCATE\s|GRANT\s|REVOKE\s)\b',
    re.IGNORECASE,
)

# SELECT INTO creates a table  - not a pure read
_SELECT_INTO_PATTERN = re.compile(
    r'\bSELECT\b.+?\bINTO\s+\w+', re.IGNORECASE | re.DOTALL
)

# FOR UPDATE/SHARE  - row-locking clauses, not supported by Redshift UNLOAD
_FOR_UPDATE_PATTERN = re.compile(
    r'\bFOR\s+(UPDATE|NO\s+KEY\s+UPDATE|SHARE|KEY\s+SHARE)\b', re.IGNORECASE
)

# Non-SELECT statements that might look like queries
_NON_SELECT_PATTERN = re.compile(
    r'^\s*(SHOW|EXPLAIN|CALL|EXECUTE|SET|RESET|BEGIN|COMMIT|ROLLBACK|LOCK|VACUUM|ANALYZE)\b',
    re.IGNORECASE,
)


@dataclass
class EligibilityResult:
    """Result of an eligibility check."""
    eligible: bool
    reason: str = ""

    def __bool__(self):
        return self.eligible


def check_read_eligibility(
    query: str,
    autocommit: bool,
    explicit_mode: bool = False,
    staging_valid: bool = True,
) -> EligibilityResult:
    """
    Check if a query is eligible for UNLOAD routing.

    All conditions must be true for auto-routing:
      1. Autocommit mode OR explicit read_mode=unload
      2. Query does not reference temp tables
      3. Query is a SELECT (not DML, DDL, cursor)
      4. Staging configuration is valid

    Args:
        query: SQL query string
        autocommit: Whether connection is in autocommit mode
        explicit_mode: Whether user explicitly requested UNLOAD mode
        staging_valid: Whether staging config is valid and reachable

    Returns:
        EligibilityResult with eligible flag and reason if not eligible
    """
    # Explicit mode bypasses autocommit check
    if not explicit_mode and not autocommit:
        return EligibilityResult(
            False,
            "Not in autocommit mode. UNLOAD runs outside transactions "
            "and may not see uncommitted changes. Use read_mode='unload' "
            "to override or enable autocommit.",
        )

    if not staging_valid:
        return EligibilityResult(
            False,
            "Staging configuration is not valid. Cannot use UNLOAD path.",
        )

    if not _is_select_query(query):
        return EligibilityResult(
            False,
            "Query is not a SELECT statement. UNLOAD only supports SELECT.",
        )

    if _references_temp_tables(query):
        return EligibilityResult(
            False,
            "Query may reference temporary tables. Temp tables may not be "
            "visible to UNLOAD. Use read_mode='direct' for this query.",
        )

    return EligibilityResult(True)


def _is_select_query(query: str) -> bool:
    """
    Check if query is a pure SELECT (or WITH...SELECT).

    Rejects:
      - DML (INSERT, UPDATE, DELETE) even inside CTEs
      - DDL (CREATE, DROP, ALTER, TRUNCATE)
      - Non-query statements (SHOW, EXPLAIN, CALL, SET, etc.)
    """
    stripped = query.strip()

    # Reject non-query statements first
    if _NON_SELECT_PATTERN.match(stripped):
        return False

    # Must start with SELECT or WITH
    if not _SELECT_PATTERN.match(stripped):
        return False

    # Reject if DML/DDL keywords appear anywhere (catches CTE + DML)
    if _DML_DDL_PATTERN.search(stripped):
        return False

    # Reject SELECT INTO (creates a table, not a pure read)
    if _SELECT_INTO_PATTERN.search(stripped):
        return False

    # Reject FOR UPDATE/SHARE (row-locking, not supported by UNLOAD)
    if _FOR_UPDATE_PATTERN.search(stripped):
        return False

    return True


def _references_temp_tables(query: str) -> bool:
    """Heuristic check for temp table references."""
    return bool(_TEMP_TABLE_PATTERN.search(query))
