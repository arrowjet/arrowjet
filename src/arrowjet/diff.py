"""
Data diff engine -- row-level comparison between source and destination.

Compares two Arrow tables by key columns and classifies every row as:
  - added: exists in source but not in destination
  - removed: exists in destination but not in source
  - changed: exists in both but non-key columns differ
  - unchanged: exists in both with identical values

Works across any two ArrowJet-supported databases (PG, MySQL, Redshift)
or within the same database (two tables or query vs table).

Usage:
    import arrowjet
    result = arrowjet.diff(
        source_engine=pg_engine, source_conn=pg_conn,
        dest_engine=mysql_engine, dest_conn=mysql_conn,
        query="SELECT * FROM orders",
        dest_table="orders",
        key_columns=["id"],
    )
    print(result)
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, List, Optional, Sequence

import pyarrow as pa
import pyarrow.compute as pc

logger = logging.getLogger(__name__)


@dataclass
class DiffResult:
    """Result of a diff operation."""

    added_count: int = 0
    removed_count: int = 0
    changed_count: int = 0
    unchanged_count: int = 0
    total_source: int = 0
    total_dest: int = 0
    changed_columns: List[str] = field(default_factory=list)
    schema_diff: Optional[dict] = None
    added_rows: Optional[pa.Table] = None
    removed_rows: Optional[pa.Table] = None
    changed_rows: Optional[pa.Table] = None
    total_time_s: float = 0.0

    @property
    def has_differences(self) -> bool:
        return self.added_count > 0 or self.removed_count > 0 or self.changed_count > 0

    def summary(self) -> str:
        """Human-readable summary."""
        lines = []
        lines.append(f"Source: {self.total_source:,} rows | Dest: {self.total_dest:,} rows")
        if not self.has_differences:
            lines.append("No differences found.")
        else:
            if self.added_count:
                lines.append(f"  Added (in source, not in dest): {self.added_count:,}")
            if self.removed_count:
                lines.append(f"  Removed (in dest, not in source): {self.removed_count:,}")
            if self.changed_count:
                cols = ", ".join(self.changed_columns) if self.changed_columns else "unknown"
                lines.append(f"  Changed: {self.changed_count:,} (columns: {cols})")
            lines.append(f"  Unchanged: {self.unchanged_count:,}")
        if self.schema_diff:
            if self.schema_diff.get("source_only"):
                lines.append(f"  Columns only in source: {self.schema_diff['source_only']}")
            if self.schema_diff.get("dest_only"):
                lines.append(f"  Columns only in dest: {self.schema_diff['dest_only']}")
        return "\n".join(lines)

    def __repr__(self):
        return (
            f"DiffResult(added={self.added_count}, removed={self.removed_count}, "
            f"changed={self.changed_count}, unchanged={self.unchanged_count})"
        )


def diff_tables(
    source: pa.Table,
    dest: pa.Table,
    key_columns: Sequence[str],
    include_rows: bool = False,
) -> DiffResult:
    """
    Compare two Arrow tables by key columns.

    Args:
        source: Source Arrow table (the "expected" state).
        dest: Destination Arrow table (the "actual" state).
        key_columns: Column(s) that uniquely identify a row.
        include_rows: If True, populate added_rows/removed_rows/changed_rows
            in the result. Default False (counts only, saves memory).

    Returns:
        DiffResult with counts and optional row details.
    """
    start = time.perf_counter()

    # Validate key columns exist
    for col in key_columns:
        if col not in source.column_names:
            raise ValueError(f"Key column '{col}' not found in source table. "
                             f"Available: {source.column_names}")
        if col not in dest.column_names:
            raise ValueError(f"Key column '{col}' not found in dest table. "
                             f"Available: {dest.column_names}")

    # Detect schema differences
    source_cols = set(source.column_names)
    dest_cols = set(dest.column_names)
    schema_diff = None
    if source_cols != dest_cols:
        schema_diff = {
            "source_only": sorted(source_cols - dest_cols),
            "dest_only": sorted(dest_cols - source_cols),
        }

    # Use only common columns for comparison (excluding key-only mismatches)
    common_cols = sorted(source_cols & dest_cols)
    value_cols = [c for c in common_cols if c not in key_columns]

    # Align both tables to common columns
    source_aligned = source.select(common_cols)
    dest_aligned = dest.select(common_cols)

    # Build key arrays for matching
    if len(key_columns) == 1:
        src_keys = source_aligned.column(key_columns[0])
        dst_keys = dest_aligned.column(key_columns[0])
    else:
        # Composite key: concatenate as strings
        src_parts = [pc.cast(source_aligned.column(k), pa.string()) for k in key_columns]
        dst_parts = [pc.cast(dest_aligned.column(k), pa.string()) for k in key_columns]
        sep = pa.scalar("|")
        src_keys = src_parts[0]
        dst_keys = dst_parts[0]
        for p in src_parts[1:]:
            src_keys = pc.binary_join_element_wise(src_keys, p, sep)
        for p in dst_parts[1:]:
            dst_keys = pc.binary_join_element_wise(dst_keys, p, sep)

    # Convert to Python sets for fast lookup
    src_key_set = set(src_keys.to_pylist())
    dst_key_set = set(dst_keys.to_pylist())

    added_keys = src_key_set - dst_key_set
    removed_keys = dst_key_set - src_key_set
    common_keys = src_key_set & dst_key_set

    # Find changed rows among common keys
    changed_count = 0
    changed_columns_set = set()
    changed_indices_src = []

    if common_keys and value_cols:
        # Build lookup dicts for fast comparison
        src_key_list = src_keys.to_pylist()
        dst_key_list = dst_keys.to_pylist()

        # Map key -> row index
        src_idx = {k: i for i, k in enumerate(src_key_list) if k in common_keys}
        dst_idx = {k: i for i, k in enumerate(dst_key_list) if k in common_keys}

        # Compare value columns for common keys
        for key in common_keys:
            si = src_idx[key]
            di = dst_idx[key]
            row_changed = False
            for col in value_cols:
                sv = source_aligned.column(col)[si].as_py()
                dv = dest_aligned.column(col)[di].as_py()
                if sv != dv:
                    row_changed = True
                    changed_columns_set.add(col)
            if row_changed:
                changed_count += 1
                changed_indices_src.append(src_idx[key])

    unchanged_count = len(common_keys) - changed_count

    # Build row details if requested
    added_rows = None
    removed_rows = None
    changed_rows = None

    if include_rows:
        if added_keys:
            src_key_list = src_keys.to_pylist()
            mask = [k in added_keys for k in src_key_list]
            added_rows = source_aligned.filter(mask)

        if removed_keys:
            dst_key_list = dst_keys.to_pylist()
            mask = [k in removed_keys for k in dst_key_list]
            removed_rows = dest_aligned.filter(mask)

        if changed_indices_src:
            changed_rows = source_aligned.take(changed_indices_src)

    total_time = time.perf_counter() - start

    result = DiffResult(
        added_count=len(added_keys),
        removed_count=len(removed_keys),
        changed_count=changed_count,
        unchanged_count=unchanged_count,
        total_source=source.num_rows,
        total_dest=dest.num_rows,
        changed_columns=sorted(changed_columns_set),
        schema_diff=schema_diff,
        added_rows=added_rows,
        removed_rows=removed_rows,
        changed_rows=changed_rows,
        total_time_s=round(total_time, 3),
    )

    logger.info(
        "Diff complete: added=%d, removed=%d, changed=%d, unchanged=%d in %.2fs",
        result.added_count, result.removed_count,
        result.changed_count, result.unchanged_count, result.total_time_s,
    )

    return result


def diff(
    source_engine: Any,
    source_conn: Any,
    dest_engine: Any,
    dest_conn: Any,
    query: str,
    dest_table: str,
    key_columns: Sequence[str],
    include_rows: bool = False,
) -> DiffResult:
    """
    Compare source query results against a destination table.

    Reads from both sides using ArrowJet engines, then diffs in memory.

    Args:
        source_engine: ArrowJet Engine for the source database.
        source_conn: DBAPI connection to the source database.
        dest_engine: ArrowJet Engine for the destination database.
        dest_conn: DBAPI connection to the destination database.
        query: SELECT query to run on the source.
        dest_table: Table name to read from the destination.
        key_columns: Column(s) that uniquely identify a row.
        include_rows: If True, include actual row data in the result.

    Returns:
        DiffResult with counts and optional row details.
    """
    start = time.perf_counter()

    logger.info("Reading source: %s", query[:100])
    source_result = source_engine.read_bulk(source_conn, query)
    source_table = source_result.table

    dest_query = f"SELECT * FROM {dest_table}"
    logger.info("Reading dest: %s", dest_query)
    dest_result = dest_engine.read_bulk(dest_conn, dest_query)
    dest_table_data = dest_result.table

    result = diff_tables(source_table, dest_table_data, key_columns, include_rows)

    # Override total_time to include read time
    result.total_time_s = round(time.perf_counter() - start, 3)

    return result
