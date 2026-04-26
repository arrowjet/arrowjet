"""
Cross-database transfer  - move data between any two supported databases.

Arrow is the universal in-memory format. Read from source as Arrow,
write to destination from Arrow. No intermediate files, no serialization
overhead, no shared infrastructure between source and destination.

Usage:
    import arrowjet

    result = arrowjet.transfer(
        source_engine=arrowjet.Engine(provider="postgresql"),
        source_conn=pg_conn,
        query="SELECT * FROM orders WHERE date > '2025-01-01'",
        dest_engine=arrowjet.Engine(provider="mysql"),
        dest_conn=mysql_conn,
        dest_table="orders",
    )
    print(f"Transferred {result.rows:,} rows in {result.total_time_s}s")

Supported paths (any combination):
    PostgreSQL -> MySQL
    PostgreSQL -> Redshift
    MySQL -> PostgreSQL
    MySQL -> Redshift
    Redshift -> PostgreSQL
    Redshift -> MySQL
    ... and any future provider
"""

from __future__ import annotations

import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)


def transfer(
    source_engine,
    source_conn,
    query: str,
    dest_engine,
    dest_conn,
    dest_table: str,
) -> "TransferResult":
    """
    Transfer data between any two databases via Arrow.

    Reads from source using the source engine's bulk read path,
    then writes to destination using the destination engine's bulk write path.
    Arrow Table is the in-memory bridge  - zero serialization overhead.

    Args:
        source_engine: Engine for the source database
        source_conn: DBAPI connection to the source database
        query: SELECT query to execute on the source
        dest_engine: Engine for the destination database
        dest_conn: DBAPI connection to the destination database
        dest_table: Target table name in the destination (must exist)

    Returns:
        TransferResult with row count, timing breakdown, and provider info
    """
    start = time.perf_counter()

    # Phase 1: Read from source
    t = time.perf_counter()
    read_result = source_engine.read_bulk(source_conn, query)
    read_time = time.perf_counter() - t

    arrow_table = read_result.table
    rows = read_result.rows

    if rows == 0:
        logger.info("Transfer: 0 rows from source query, nothing to write.")
        return TransferResult(
            rows=0,
            read_time_s=round(read_time, 3),
            write_time_s=0.0,
            total_time_s=round(time.perf_counter() - start, 3),
            source_provider=source_engine.provider,
            dest_provider=dest_engine.provider,
            dest_table=dest_table,
        )

    logger.info(
        "Transfer: read %d rows from %s in %.2fs",
        rows, source_engine.provider, read_time,
    )

    # Phase 2: Write to destination
    t = time.perf_counter()
    dest_engine.write_bulk(dest_conn, arrow_table, dest_table)
    write_time = time.perf_counter() - t

    total_time = time.perf_counter() - start

    logger.info(
        "Transfer: wrote %d rows to %s.%s in %.2fs (total=%.2fs)",
        rows, dest_engine.provider, dest_table, write_time, total_time,
    )

    return TransferResult(
        rows=rows,
        read_time_s=round(read_time, 3),
        write_time_s=round(write_time, 3),
        total_time_s=round(total_time, 3),
        source_provider=source_engine.provider,
        dest_provider=dest_engine.provider,
        dest_table=dest_table,
    )


class TransferResult:
    """Result of a cross-database transfer operation."""

    __slots__ = (
        "rows", "read_time_s", "write_time_s", "total_time_s",
        "source_provider", "dest_provider", "dest_table",
    )

    def __init__(self, rows, read_time_s, write_time_s, total_time_s,
                 source_provider, dest_provider, dest_table):
        self.rows = rows
        self.read_time_s = read_time_s
        self.write_time_s = write_time_s
        self.total_time_s = total_time_s
        self.source_provider = source_provider
        self.dest_provider = dest_provider
        self.dest_table = dest_table

    def __repr__(self):
        return (
            f"TransferResult(rows={self.rows:,}, "
            f"{self.source_provider}->{self.dest_provider}, "
            f"total={self.total_time_s}s)"
        )
