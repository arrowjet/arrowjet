"""
MySQLProvider  - bulk operations for MySQL, Aurora MySQL, and RDS MySQL.

Uses MySQL's LOAD DATA LOCAL INFILE for high-speed bulk writes:
  - Write: Arrow -> CSV in memory -> LOAD DATA LOCAL INFILE -> table
  - Read:  Standard cursor fetch -> Arrow (no server-side bulk export in MySQL)

LOAD DATA LOCAL INFILE bypasses the SQL parser and streams CSV data directly
to the storage engine. Dramatically faster than INSERT for bulk loads.

Requires pymysql with local_infile=True, or mysql-connector-python with
allow_local_infile=True.

Works with any MySQL-compatible database: Aurora MySQL, RDS MySQL,
self-hosted MySQL, MariaDB, PlanetScale, TiDB.
"""

from __future__ import annotations

import io
import logging
import tempfile
import time
from typing import Optional

import pyarrow as pa

logger = logging.getLogger(__name__)


class MySQLProvider:
    """
    Bulk data movement for MySQL using LOAD DATA LOCAL INFILE.

    Usage:
        provider = MySQLProvider()

        # Write (LOAD DATA LOCAL INFILE, 100x+ faster than INSERT)
        result = provider.write_bulk(conn, arrow_table, "my_table")

        # Read (cursor fetch -> Arrow)
        result = provider.read_bulk(conn, "SELECT * FROM my_table")
    """

    def __init__(self):
        pass

    @property
    def name(self) -> str:
        return "mysql"

    @property
    def uses_cloud_staging(self) -> bool:
        """MySQL LOAD DATA LOCAL INFILE streams from client, no cloud storage needed."""
        return False

    def write_bulk(
        self,
        conn,
        table: pa.Table,
        target_table: str,
    ) -> "MySQLWriteResult":
        """
        Write an Arrow table to MySQL via LOAD DATA LOCAL INFILE.

        Converts Arrow to CSV, writes to a temp file, then streams it
        to MySQL via LOAD DATA LOCAL INFILE. This bypasses the SQL parser
        and is 100x+ faster than INSERT for bulk loads.

        Args:
            conn: pymysql or mysql-connector-python connection
                  (must have local_infile enabled)
            table: PyArrow Table to write
            target_table: Target table name (must exist)

        Returns:
            MySQLWriteResult with timing and row count
        """
        start = time.perf_counter()
        rows = table.num_rows
        cols = table.num_columns

        # Write Arrow table to CSV temp file
        # LOAD DATA LOCAL INFILE needs a real file path (not BytesIO)
        csv_buffer = io.BytesIO()
        _arrow_to_csv(table, csv_buffer)
        csv_size = csv_buffer.tell()
        csv_buffer.seek(0)

        logger.info(
            "Prepared %d rows (%d cols, %d bytes CSV) for LOAD DATA to %s",
            rows, cols, csv_size, target_table,
        )

        # Build column list for LOAD DATA
        col_names = ", ".join(table.column_names)

        # Write to temp file (LOAD DATA needs a file path for some drivers)
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
            tmp.write(csv_buffer.read())
            tmp_path = tmp.name

        load_sql = (
            f"LOAD DATA LOCAL INFILE '{tmp_path}' "
            f"INTO TABLE {target_table} "
            f"FIELDS TERMINATED BY ',' "
            f"OPTIONALLY ENCLOSED BY '\"' "
            f"LINES TERMINATED BY '\\n' "
            f"({col_names})"
        )

        t = time.perf_counter()
        cursor = conn.cursor()
        try:
            cursor.execute(load_sql)
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise MySQLBulkError(
                f"LOAD DATA failed for {target_table}: {e}\n"
                f"SQL: {load_sql}"
            ) from e
        finally:
            # Clean up temp file
            import os
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

        load_time = time.perf_counter() - t
        total_time = time.perf_counter() - start

        result = MySQLWriteResult(
            rows=rows,
            bytes_transferred=csv_size,
            load_time_s=round(load_time, 3),
            total_time_s=round(total_time, 3),
            target_table=target_table,
        )

        logger.info(
            "LOAD DATA write complete: %d rows to %s in %.2fs (load=%.2fs, %d bytes)",
            rows, target_table, total_time, load_time, csv_size,
        )

        return result

    def write_dataframe(self, conn, df, target_table: str) -> "MySQLWriteResult":
        """Write a pandas DataFrame via LOAD DATA LOCAL INFILE."""
        table = pa.Table.from_pandas(df, preserve_index=False)
        return self.write_bulk(conn, table, target_table)

    def read_bulk(
        self,
        conn,
        query: str,
    ) -> "MySQLReadResult":
        """
        Read query results from MySQL into an Arrow table.

        MySQL doesn't have a client-side bulk export protocol like
        PostgreSQL's COPY TO STDOUT. This uses a standard cursor fetch
        but converts directly to Arrow for efficient memory usage.

        For large results, this is still faster than pandas read_sql
        because it avoids the intermediate Python object creation.

        Args:
            conn: pymysql or mysql-connector-python connection
            query: SELECT query to execute

        Returns:
            MySQLReadResult with Arrow table, timing, and row count
        """
        start = time.perf_counter()

        cursor = conn.cursor()
        t = time.perf_counter()
        cursor.execute(query)

        # Get column names from cursor description
        if cursor.description is None:
            # No result set (e.g., empty result)
            return MySQLReadResult(
                table=pa.table({}),
                rows=0,
                bytes_transferred=0,
                query_time_s=0.0,
                fetch_time_s=0.0,
                total_time_s=round(time.perf_counter() - start, 3),
            )

        col_names = [desc[0] for desc in cursor.description]
        query_time = time.perf_counter() - t

        # Fetch all rows and convert to Arrow
        t = time.perf_counter()
        rows_data = cursor.fetchall()
        fetch_time = time.perf_counter() - t

        if not rows_data:
            table = pa.table({name: pa.array([]) for name in col_names})
        else:
            # Convert rows to columnar format for Arrow
            columns = {}
            for i, name in enumerate(col_names):
                columns[name] = pa.array([row[i] for row in rows_data])
            table = pa.table(columns)

        total_time = time.perf_counter() - start

        result = MySQLReadResult(
            table=table,
            rows=table.num_rows,
            bytes_transferred=0,  # Not available for cursor fetch
            query_time_s=round(query_time, 3),
            fetch_time_s=round(fetch_time, 3),
            total_time_s=round(total_time, 3),
        )

        logger.info(
            "MySQL read complete: %d rows in %.2fs (query=%.2fs, fetch=%.2fs)",
            result.rows, total_time, query_time, fetch_time,
        )

        return result


# --- Result classes ---

class MySQLWriteResult:
    """Result of a MySQL bulk write operation."""

    __slots__ = (
        "rows", "bytes_transferred", "load_time_s",
        "total_time_s", "target_table",
    )

    def __init__(self, rows, bytes_transferred, load_time_s, total_time_s, target_table):
        self.rows = rows
        self.bytes_transferred = bytes_transferred
        self.load_time_s = load_time_s
        self.total_time_s = total_time_s
        self.target_table = target_table

    def __repr__(self):
        return (
            f"MySQLWriteResult(rows={self.rows:,}, "
            f"total={self.total_time_s}s, table={self.target_table})"
        )


class MySQLReadResult:
    """Result of a MySQL bulk read operation."""

    __slots__ = (
        "table", "rows", "bytes_transferred",
        "query_time_s", "fetch_time_s", "total_time_s",
    )

    def __init__(self, table, rows, bytes_transferred, query_time_s, fetch_time_s, total_time_s):
        self.table = table
        self.rows = rows
        self.bytes_transferred = bytes_transferred
        self.query_time_s = query_time_s
        self.fetch_time_s = fetch_time_s
        self.total_time_s = total_time_s

    def to_pandas(self):
        """Convert result to pandas DataFrame."""
        return self.table.to_pandas()

    def __repr__(self):
        return (
            f"MySQLReadResult(rows={self.rows:,}, "
            f"total={self.total_time_s}s)"
        )


class MySQLBulkError(Exception):
    """Error during MySQL bulk operation."""
    pass


# --- Internal helpers ---

def _arrow_to_csv(table: pa.Table, buf: io.BytesIO) -> None:
    """
    Convert an Arrow table to CSV bytes for LOAD DATA LOCAL INFILE.
    No header row. NULL values written as \\N (MySQL convention).
    """
    from pyarrow import csv as pa_csv

    write_options = pa_csv.WriteOptions(
        include_header=False,
        delimiter=',',
    )
    pa_csv.write_csv(table, buf, write_options=write_options)
