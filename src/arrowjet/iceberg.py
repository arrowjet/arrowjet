"""
Iceberg output format -- write Arrow tables as Apache Iceberg tables.

Uses pyiceberg to commit proper Iceberg metadata (snapshots, manifests,
schema) instead of just dumping Parquet files. The result is queryable
from Athena, Redshift Spectrum, Spark, Trino, DuckDB, or any Iceberg-
compatible engine.

This module handles writing TO Iceberg. It does not manage Iceberg tables
(no compaction, snapshot expiry, or partition evolution). That's what
Spark/Trino/Athena do.

Requires: pip install arrowjet[iceberg]
"""

from __future__ import annotations

import logging
import time
from typing import Optional

import pyarrow as pa

logger = logging.getLogger(__name__)


class IcebergWriteResult:
    """Result of an Iceberg write operation."""

    __slots__ = (
        "rows", "table_name", "mode", "catalog_type",
        "warehouse", "total_time_s",
    )

    def __init__(self, rows, table_name, mode, catalog_type, warehouse, total_time_s):
        self.rows = rows
        self.table_name = table_name
        self.mode = mode
        self.catalog_type = catalog_type
        self.warehouse = warehouse
        self.total_time_s = total_time_s

    def __repr__(self):
        return (
            f"IcebergWriteResult(rows={self.rows:,}, table={self.table_name}, "
            f"mode={self.mode}, time={self.total_time_s}s)"
        )


def write_iceberg(
    table: pa.Table,
    table_name: str,
    warehouse: str,
    mode: str = "append",
    catalog_name: str = "default",
    catalog_type: str = "sql",
    catalog_properties: Optional[dict] = None,
) -> IcebergWriteResult:
    """
    Write an Arrow table to an Iceberg table.

    Creates the table if it doesn't exist. Appends or overwrites data.

    Args:
        table: PyArrow Table to write.
        table_name: Fully qualified table name (e.g., "analytics.orders").
        warehouse: Warehouse location (e.g., "s3://my-lake/warehouse").
        mode: "append" or "overwrite". Default: "append".
        catalog_name: Catalog name. Default: "default".
        catalog_type: Catalog type. Default: "sql" (SQLite-based, no server needed).
            Options: "sql", "rest", "glue", "hive".
        catalog_properties: Additional catalog properties (e.g., URI, credentials).

    Returns:
        IcebergWriteResult with row count, timing, and metadata.

    Raises:
        ImportError: If pyiceberg is not installed.
        ValueError: If mode is not "append" or "overwrite".
    """
    try:
        from pyiceberg.catalog import load_catalog
    except ImportError:
        raise ImportError(
            "pyiceberg is required for Iceberg output. "
            "Install it with: pip install arrowjet[iceberg]"
        )

    if mode not in ("append", "overwrite"):
        raise ValueError(f"mode must be 'append' or 'overwrite', got '{mode}'")

    # Parse namespace and table (validate early, before catalog)
    parts = table_name.split(".")
    if len(parts) < 2:
        raise ValueError(
            f"table_name must be fully qualified (namespace.table), got '{table_name}'"
        )

    start = time.perf_counter()

    # Build catalog properties
    props = {
        "type": catalog_type,
        "warehouse": warehouse,
    }
    if catalog_type == "sql":
        # SQLite-based catalog -- no server needed, stores metadata locally
        import os
        os.makedirs(warehouse, exist_ok=True)
        props["uri"] = f"sqlite:///{warehouse.rstrip('/')}/catalog.db"
    if catalog_properties:
        props.update(catalog_properties)

    logger.info("Loading Iceberg catalog '%s' (type=%s, warehouse=%s)",
                catalog_name, catalog_type, warehouse)

    catalog = load_catalog(catalog_name, **props)

    # Namespace and table already parsed above
    namespace = ".".join(parts[:-1])
    tbl_name = parts[-1]

    # Ensure namespace exists
    try:
        catalog.create_namespace(namespace)
        logger.info("Created namespace '%s'", namespace)
    except Exception:
        # Namespace already exists
        pass

    # Create or load table
    full_id = table_name
    try:
        iceberg_table = catalog.load_table(full_id)
        logger.info("Loaded existing Iceberg table '%s'", full_id)
    except Exception:
        # Table doesn't exist -- create from Arrow schema
        iceberg_table = catalog.create_table(
            identifier=full_id,
            schema=table.schema,
        )
        logger.info("Created new Iceberg table '%s' with %d columns",
                     full_id, len(table.schema))

    # Write data
    if mode == "append":
        iceberg_table.append(table)
    else:
        iceberg_table.overwrite(table)

    total_time = time.perf_counter() - start

    result = IcebergWriteResult(
        rows=table.num_rows,
        table_name=table_name,
        mode=mode,
        catalog_type=catalog_type,
        warehouse=warehouse,
        total_time_s=round(total_time, 3),
    )

    logger.info(
        "Iceberg write complete: %d rows to '%s' (%s) in %.2fs",
        result.rows, result.table_name, result.mode, result.total_time_s,
    )

    return result


def read_iceberg(
    table_name: str,
    warehouse: str,
    catalog_name: str = "default",
    catalog_type: str = "sql",
    catalog_properties: Optional[dict] = None,
    row_filter: Optional[str] = None,
    selected_fields: Optional[tuple] = None,
) -> pa.Table:
    """
    Read an Iceberg table into an Arrow table.

    Args:
        table_name: Fully qualified table name.
        warehouse: Warehouse location.
        catalog_name: Catalog name.
        catalog_type: Catalog type.
        catalog_properties: Additional catalog properties.
        row_filter: Optional filter expression (e.g., "amount > 100").
        selected_fields: Optional tuple of column names to select.

    Returns:
        PyArrow Table.
    """
    try:
        from pyiceberg.catalog import load_catalog
    except ImportError:
        raise ImportError(
            "pyiceberg is required for Iceberg support. "
            "Install it with: pip install arrowjet[iceberg]"
        )

    props = {
        "type": catalog_type,
        "warehouse": warehouse,
    }
    if catalog_type == "sql":
        props["uri"] = f"sqlite:///{warehouse.rstrip('/')}/catalog.db"
    if catalog_properties:
        props.update(catalog_properties)

    catalog = load_catalog(catalog_name, **props)
    iceberg_table = catalog.load_table(table_name)

    scan_kwargs = {}
    if row_filter is not None:
        scan_kwargs["row_filter"] = row_filter
    if selected_fields is not None:
        scan_kwargs["selected_fields"] = selected_fields

    scan = iceberg_table.scan(**scan_kwargs)

    return scan.to_arrow()
