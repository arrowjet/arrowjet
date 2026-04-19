"""
M0 Benchmark: Compares baseline (redshift_connector) vs prototype (UNLOAD/COPY via S3).

Measures:
  - Read: direct fetch vs read_bulk (UNLOAD -> S3 -> Parquet -> Arrow)
  - Write: INSERT vs write_bulk (Arrow -> Parquet -> S3 -> COPY)
"""

import json
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import redshift_connector

from bulk_ops import read_bulk, write_bulk
from config_loader import load_config


def get_connection(cfg: dict) -> redshift_connector.Connection:
    rs = cfg["redshift"]
    return redshift_connector.connect(
        host=rs["host"],
        port=rs.get("port", 5439),
        database=rs["database"],
        user=rs["user"],
        password=rs["password"],
    )


def benchmark_read_baseline(conn, table_name: str, row_count: int) -> dict:
    """Baseline: redshift_connector direct fetch -> DataFrame."""
    print(f"  [baseline] Reading {row_count:,} rows via direct fetch...")
    cursor = conn.cursor()

    start = time.perf_counter()
    cursor.execute(f"SELECT * FROM {table_name}")
    df = cursor.fetch_dataframe()
    elapsed = time.perf_counter() - start

    rows = len(df)
    print(f"  [baseline] {rows:,} rows in {elapsed:.2f}s")
    return {
        "method": "direct_fetch",
        "operation": "read",
        "row_count": rows,
        "duration_s": round(elapsed, 3),
        "rows_per_sec": round(rows / elapsed),
    }


def benchmark_read_bulk(conn, table_name: str, row_count: int, cfg: dict) -> dict:
    """Prototype: UNLOAD -> S3 -> Parquet -> Arrow."""
    stg = cfg["staging"]
    print(f"  [bulk] Reading {row_count:,} rows via UNLOAD...")

    start = time.perf_counter()
    arrow_table = read_bulk(
        conn=conn,
        query=f"SELECT * FROM {table_name}",
        bucket=stg["bucket"],
        prefix=stg["prefix"],
        iam_role=stg["iam_role"],
        region=stg["region"],
    )
    elapsed = time.perf_counter() - start

    rows = arrow_table.num_rows
    print(f"  [bulk] {rows:,} rows in {elapsed:.2f}s")
    return {
        "method": "unload_bulk",
        "operation": "read",
        "row_count": rows,
        "duration_s": round(elapsed, 3),
        "rows_per_sec": round(rows / elapsed),
    }


def _generate_arrow_table(row_count: int, cfg: dict) -> pa.Table:
    """Generate a PyArrow table with synthetic data for write benchmarks."""
    num_int = cfg["benchmark"]["num_int_cols"]
    num_float = cfg["benchmark"]["num_float_cols"]
    num_str = cfg["benchmark"]["num_string_cols"]
    str_len = cfg["benchmark"]["string_length"]

    arrays = {}
    for i in range(num_int):
        arrays[f"int_col_{i}"] = pa.array(
            np.random.randint(0, 1000000, size=row_count), type=pa.int64()
        )
    for i in range(num_float):
        arrays[f"float_col_{i}"] = pa.array(
            np.random.random(size=row_count) * 1000.0, type=pa.float64()
        )
    for i in range(num_str):
        arrays[f"str_col_{i}"] = pa.array(
            [uuid_str()[:str_len] for _ in range(row_count)], type=pa.string()
        )
    return pa.table(arrays)


def uuid_str() -> str:
    import uuid
    return uuid.uuid4().hex


def benchmark_write_baseline(conn, table_name: str, row_count: int, cfg: dict) -> dict:
    """Baseline: redshift_connector write_dataframe (INSERT-based)."""
    print(f"  [baseline] Writing {row_count:,} rows via INSERT...")

    # Generate pandas DataFrame
    arrow_table = _generate_arrow_table(row_count, cfg)
    df = arrow_table.to_pandas()

    # Create target table
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}_write_baseline")

    col_defs = []
    for i in range(cfg["benchmark"]["num_int_cols"]):
        col_defs.append(f"int_col_{i} BIGINT")
    for i in range(cfg["benchmark"]["num_float_cols"]):
        col_defs.append(f"float_col_{i} DOUBLE PRECISION")
    for i in range(cfg["benchmark"]["num_string_cols"]):
        col_defs.append(f"str_col_{i} VARCHAR({cfg['benchmark']['string_length']})")

    cursor.execute(
        f"CREATE TABLE {table_name}_write_baseline ({', '.join(col_defs)})"
    )
    conn.commit()

    start = time.perf_counter()
    cursor.write_dataframe(df, f"{table_name}_write_baseline")
    conn.commit()
    elapsed = time.perf_counter() - start

    print(f"  [baseline] {row_count:,} rows in {elapsed:.2f}s")
    return {
        "method": "insert_dataframe",
        "operation": "write",
        "row_count": row_count,
        "duration_s": round(elapsed, 3),
        "rows_per_sec": round(row_count / elapsed),
    }


def benchmark_write_bulk(conn, table_name: str, row_count: int, cfg: dict) -> dict:
    """Prototype: Arrow -> Parquet -> S3 -> COPY."""
    stg = cfg["staging"]
    print(f"  [bulk] Writing {row_count:,} rows via COPY...")

    arrow_table = _generate_arrow_table(row_count, cfg)

    # Create target table
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}_write_bulk")

    col_defs = []
    for i in range(cfg["benchmark"]["num_int_cols"]):
        col_defs.append(f"int_col_{i} BIGINT")
    for i in range(cfg["benchmark"]["num_float_cols"]):
        col_defs.append(f"float_col_{i} DOUBLE PRECISION")
    for i in range(cfg["benchmark"]["num_string_cols"]):
        col_defs.append(f"str_col_{i} VARCHAR({cfg['benchmark']['string_length']})")

    cursor.execute(
        f"CREATE TABLE {table_name}_write_bulk ({', '.join(col_defs)})"
    )
    conn.commit()

    start = time.perf_counter()
    write_bulk(
        conn=conn,
        table=arrow_table,
        target_table=f"{table_name}_write_bulk",
        bucket=stg["bucket"],
        prefix=stg["prefix"],
        iam_role=stg["iam_role"],
        region=stg["region"],
    )
    elapsed = time.perf_counter() - start

    print(f"  [bulk] {row_count:,} rows in {elapsed:.2f}s")
    return {
        "method": "copy_bulk",
        "operation": "write",
        "row_count": row_count,
        "duration_s": round(elapsed, 3),
        "rows_per_sec": round(row_count / elapsed),
    }


def run_benchmarks():
    cfg = load_config()
    prefix = cfg["benchmark"]["test_table_prefix"]
    row_counts = cfg["benchmark"]["row_counts"]
    results = []

    conn = get_connection(cfg)
    conn.autocommit = True

    for row_count in row_counts:
        table_name = f"{prefix}_{row_count // 1000000}m"
        label = f"{row_count // 1000000}M rows"
        print(f"\n{'='*60}")
        print(f"Benchmarking: {label}")
        print(f"{'='*60}")

        # Read benchmarks
        print(f"\n--- READ ({label}) ---")
        try:
            r_baseline = benchmark_read_baseline(conn, table_name, row_count)
            results.append(r_baseline)
        except Exception as e:
            print(f"  [baseline read] FAILED: {e}")
            r_baseline = None

        try:
            r_bulk = benchmark_read_bulk(conn, table_name, row_count, cfg)
            results.append(r_bulk)
        except Exception as e:
            print(f"  [bulk read] FAILED: {e}")
            r_bulk = None

        if r_baseline and r_bulk and r_baseline["duration_s"] > 0:
            speedup = r_baseline["duration_s"] / r_bulk["duration_s"]
            print(f"  >> Read speedup: {speedup:.1f}x")

        # Write benchmarks (use smaller counts for INSERT baseline — it's very slow)
        write_count = min(row_count, 1000000)  # Cap INSERT at 1M — it's too slow beyond that
        print(f"\n--- WRITE ({write_count:,} rows) ---")
        try:
            w_baseline = benchmark_write_baseline(conn, table_name, write_count, cfg)
            results.append(w_baseline)
        except Exception as e:
            print(f"  [baseline write] FAILED: {e}")
            w_baseline = None

        try:
            w_bulk = benchmark_write_bulk(conn, table_name, row_count, cfg)
            results.append(w_bulk)
        except Exception as e:
            print(f"  [bulk write] FAILED: {e}")
            w_bulk = None

        if w_baseline and w_bulk and w_baseline["duration_s"] > 0:
            speedup = w_baseline["duration_s"] / w_bulk["duration_s"]
            print(f"  >> Write speedup: {speedup:.1f}x (note: baseline capped at {write_count:,} rows)")

    conn.close()

    # Save results
    results_dir = Path(__file__).parent / "results"
    results_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = results_dir / f"benchmark_{timestamp}.json"

    with open(results_file, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\n{'='*60}")
    print(f"Results saved to: {results_file}")
    print(f"{'='*60}")

    # Print summary table
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"{'Method':<20} {'Op':<8} {'Rows':>12} {'Time (s)':>10} {'Rows/s':>14}")
    print("-" * 66)
    for r in results:
        print(
            f"{r['method']:<20} {r['operation']:<8} "
            f"{r['row_count']:>12,} {r['duration_s']:>10.2f} "
            f"{r['rows_per_sec']:>14,}"
        )

    return results


if __name__ == "__main__":
    run_benchmarks()
