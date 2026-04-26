"""
Write benchmark: INSERT baseline (batched, 50K rows) vs COPY bulk (1M rows).

INSERT baseline uses write_dataframe in 1K-row batches with commits and progress logging.
COPY bulk uses Arrow -> Parquet -> S3 -> COPY on the full 1M rows.
INSERT rate is extrapolated to 1M for comparison.
"""

import sys
import time
import numpy as np
import pyarrow as pa
import redshift_connector
from config_loader import load_config
from bulk_ops import write_bulk

sys.stdout.reconfigure(line_buffering=True)

BASELINE_ROWS = 50_000
BASELINE_BATCH = 1_000
BULK_ROWS = 1_000_000


def generate_arrow_table(row_count: int, cfg: dict) -> pa.Table:
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
        raw = np.random.bytes(row_count * 16)
        strings = [raw[j*16:(j+1)*16].hex()[:str_len] for j in range(row_count)]
        arrays[f"str_col_{i}"] = pa.array(strings, type=pa.string())
    return pa.table(arrays)


def create_target_table(conn, table_name: str, cfg: dict):
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    col_defs = []
    for i in range(cfg["benchmark"]["num_int_cols"]):
        col_defs.append(f"int_col_{i} BIGINT")
    for i in range(cfg["benchmark"]["num_float_cols"]):
        col_defs.append(f"float_col_{i} DOUBLE PRECISION")
    for i in range(cfg["benchmark"]["num_string_cols"]):
        col_defs.append(f"str_col_{i} VARCHAR({cfg['benchmark']['string_length']})")
    cursor.execute(f"CREATE TABLE {table_name} ({', '.join(col_defs)})")
    conn.commit()


def run_baseline_batched(conn, cfg):
    """INSERT baseline: write_dataframe in 1K batches with commits and progress."""
    print(f"\n--- BASELINE: write_dataframe INSERT ({BASELINE_ROWS:,} rows, {BASELINE_BATCH:,}/batch) ---")

    print(f"  Generating {BASELINE_ROWS:,} rows...")
    table = generate_arrow_table(BASELINE_ROWS, cfg)
    df = table.to_pandas()
    print(f"  Data ready: {table.nbytes / 1024 / 1024:.1f} MB")

    create_target_table(conn, "write_test_baseline", cfg)
    cursor = conn.cursor()

    total_written = 0
    start = time.perf_counter()

    for batch_start in range(0, BASELINE_ROWS, BASELINE_BATCH):
        batch_end = min(batch_start + BASELINE_BATCH, BASELINE_ROWS)
        batch_df = df.iloc[batch_start:batch_end]

        batch_t = time.perf_counter()
        cursor.write_dataframe(batch_df, "write_test_baseline")
        conn.commit()
        batch_elapsed = time.perf_counter() - batch_t

        total_written += len(batch_df)
        total_elapsed = time.perf_counter() - start
        rate = total_written / total_elapsed

        print(f"  Batch {total_written:>7,}/{BASELINE_ROWS:,}  - "
              f"batch: {batch_elapsed:.1f}s, "
              f"total: {total_elapsed:.1f}s, "
              f"rate: {rate:,.0f} rows/s")

    total_time = time.perf_counter() - start
    rate = BASELINE_ROWS / total_time
    extrapolated_1m = BULK_ROWS / rate

    print(f"\n  RESULT: {BASELINE_ROWS:,} rows in {total_time:.2f}s ({rate:,.0f} rows/s)")
    print(f"  Extrapolated 1M rows: ~{extrapolated_1m:.0f}s ({extrapolated_1m/60:.1f} min)")

    return {
        "method": "insert_batched",
        "rows_written": BASELINE_ROWS,
        "duration_s": round(total_time, 2),
        "rows_per_sec": round(rate),
        "extrapolated_1m_s": round(extrapolated_1m),
    }


def run_bulk(conn, cfg):
    """COPY bulk: Arrow -> Parquet -> S3 -> COPY on full 1M rows."""
    stg = cfg["staging"]

    print(f"\n--- BULK: Arrow -> Parquet -> S3 -> COPY ({BULK_ROWS:,} rows) ---")

    print(f"  Generating {BULK_ROWS:,} rows...")
    gen_start = time.perf_counter()
    table = generate_arrow_table(BULK_ROWS, cfg)
    gen_time = time.perf_counter() - gen_start
    print(f"  Data ready: {table.nbytes / 1024 / 1024:.1f} MB (generated in {gen_time:.1f}s)")

    create_target_table(conn, "write_test_bulk", cfg)

    print(f"  Running COPY pipeline...")
    start = time.perf_counter()
    write_bulk(
        conn=conn,
        table=table,
        target_table="write_test_bulk",
        bucket=stg["bucket"],
        prefix=stg["prefix"],
        iam_role=stg["iam_role"],
        region=stg["region"],
    )
    bulk_time = time.perf_counter() - start

    rate = BULK_ROWS / bulk_time
    print(f"\n  RESULT: {BULK_ROWS:,} rows in {bulk_time:.2f}s ({rate:,.0f} rows/s)")

    return {
        "method": "copy_bulk",
        "rows_written": BULK_ROWS,
        "duration_s": round(bulk_time, 2),
        "rows_per_sec": round(rate),
    }


def main():
    cfg = load_config()
    rs = cfg["redshift"]

    conn = redshift_connector.connect(
        host=rs["host"],
        port=rs.get("port", 5439),
        database=rs["database"],
        user=rs["user"],
        password=rs["password"],
    )

    # Run baseline (batched INSERT, 50K rows)
    baseline = run_baseline_batched(conn, cfg)

    # Run bulk (COPY, 1M rows)
    bulk = run_bulk(conn, cfg)

    # Comparison
    print(f"\n{'='*60}")
    print("COMPARISON")
    print(f"{'='*60}")
    print(f"  INSERT rate:     {baseline['rows_per_sec']:,} rows/s (from {BASELINE_ROWS:,} rows)")
    print(f"  COPY rate:       {bulk['rows_per_sec']:,} rows/s (from {BULK_ROWS:,} rows)")
    print(f"  INSERT for 1M:   ~{baseline['extrapolated_1m_s']}s ({baseline['extrapolated_1m_s']/60:.1f} min)")
    print(f"  COPY for 1M:     {bulk['duration_s']}s")
    speedup = baseline['extrapolated_1m_s'] / bulk['duration_s']
    print(f"  >> Write speedup: {speedup:.0f}x")

    # Verify row counts
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM write_test_baseline")
    print(f"\n  Baseline rows verified: {cursor.fetchone()[0]:,}")
    cursor.execute("SELECT COUNT(*) FROM write_test_bulk")
    print(f"  Bulk rows verified: {cursor.fetchone()[0]:,}")

    conn.close()


if __name__ == "__main__":
    main()
