"""
Write benchmark: COPY bulk only (1M rows).
INSERT baseline already measured at 12 rows/sec (see WORKLOG).
"""

import sys
import time
import numpy as np
import pyarrow as pa
import redshift_connector
from config_loader import load_config
from bulk_ops import write_bulk

sys.stdout.reconfigure(line_buffering=True)

BULK_ROWS = 1_000_000


def generate_arrow_table(row_count, cfg):
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


def create_target_table(conn, table_name, cfg):
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


def main():
    cfg = load_config()
    rs = cfg["redshift"]
    stg = cfg["staging"]

    conn = redshift_connector.connect(
        host=rs["host"],
        port=rs.get("port", 5439),
        database=rs["database"],
        user=rs["user"],
        password=rs["password"],
    )

    print(f"Generating {BULK_ROWS:,} rows...")
    gen_start = time.perf_counter()
    table = generate_arrow_table(BULK_ROWS, cfg)
    gen_time = time.perf_counter() - gen_start
    print(f"  Data ready: {table.nbytes / 1024 / 1024:.1f} MB (generated in {gen_time:.1f}s)")

    create_target_table(conn, "write_test_bulk", cfg)

    print(f"\n--- COPY: Arrow -> Parquet -> S3 -> COPY ({BULK_ROWS:,} rows) ---")
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

    print(f"  {BULK_ROWS:,} rows in {bulk_time:.2f}s ({rate:,.0f} rows/s)")

    # Verify
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM write_test_bulk")
    actual = cursor.fetchone()[0]
    print(f"  Verified: {actual:,} rows in table")

    # Compare with INSERT baseline
    insert_rate = 12  # rows/sec from batched INSERT benchmark
    insert_time_1m = BULK_ROWS / insert_rate
    speedup = insert_time_1m / bulk_time

    print(f"\n--- COMPARISON ---")
    print(f"  INSERT rate:   {insert_rate} rows/s (measured from 29K rows)")
    print(f"  COPY rate:     {rate:,.0f} rows/s")
    print(f"  INSERT for 1M: ~{insert_time_1m:.0f}s ({insert_time_1m/60:.1f} min)")
    print(f"  COPY for 1M:   {bulk_time:.2f}s")
    print(f"  >> Speedup:    {speedup:,.0f}x")

    conn.close()


if __name__ == "__main__":
    main()
