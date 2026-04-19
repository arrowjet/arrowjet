"""
Rigorous write benchmark: 3 iterations of each variant with all precautions.

Precautions:
  1. Disable Redshift result cache
  2. Verify S3 staging prefix is empty before each run
  3. DROP + CREATE target table before each run (clean state)
  4. Same input Arrow table for all runs
  5. Same Parquet compression (snappy)
  6. Same single file
  7. Same COPY options
  8. Cleanup excluded from timing
  9. 10-second pause between runs to let Redshift WLM settle
  10. Report min/max/median across iterations
"""

import sys
import io
import os
import time
import uuid
import tempfile
import statistics
import random
import numpy as np
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import redshift_connector
from config_loader import load_config

sys.stdout.reconfigure(line_buffering=True)

ROWS = 1_000_000
ITERATIONS = 3
PAUSE_BETWEEN_RUNS = 10  # seconds


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


def verify_s3_clean(s3, bucket, prefix):
    """Verify no leftover files exist under the staging prefix."""
    paginator = s3.get_paginator("list_objects_v2")
    count = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        count += len(page.get("Contents", []))
    return count


def cleanup_s3_prefix(s3, bucket, prefix):
    """Delete all objects under a prefix."""
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = page.get("Contents", [])
        if objects:
            s3.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": obj["Key"]} for obj in objects]},
            )


def disable_caches(conn):
    """Disable Redshift result cache for this session."""
    cursor = conn.cursor()
    cursor.execute("SET enable_result_cache_for_session = off")
    conn.commit()
    print("  Redshift result cache disabled for session")


def run_single(conn, table, cfg, variant, iteration, s3_client):
    """Run a single write benchmark iteration. Returns phase timings."""
    stg = cfg["staging"]
    op_id = uuid.uuid4().hex[:12]
    key_prefix = f"{stg['prefix']}/rigorous_{variant}_{iteration}_{op_id}/"
    s3_uri = f"s3://{stg['bucket']}/{key_prefix}"
    parquet_key = f"{key_prefix}data.parquet"
    target_table = f"rigorous_{variant}_{iteration}"

    # Pre-check: verify S3 prefix is clean
    leftover = verify_s3_clean(s3_client, stg["bucket"], key_prefix)
    if leftover > 0:
        print(f"    WARNING: {leftover} leftover files found, cleaning...")
        cleanup_s3_prefix(s3_client, stg["bucket"], key_prefix)

    # Create clean target table
    create_target_table(conn, target_table, cfg)

    if variant == "manual":
        # Manual COPY: BytesIO path
        t1 = time.perf_counter()
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        parquet_bytes = buf.getbuffer().nbytes
        p_parquet = time.perf_counter() - t1

        t2 = time.perf_counter()
        s3_client.upload_fileobj(buf, stg["bucket"], parquet_key)
        p_upload = time.perf_counter() - t2

        t3 = time.perf_counter()
        cursor = conn.cursor()
        cursor.execute(
            f"COPY {target_table} FROM '{s3_uri}' "
            f"IAM_ROLE '{stg['iam_role']}' FORMAT PARQUET"
        )
        conn.commit()
        p_copy = time.perf_counter() - t3

    elif variant == "ours_bytesio":
        # Our path: identical to manual (BytesIO)
        t1 = time.perf_counter()
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        parquet_bytes = buf.getbuffer().nbytes
        p_parquet = time.perf_counter() - t1

        t2 = time.perf_counter()
        s3_client.upload_fileobj(buf, stg["bucket"], parquet_key)
        p_upload = time.perf_counter() - t2

        t3 = time.perf_counter()
        cursor = conn.cursor()
        cursor.execute(
            f"COPY {target_table} FROM '{s3_uri}' "
            f"IAM_ROLE '{stg['iam_role']}' FORMAT PARQUET"
        )
        conn.commit()
        p_copy = time.perf_counter() - t3

    elif variant == "ours_tmpfile":
        # Our path: temp file + upload_file
        t1 = time.perf_counter()
        tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        pq.write_table(table, tmp.name, compression="snappy")
        parquet_bytes = os.path.getsize(tmp.name)
        p_parquet = time.perf_counter() - t1

        t2 = time.perf_counter()
        s3_client.upload_file(tmp.name, stg["bucket"], parquet_key)
        p_upload = time.perf_counter() - t2
        os.unlink(tmp.name)

        t3 = time.perf_counter()
        cursor = conn.cursor()
        cursor.execute(
            f"COPY {target_table} FROM '{s3_uri}' "
            f"IAM_ROLE '{stg['iam_role']}' FORMAT PARQUET"
        )
        conn.commit()
        p_copy = time.perf_counter() - t3

    total = p_parquet + p_upload + p_copy

    # Verify row count
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
    actual_rows = cursor.fetchone()[0]

    # Cleanup S3 (not timed)
    s3_client.delete_object(Bucket=stg["bucket"], Key=parquet_key)

    # Drop table (not timed)
    cursor.execute(f"DROP TABLE IF EXISTS {target_table}")
    conn.commit()

    return {
        "variant": variant,
        "iteration": iteration,
        "parquet_s": round(p_parquet, 3),
        "upload_s": round(p_upload, 3),
        "copy_s": round(p_copy, 3),
        "total_s": round(total, 3),
        "parquet_mb": round(parquet_bytes / 1024 / 1024, 1),
        "rows_verified": actual_rows,
    }


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

    s3 = boto3.client("s3", region_name=stg["region"])

    # Precautions
    print("=" * 60)
    print("PRECAUTIONS")
    print("=" * 60)
    disable_caches(conn)

    leftover = verify_s3_clean(s3, stg["bucket"], stg["prefix"] + "/rigorous_")
    print(f"  S3 staging prefix clean: {leftover} leftover files")
    if leftover > 0:
        print("  Cleaning up...")
        cleanup_s3_prefix(s3, stg["bucket"], stg["prefix"] + "/rigorous_")

    # Generate data once
    print(f"\nGenerating {ROWS:,} rows (shared across all tests)...")
    table = generate_arrow_table(ROWS, cfg)
    print(f"  Arrow table: {table.nbytes / 1024 / 1024:.1f} MB, {table.num_columns} columns")

    # Run all variants
    variants = ["manual", "ours_bytesio", "ours_tmpfile"]
    all_results = {v: [] for v in variants}

    for iteration in range(1, ITERATIONS + 1):
        print(f"\n{'=' * 60}")
        print(f"ITERATION {iteration}/{ITERATIONS}")
        print(f"{'=' * 60}")

        for variant in random.sample(variants, len(variants)):
            print(f"\n  [{variant}] iteration {iteration}...")
            result = run_single(conn, table, cfg, variant, iteration, s3)
            all_results[variant].append(result)
            print(f"    parquet: {result['parquet_s']}s | upload: {result['upload_s']}s | "
                  f"copy: {result['copy_s']}s | total: {result['total_s']}s | "
                  f"rows: {result['rows_verified']:,}")

            # Pause between runs
            if not (iteration == ITERATIONS and variant == variants[-1]):
                print(f"    (pausing {PAUSE_BETWEEN_RUNS}s)")
                time.sleep(PAUSE_BETWEEN_RUNS)

    # Summary with statistics
    print(f"\n{'=' * 60}")
    print(f"FINAL SUMMARY ({ITERATIONS} iterations, {ROWS:,} rows)")
    print(f"{'=' * 60}")

    for variant in variants:
        results = all_results[variant]
        totals = [r["total_s"] for r in results]
        parquets = [r["parquet_s"] for r in results]
        uploads = [r["upload_s"] for r in results]
        copies = [r["copy_s"] for r in results]

        print(f"\n  [{variant}]")
        print(f"    Parquet:  min={min(parquets):.3f}s  max={max(parquets):.3f}s  median={statistics.median(parquets):.3f}s")
        print(f"    Upload:   min={min(uploads):.3f}s  max={max(uploads):.3f}s  median={statistics.median(uploads):.3f}s")
        print(f"    COPY:     min={min(copies):.3f}s  max={max(copies):.3f}s  median={statistics.median(copies):.3f}s")
        print(f"    TOTAL:    min={min(totals):.3f}s  max={max(totals):.3f}s  median={statistics.median(totals):.3f}s")

    # Cross-variant comparison using medians
    print(f"\n{'=' * 60}")
    print("MEDIAN COMPARISON")
    print(f"{'=' * 60}")
    medians = {}
    for variant in variants:
        totals = [r["total_s"] for r in all_results[variant]]
        medians[variant] = statistics.median(totals)
        print(f"  {variant:<20} median: {medians[variant]:.3f}s")

    manual_median = medians["manual"]
    for variant in variants:
        if variant != "manual":
            ratio = medians[variant] / manual_median
            label = "faster" if ratio < 1 else "slower"
            print(f"  {variant} vs manual: {ratio:.2f}x ({label})")

    conn.close()


if __name__ == "__main__":
    main()
