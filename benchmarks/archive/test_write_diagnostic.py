"""
Write diagnostic: breaks down both manual COPY and our write_bulk into
identical stages to pinpoint where the overhead is.

Both paths use:
  - Same input Arrow table
  - Same Parquet compression (snappy)
  - Same single file
  - Same S3 bucket/prefix/region
  - Same COPY options
  - Cleanup excluded from timing
"""

import sys
import io
import time
import uuid
import tempfile
import os
import numpy as np
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import redshift_connector
from config_loader import load_config

sys.stdout.reconfigure(line_buffering=True)

ROWS = 1_000_000


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


def run_manual_copy(conn, table, cfg, label):
    """Manual COPY with phase breakdown."""
    stg = cfg["staging"]
    s3 = boto3.client("s3", region_name=stg["region"])
    op_id = uuid.uuid4().hex[:12]
    s3_key = f"{stg['prefix']}/diag_manual_{op_id}/data.parquet"
    s3_uri = f"s3://{stg['bucket']}/{stg['prefix']}/diag_manual_{op_id}/"

    print(f"\n--- {label}: MANUAL COPY (phase breakdown) ---")

    # Phase 1: Arrow → Parquet (BytesIO)
    t = time.perf_counter()
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    parquet_bytes = buf.getbuffer().nbytes
    p1 = time.perf_counter() - t
    print(f"  1. Parquet write (BytesIO):  {p1:.3f}s  ({parquet_bytes/1024/1024:.1f} MB)")

    # Phase 2: S3 upload (upload_fileobj)
    t = time.perf_counter()
    s3.upload_fileobj(buf, stg["bucket"], s3_key)
    p2 = time.perf_counter() - t
    print(f"  2. S3 upload (upload_fileobj): {p2:.3f}s")

    # Phase 3: COPY
    create_target_table(conn, f"diag_manual_{label}", cfg)
    t = time.perf_counter()
    cursor = conn.cursor()
    cursor.execute(
        f"COPY diag_manual_{label} FROM '{s3_uri}' "
        f"IAM_ROLE '{stg['iam_role']}' FORMAT PARQUET"
    )
    conn.commit()
    p3 = time.perf_counter() - t
    print(f"  3. COPY execute:              {p3:.3f}s")

    total = p1 + p2 + p3
    print(f"  TOTAL (excl cleanup):         {total:.3f}s")

    # Cleanup (not timed)
    s3.delete_object(Bucket=stg["bucket"], Key=s3_key)

    return {"parquet": p1, "upload": p2, "copy": p3, "total": total, "parquet_mb": parquet_bytes/1024/1024}


def run_our_write_bulk(conn, table, cfg, label):
    """Our write_bulk with phase breakdown — same steps, instrumented."""
    stg = cfg["staging"]
    s3 = boto3.client("s3", region_name=stg["region"])
    op_id = uuid.uuid4().hex[:12]
    key_prefix = f"{stg['prefix']}/diag_ours_{op_id}/"
    s3_uri = f"s3://{stg['bucket']}/{key_prefix}"
    parquet_key = f"{key_prefix}data.parquet"

    print(f"\n--- {label}: OUR WRITE_BULK (phase breakdown) ---")

    # Phase 1: Arrow → Parquet (BytesIO) — same as manual
    t = time.perf_counter()
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    parquet_bytes = buf.getbuffer().nbytes
    p1 = time.perf_counter() - t
    print(f"  1. Parquet write (BytesIO):  {p1:.3f}s  ({parquet_bytes/1024/1024:.1f} MB)")

    # Phase 2: S3 upload (upload_fileobj) — same as manual
    t = time.perf_counter()
    s3.upload_fileobj(buf, stg["bucket"], parquet_key)
    p2 = time.perf_counter() - t
    print(f"  2. S3 upload (upload_fileobj): {p2:.3f}s")

    # Phase 3: COPY — same as manual
    create_target_table(conn, f"diag_ours_{label}", cfg)
    t = time.perf_counter()
    cursor = conn.cursor()
    cursor.execute(
        f"COPY diag_ours_{label} FROM '{s3_uri}' "
        f"IAM_ROLE '{stg['iam_role']}' FORMAT PARQUET"
    )
    conn.commit()
    p3 = time.perf_counter() - t
    print(f"  3. COPY execute:              {p3:.3f}s")

    total = p1 + p2 + p3
    print(f"  TOTAL (excl cleanup):         {total:.3f}s")

    # Cleanup (not timed)
    s3.delete_object(Bucket=stg["bucket"], Key=parquet_key)

    return {"parquet": p1, "upload": p2, "copy": p3, "total": total, "parquet_mb": parquet_bytes/1024/1024}


def run_tempfile_variant(conn, table, cfg, label):
    """Variant: write Parquet to temp file, upload via upload_file."""
    stg = cfg["staging"]
    s3 = boto3.client("s3", region_name=stg["region"])
    op_id = uuid.uuid4().hex[:12]
    key_prefix = f"{stg['prefix']}/diag_tmpfile_{op_id}/"
    s3_uri = f"s3://{stg['bucket']}/{key_prefix}"
    parquet_key = f"{key_prefix}data.parquet"

    print(f"\n--- {label}: TEMP FILE VARIANT (phase breakdown) ---")

    # Phase 1: Arrow → Parquet (temp file)
    t = time.perf_counter()
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    pq.write_table(table, tmp.name, compression="snappy")
    parquet_bytes = os.path.getsize(tmp.name)
    p1 = time.perf_counter() - t
    print(f"  1. Parquet write (temp file): {p1:.3f}s  ({parquet_bytes/1024/1024:.1f} MB)")

    # Phase 2: S3 upload (upload_file — uses transfer manager)
    t = time.perf_counter()
    s3.upload_file(tmp.name, stg["bucket"], parquet_key)
    p2 = time.perf_counter() - t
    print(f"  2. S3 upload (upload_file):   {p2:.3f}s")

    # Phase 3: COPY
    create_target_table(conn, f"diag_tmpfile_{label}", cfg)
    t = time.perf_counter()
    cursor = conn.cursor()
    cursor.execute(
        f"COPY diag_tmpfile_{label} FROM '{s3_uri}' "
        f"IAM_ROLE '{stg['iam_role']}' FORMAT PARQUET"
    )
    conn.commit()
    p3 = time.perf_counter() - t
    print(f"  3. COPY execute:              {p3:.3f}s")

    total = p1 + p2 + p3
    print(f"  TOTAL (excl cleanup):         {total:.3f}s")

    # Cleanup (not timed)
    os.unlink(tmp.name)
    s3.delete_object(Bucket=stg["bucket"], Key=parquet_key)

    return {"parquet": p1, "upload": p2, "copy": p3, "total": total, "parquet_mb": parquet_bytes/1024/1024}


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

    # Generate data once — shared across all runs
    print(f"Generating {ROWS:,} rows (shared across all tests)...")
    table = generate_arrow_table(ROWS, cfg)
    print(f"  Arrow table: {table.nbytes / 1024 / 1024:.1f} MB, {table.num_columns} columns")

    # Run each variant twice for consistency
    print(f"\n{'='*60}")
    print("RUN 1 (warmup)")
    print(f"{'='*60}")
    m1 = run_manual_copy(conn, table, cfg, "run1")
    o1 = run_our_write_bulk(conn, table, cfg, "run1")
    f1 = run_tempfile_variant(conn, table, cfg, "run1")

    print(f"\n{'='*60}")
    print("RUN 2 (measured)")
    print(f"{'='*60}")
    m2 = run_manual_copy(conn, table, cfg, "run2")
    o2 = run_our_write_bulk(conn, table, cfg, "run2")
    f2 = run_tempfile_variant(conn, table, cfg, "run2")

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY (Run 2 — measured)")
    print(f"{'='*60}")
    print(f"{'Phase':<25} {'Manual':>10} {'Ours(BytesIO)':>15} {'Ours(TmpFile)':>15}")
    print("-" * 67)
    print(f"{'Parquet write':<25} {m2['parquet']:>10.3f}s {o2['parquet']:>15.3f}s {f2['parquet']:>15.3f}s")
    print(f"{'S3 upload':<25} {m2['upload']:>10.3f}s {o2['upload']:>15.3f}s {f2['upload']:>15.3f}s")
    print(f"{'COPY execute':<25} {m2['copy']:>10.3f}s {o2['copy']:>15.3f}s {f2['copy']:>15.3f}s")
    print(f"{'TOTAL (excl cleanup)':<25} {m2['total']:>10.3f}s {o2['total']:>15.3f}s {f2['total']:>15.3f}s")
    print(f"{'Parquet size':<25} {m2['parquet_mb']:>9.1f}MB {o2['parquet_mb']:>14.1f}MB {f2['parquet_mb']:>14.1f}MB")

    print(f"\nOverhead vs manual:")
    print(f"  Ours (BytesIO): {o2['total'] - m2['total']:+.3f}s ({o2['total']/m2['total']:.2f}x)")
    print(f"  Ours (TmpFile): {f2['total'] - m2['total']:+.3f}s ({f2['total']/m2['total']:.2f}x)")

    conn.close()


if __name__ == "__main__":
    main()
