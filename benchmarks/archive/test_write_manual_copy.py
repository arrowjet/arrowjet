"""
Write benchmark: Manual COPY baseline.

This simulates what a competent Redshift user does today:
  1. Write data as Parquet locally
  2. Upload to S3
  3. Run COPY command

This is the fairness check  - our product must be compared against
the best practice, not just the worst case (write_dataframe).
"""

import sys
import io
import time
import uuid
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

    # Generate data (not timed  - same for all benchmarks)
    print(f"Generating {ROWS:,} rows...")
    table = generate_arrow_table(ROWS, cfg)
    print(f"  Data ready: {table.nbytes / 1024 / 1024:.1f} MB")

    create_target_table(conn, "write_test_manual_copy", cfg)

    s3 = boto3.client("s3", region_name=stg["region"])
    op_id = uuid.uuid4().hex[:12]
    s3_key = f"{stg['prefix']}/manual_copy_{op_id}/data.parquet"
    s3_uri = f"s3://{stg['bucket']}/{stg['prefix']}/manual_copy_{op_id}/"

    print(f"\n--- MANUAL COPY: Parquet -> S3 -> COPY ({ROWS:,} rows) ---")
    print(f"  This simulates what a competent user scripts manually.")

    # Phase 1: Write Parquet to buffer
    t1_start = time.perf_counter()
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    parquet_size = buf.getbuffer().nbytes
    t1 = time.perf_counter() - t1_start
    print(f"  Phase 1 - Parquet write:  {t1:.2f}s ({parquet_size / 1024 / 1024:.1f} MB)")

    # Phase 2: Upload to S3
    t2_start = time.perf_counter()
    s3.upload_fileobj(buf, stg["bucket"], s3_key)
    t2 = time.perf_counter() - t2_start
    print(f"  Phase 2 - S3 upload:      {t2:.2f}s")

    # Phase 3: COPY command
    t3_start = time.perf_counter()
    cursor = conn.cursor()
    copy_sql = (
        f"COPY write_test_manual_copy "
        f"FROM '{s3_uri}' "
        f"IAM_ROLE '{stg['iam_role']}' "
        f"FORMAT PARQUET"
    )
    cursor.execute(copy_sql)
    conn.commit()
    t3 = time.perf_counter() - t3_start
    print(f"  Phase 3 - COPY execute:   {t3:.2f}s")

    # Phase 4: Cleanup
    t4_start = time.perf_counter()
    s3.delete_object(Bucket=stg["bucket"], Key=s3_key)
    t4 = time.perf_counter() - t4_start
    print(f"  Phase 4 - S3 cleanup:     {t4:.2f}s")

    total = t1 + t2 + t3 + t4
    total_no_cleanup = t1 + t2 + t3
    rate = ROWS / total

    print(f"\n  TOTAL (with cleanup):    {total:.2f}s ({rate:,.0f} rows/s)")
    print(f"  TOTAL (without cleanup): {total_no_cleanup:.2f}s ({ROWS/total_no_cleanup:,.0f} rows/s)")

    # Verify
    cursor.execute("SELECT COUNT(*) FROM write_test_manual_copy")
    actual = cursor.fetchone()[0]
    print(f"  Verified: {actual:,} rows")

    # Compare all three approaches
    insert_rate = 12  # from batched INSERT benchmark
    our_copy_time = 26.56  # from test_write_copy_only.py

    print(f"\n{'='*60}")
    print("ALL WRITE BASELINES COMPARED")
    print(f"{'='*60}")
    print(f"  write_dataframe (INSERT):  12 rows/s -> ~23 hours for 1M rows")
    print(f"  Manual COPY (user script): {total_no_cleanup:.2f}s for 1M rows ({ROWS/total_no_cleanup:,.0f} rows/s)")
    print(f"  Our write_bulk (automated):{our_copy_time:.2f}s for 1M rows ({ROWS/our_copy_time:,.0f} rows/s)")
    print(f"")
    print(f"  Our product vs INSERT:      {(ROWS/insert_rate)/our_copy_time:,.0f}x faster")
    print(f"  Our product vs manual COPY: {total_no_cleanup/our_copy_time:.2f}x {'faster' if total_no_cleanup > our_copy_time else 'slower'}")
    print(f"")
    print(f"  Value proposition:")
    if total_no_cleanup > our_copy_time * 1.1:
        print(f"    Speed + convenience: our pipeline is faster AND automated")
    elif total_no_cleanup < our_copy_time * 0.9:
        overhead = ((our_copy_time / total_no_cleanup) - 1) * 100
        print(f"    Convenience over speed: manual COPY is faster by ~{overhead:.0f}%")
        print(f"    Our value is automation, safety, cleanup  - not raw speed")
    else:
        print(f"    Parity: our automated pipeline matches manual COPY speed")
        print(f"    Our value is automation, safety, cleanup at no speed cost")

    conn.close()


if __name__ == "__main__":
    main()
