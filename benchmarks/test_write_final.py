"""
Final write benchmark: Manual COPY vs TmpFile, randomized order, 5 iterations.

Precautions:
  - Disable result cache
  - Randomize variant order each round
  - Fresh target table each run (DROP + CREATE)
  - Same input data, compression, file count, COPY options
  - Cleanup excluded from timing
  - 10s pause between runs
  - Report median and p95
"""

import sys
import os
import io
import time
import uuid
import random
import tempfile
import statistics
import numpy as np
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import redshift_connector
from config_loader import load_config

sys.stdout.reconfigure(line_buffering=True)

ROWS = 1_000_000
ITERATIONS = 5
PAUSE_S = 10


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


def col_defs(cfg):
    cols = []
    for i in range(cfg["benchmark"]["num_int_cols"]):
        cols.append(f"int_col_{i} BIGINT")
    for i in range(cfg["benchmark"]["num_float_cols"]):
        cols.append(f"float_col_{i} DOUBLE PRECISION")
    for i in range(cfg["benchmark"]["num_string_cols"]):
        cols.append(f"str_col_{i} VARCHAR({cfg['benchmark']['string_length']})")
    return ", ".join(cols)


def run_manual(conn, table, cfg, s3, iteration, run_idx):
    stg = cfg["staging"]
    op_id = uuid.uuid4().hex[:12]
    key = f"{stg['prefix']}/final_manual_{iteration}_{run_idx}_{op_id}/data.parquet"
    s3_uri = f"s3://{stg['bucket']}/{stg['prefix']}/final_manual_{iteration}_{run_idx}_{op_id}/"
    tbl_name = f"final_manual_{iteration}_{run_idx}"

    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {tbl_name}")
    cursor.execute(f"CREATE TABLE {tbl_name} ({col_defs(cfg)})")
    conn.commit()

    # Parquet write (BytesIO  - this is what a manual user would do)
    t = time.perf_counter()
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    p_parquet = time.perf_counter() - t

    # S3 upload
    t = time.perf_counter()
    s3.upload_fileobj(buf, stg["bucket"], key)
    p_upload = time.perf_counter() - t

    # COPY
    t = time.perf_counter()
    cursor.execute(f"COPY {tbl_name} FROM '{s3_uri}' IAM_ROLE '{stg['iam_role']}' FORMAT PARQUET")
    conn.commit()
    p_copy = time.perf_counter() - t

    # Verify
    cursor.execute(f"SELECT COUNT(*) FROM {tbl_name}")
    rows = cursor.fetchone()[0]

    # Cleanup (not timed)
    s3.delete_object(Bucket=stg["bucket"], Key=key)
    cursor.execute(f"DROP TABLE IF EXISTS {tbl_name}")
    conn.commit()

    return {"parquet": p_parquet, "upload": p_upload, "copy": p_copy,
            "total": p_parquet + p_upload + p_copy, "rows": rows}


def run_tmpfile(conn, table, cfg, s3, iteration, run_idx):
    stg = cfg["staging"]
    op_id = uuid.uuid4().hex[:12]
    key = f"{stg['prefix']}/final_tmpfile_{iteration}_{run_idx}_{op_id}/data.parquet"
    s3_uri = f"s3://{stg['bucket']}/{stg['prefix']}/final_tmpfile_{iteration}_{run_idx}_{op_id}/"
    tbl_name = f"final_tmpfile_{iteration}_{run_idx}"

    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {tbl_name}")
    cursor.execute(f"CREATE TABLE {tbl_name} ({col_defs(cfg)})")
    conn.commit()

    # Parquet write (temp file)
    t = time.perf_counter()
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    pq.write_table(table, tmp.name, compression="snappy")
    p_parquet = time.perf_counter() - t

    # S3 upload (upload_file  - uses transfer manager)
    t = time.perf_counter()
    s3.upload_file(tmp.name, stg["bucket"], key)
    p_upload = time.perf_counter() - t
    os.unlink(tmp.name)

    # COPY
    t = time.perf_counter()
    cursor = conn.cursor()
    cursor.execute(f"COPY {tbl_name} FROM '{s3_uri}' IAM_ROLE '{stg['iam_role']}' FORMAT PARQUET")
    conn.commit()
    p_copy = time.perf_counter() - t

    # Verify
    cursor.execute(f"SELECT COUNT(*) FROM {tbl_name}")
    rows = cursor.fetchone()[0]

    # Cleanup (not timed)
    s3.delete_object(Bucket=stg["bucket"], Key=key)
    cursor.execute(f"DROP TABLE IF EXISTS {tbl_name}")
    conn.commit()

    return {"parquet": p_parquet, "upload": p_upload, "copy": p_copy,
            "total": p_parquet + p_upload + p_copy, "rows": rows}


def percentile(data, p):
    """Simple percentile calculation."""
    sorted_data = sorted(data)
    idx = (p / 100) * (len(sorted_data) - 1)
    lower = int(idx)
    upper = min(lower + 1, len(sorted_data) - 1)
    frac = idx - lower
    return sorted_data[lower] * (1 - frac) + sorted_data[upper] * frac


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
    print("FINAL WRITE BENCHMARK")
    print("=" * 60)
    cursor = conn.cursor()
    cursor.execute("SET enable_result_cache_for_session = off")
    conn.commit()
    print("  Result cache: disabled")
    print(f"  Iterations: {ITERATIONS}")
    print(f"  Variants: manual, tmpfile (randomized order each round)")
    print(f"  Pause between runs: {PAUSE_S}s")

    # Generate data
    print(f"\nGenerating {ROWS:,} rows...")
    table = generate_arrow_table(ROWS, cfg)
    print(f"  Arrow: {table.nbytes / 1024 / 1024:.1f} MB, {table.num_columns} cols")

    results = {"manual": [], "tmpfile": []}
    variants = ["manual", "tmpfile"]

    for iteration in range(1, ITERATIONS + 1):
        order = variants[:]
        random.shuffle(order)
        print(f"\n--- Iteration {iteration}/{ITERATIONS} (order: {order}) ---")

        for run_idx, variant in enumerate(order):
            if variant == "manual":
                r = run_manual(conn, table, cfg, s3, iteration, run_idx)
            else:
                r = run_tmpfile(conn, table, cfg, s3, iteration, run_idx)

            results[variant].append(r)
            print(f"  [{variant:>8}] parquet={r['parquet']:.3f}s upload={r['upload']:.3f}s "
                  f"copy={r['copy']:.3f}s total={r['total']:.3f}s rows={r['rows']:,}")

            # Pause (except last run of last iteration)
            if not (iteration == ITERATIONS and run_idx == len(order) - 1):
                time.sleep(PAUSE_S)

    # Summary
    print(f"\n{'=' * 60}")
    print(f"RESULTS ({ITERATIONS} iterations, randomized order)")
    print(f"{'=' * 60}")

    for variant in variants:
        data = results[variant]
        totals = [r["total"] for r in data]
        parquets = [r["parquet"] for r in data]
        uploads = [r["upload"] for r in data]
        copies = [r["copy"] for r in data]

        print(f"\n  [{variant}]")
        print(f"    Parquet: median={statistics.median(parquets):.3f}s  min={min(parquets):.3f}s  max={max(parquets):.3f}s")
        print(f"    Upload:  median={statistics.median(uploads):.3f}s  min={min(uploads):.3f}s  max={max(uploads):.3f}s")
        print(f"    COPY:    median={statistics.median(copies):.3f}s  min={min(copies):.3f}s  max={max(copies):.3f}s")
        print(f"    TOTAL:   median={statistics.median(totals):.3f}s  min={min(totals):.3f}s  max={max(totals):.3f}s  p95={percentile(totals, 95):.3f}s")

    # Comparison
    m_median = statistics.median([r["total"] for r in results["manual"]])
    t_median = statistics.median([r["total"] for r in results["tmpfile"]])
    ratio = t_median / m_median

    print(f"\n{'=' * 60}")
    print("VERDICT")
    print(f"{'=' * 60}")
    print(f"  Manual COPY median:  {m_median:.3f}s")
    print(f"  TmpFile median:      {t_median:.3f}s")
    print(f"  Ratio:               {ratio:.3f}x {'(faster)' if ratio < 1 else '(slower)' if ratio > 1 else '(parity)'}")

    if ratio <= 1.15:
        print(f"\n  ✅ WRITE-SIDE VALIDATED: TmpFile is within 1.15x of manual COPY")
    else:
        print(f"\n  ⚠️  TmpFile is {ratio:.2f}x of manual  - investigate further")

    conn.close()


if __name__ == "__main__":
    main()
