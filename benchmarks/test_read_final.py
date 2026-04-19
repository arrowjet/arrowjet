"""
Final read benchmark on 4-node cluster: baseline vs bulk, rigorous.

Precautions:
  - Disable result cache
  - 5 iterations
  - Randomize order (baseline vs bulk) each round
  - S3 cleanup after each bulk run
  - 10s pause between runs
  - Report median, p95, phase breakdown
  - Run on 10M rows only (where the difference matters)
"""

import sys
import io
import time
import uuid
import random
import statistics
import yaml
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs
import redshift_connector

sys.stdout.reconfigure(line_buffering=True)

ITERATIONS = 5
PAUSE_S = 10
TABLE = "benchmark_test_10m"


def load_config():
    with open(Path(__file__).parent / "config_4node.yaml") as f:
        return yaml.safe_load(f)


def percentile(data, p):
    sorted_data = sorted(data)
    idx = (p / 100) * (len(sorted_data) - 1)
    lower = int(idx)
    upper = min(lower + 1, len(sorted_data) - 1)
    frac = idx - lower
    return sorted_data[lower] * (1 - frac) + sorted_data[upper] * frac


def run_baseline(conn):
    """Direct fetch via PG wire protocol."""
    cursor = conn.cursor()
    start = time.perf_counter()
    cursor.execute(f"SELECT * FROM {TABLE}")
    df = cursor.fetch_dataframe()
    elapsed = time.perf_counter() - start
    rows = len(df)
    del df  # free memory before next run
    return {"method": "baseline", "rows": rows, "total_s": elapsed}


def run_bulk(conn, cfg):
    """UNLOAD -> S3 -> Parquet -> Arrow with phase breakdown."""
    stg = cfg["staging"]
    s3 = boto3.client("s3", region_name=stg["region"])
    op_id = uuid.uuid4().hex[:12]
    key_prefix = f"{stg['prefix']}/readfinal_{op_id}/"
    s3_uri = f"s3://{stg['bucket']}/{key_prefix}"

    # Phase 1: UNLOAD
    t = time.perf_counter()
    cursor = conn.cursor()
    cursor.execute(
        f"UNLOAD ($$SELECT * FROM {TABLE}$$) "
        f"TO '{s3_uri}' "
        f"IAM_ROLE '{stg['iam_role']}' "
        f"FORMAT PARQUET "
        f"ALLOWOVERWRITE "
        f"PARALLEL ON"
    )
    p_unload = time.perf_counter() - t

    # Phase 2: List files
    t = time.perf_counter()
    files = []
    total_bytes = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=stg["bucket"], Prefix=key_prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                files.append(obj)
                total_bytes += obj["Size"]
    p_list = time.perf_counter() - t

    # Phase 3: Read Parquet
    t = time.perf_counter()
    s3fs = pafs.S3FileSystem(region=stg["region"])
    dataset = pq.ParquetDataset(f"{stg['bucket']}/{key_prefix}", filesystem=s3fs)
    arrow_table = dataset.read()
    p_read = time.perf_counter() - t
    rows = arrow_table.num_rows
    del arrow_table  # free memory

    # Cleanup (not timed)
    for page in paginator.paginate(Bucket=stg["bucket"], Prefix=key_prefix):
        objects = page.get("Contents", [])
        if objects:
            s3.delete_objects(
                Bucket=stg["bucket"],
                Delete={"Objects": [{"Key": obj["Key"]} for obj in objects]},
            )

    total = p_unload + p_list + p_read
    return {
        "method": "bulk",
        "rows": rows,
        "p_unload": p_unload,
        "p_list": p_list,
        "p_read": p_read,
        "total_s": total,
        "files": len(files),
        "staged_mb": total_bytes / 1024 / 1024,
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
    conn.autocommit = True

    # Precautions
    cursor = conn.cursor()
    cursor.execute("SET enable_result_cache_for_session = off")

    # Cluster info
    cursor.execute("SELECT COUNT(*) FROM stv_slices WHERE node >= 0 GROUP BY node")
    slices = cursor.fetchall()
    total_slices = sum(r[0] for r in slices)

    print("=" * 60)
    print("FINAL READ BENCHMARK (4-node cluster)")
    print("=" * 60)
    print(f"  Nodes: {len(slices)}, Slices: {total_slices}")
    print(f"  Table: {TABLE}")
    print(f"  Iterations: {ITERATIONS}")
    print(f"  Order: randomized each round")
    print(f"  Result cache: disabled")
    print(f"  Pause between runs: {PAUSE_S}s")

    results = {"baseline": [], "bulk": []}
    variants = ["baseline", "bulk"]

    for iteration in range(1, ITERATIONS + 1):
        order = variants[:]
        random.shuffle(order)
        print(f"\n--- Iteration {iteration}/{ITERATIONS} (order: {order}) ---")

        for run_idx, variant in enumerate(order):
            if variant == "baseline":
                r = run_baseline(conn)
                print(f"  [baseline] {r['rows']:,} rows in {r['total_s']:.2f}s "
                      f"({r['rows']/r['total_s']:,.0f} rows/s)")
            else:
                r = run_bulk(conn, cfg)
                print(f"  [   bulk ] {r['rows']:,} rows in {r['total_s']:.2f}s "
                      f"(UNLOAD={r['p_unload']:.2f}s List={r['p_list']:.2f}s "
                      f"Read={r['p_read']:.2f}s Files={r['files']})")

            results[variant].append(r)

            # Pause (except last run of last iteration)
            if not (iteration == ITERATIONS and run_idx == len(order) - 1):
                time.sleep(PAUSE_S)

    # Summary
    print(f"\n{'=' * 60}")
    print(f"RESULTS ({ITERATIONS} iterations, randomized order)")
    print(f"{'=' * 60}")

    b_totals = [r["total_s"] for r in results["baseline"]]
    k_totals = [r["total_s"] for r in results["bulk"]]
    k_unloads = [r["p_unload"] for r in results["bulk"]]
    k_reads = [r["p_read"] for r in results["bulk"]]

    print(f"\n  [baseline]")
    print(f"    TOTAL:   median={statistics.median(b_totals):.2f}s  "
          f"min={min(b_totals):.2f}s  max={max(b_totals):.2f}s  "
          f"p95={percentile(b_totals, 95):.2f}s")

    print(f"\n  [bulk]")
    print(f"    UNLOAD:  median={statistics.median(k_unloads):.2f}s  "
          f"min={min(k_unloads):.2f}s  max={max(k_unloads):.2f}s")
    print(f"    Read:    median={statistics.median(k_reads):.2f}s  "
          f"min={min(k_reads):.2f}s  max={max(k_reads):.2f}s")
    print(f"    TOTAL:   median={statistics.median(k_totals):.2f}s  "
          f"min={min(k_totals):.2f}s  max={max(k_totals):.2f}s  "
          f"p95={percentile(k_totals, 95):.2f}s")

    # Verdict
    b_median = statistics.median(b_totals)
    k_median = statistics.median(k_totals)
    speedup = b_median / k_median

    print(f"\n{'=' * 60}")
    print("VERDICT")
    print(f"{'=' * 60}")
    print(f"  Baseline median:  {b_median:.2f}s")
    print(f"  Bulk median:      {k_median:.2f}s")
    print(f"  Speedup:          {speedup:.2f}x")

    if speedup >= 3.0:
        print(f"\n  ✅ READ-SIDE VALIDATED: {speedup:.1f}x speedup on 4-node cluster")
    elif speedup >= 2.0:
        print(f"\n  ⚠️  Moderate speedup ({speedup:.1f}x) — acceptable but below 3x target")
    else:
        print(f"\n  ❌ Speedup below 2x — investigate")

    conn.close()


if __name__ == "__main__":
    main()
