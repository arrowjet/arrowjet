"""
Read benchmark on 4-node cluster (8 slices).
Compares baseline (direct fetch) vs bulk (UNLOAD) with phase breakdown.
Uses config_4node.yaml.

Key question: does 4x more slices (8 vs 2) improve UNLOAD parallelism?
"""

import sys
import io
import time
import uuid
import yaml
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs
import redshift_connector

sys.stdout.reconfigure(line_buffering=True)


def load_config():
    with open(Path(__file__).parent / "config_4node.yaml") as f:
        return yaml.safe_load(f)


def get_cluster_info(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT version()")
    version = cursor.fetchone()[0][:80]
    cursor.execute("SELECT COUNT(*) FROM stv_slices WHERE node >= 0 GROUP BY node")
    slices = cursor.fetchall()
    return {
        "version": version,
        "node_count": len(slices),
        "slices_per_node": slices[0][0] if slices else 0,
        "total_slices": sum(r[0] for r in slices),
    }


def run_baseline(conn, table_name):
    """Direct fetch via PG wire protocol."""
    print(f"  [baseline] direct fetch...")
    cursor = conn.cursor()
    start = time.perf_counter()
    cursor.execute(f"SELECT * FROM {table_name}")
    df = cursor.fetch_dataframe()
    elapsed = time.perf_counter() - start
    rows = len(df)
    mem_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
    print(f"  [baseline] {rows:,} rows in {elapsed:.2f}s ({rows/elapsed:,.0f} rows/s, {mem_mb:.0f} MB)")
    return {"method": "direct", "rows": rows, "duration_s": elapsed, "mem_mb": mem_mb}


def run_bulk_diagnostic(conn, table_name, cfg):
    """UNLOAD with phase breakdown."""
    stg = cfg["staging"]
    s3 = boto3.client("s3", region_name=stg["region"])
    op_id = uuid.uuid4().hex[:12]
    key_prefix = f"{stg['prefix']}/read4n_{op_id}/"
    s3_uri = f"s3://{stg['bucket']}/{key_prefix}"

    # Phase 1: UNLOAD
    print(f"  [bulk] Phase 1: UNLOAD...")
    t = time.perf_counter()
    cursor = conn.cursor()
    cursor.execute(
        f"UNLOAD ($$SELECT * FROM {table_name}$$) "
        f"TO '{s3_uri}' "
        f"IAM_ROLE '{stg['iam_role']}' "
        f"FORMAT PARQUET "
        f"ALLOWOVERWRITE "
        f"PARALLEL ON"
    )
    p_unload = time.perf_counter() - t
    print(f"           {p_unload:.2f}s")

    # Phase 2: List files
    print(f"  [bulk] Phase 2: List S3 files...")
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
    total_mb = total_bytes / 1024 / 1024
    print(f"           {p_list:.2f}s  - {len(files)} files, {total_mb:.1f} MB")
    for f in files:
        print(f"             {f['Key'].split('/')[-1]}: {f['Size']/1024/1024:.1f} MB")

    # Phase 3: Read via PyArrow S3FileSystem
    print(f"  [bulk] Phase 3: Read Parquet (PyArrow S3FS)...")
    t = time.perf_counter()
    s3fs = pafs.S3FileSystem(region=stg["region"])
    dataset = pq.ParquetDataset(f"{stg['bucket']}/{key_prefix}", filesystem=s3fs)
    arrow_table = dataset.read()
    p_read = time.perf_counter() - t
    rows = arrow_table.num_rows
    arrow_mb = arrow_table.nbytes / 1024 / 1024
    print(f"           {p_read:.2f}s  - {rows:,} rows, {arrow_mb:.0f} MB Arrow")

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
        "method": "unload",
        "rows": rows,
        "p_unload": p_unload,
        "p_list": p_list,
        "p_read": p_read,
        "total_s": total,
        "files": len(files),
        "staged_mb": total_mb,
        "arrow_mb": arrow_mb,
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

    # Disable result cache
    cursor = conn.cursor()
    cursor.execute("SET enable_result_cache_for_session = off")

    # Cluster info
    print("=" * 60)
    print("4-NODE CLUSTER READ BENCHMARK")
    print("=" * 60)
    info = get_cluster_info(conn)
    for k, v in info.items():
        print(f"  {k}: {v}")

    # Compare with 1-node results
    print(f"\n  Previous (1-node, 2 slices):")
    print(f"    1M:  baseline=16.0s, bulk=8.5s (1.89x)")
    print(f"    10M: baseline=150.0s, bulk=99.7s (1.51x)")
    print(f"         UNLOAD=93.7s, Read=5.9s")

    for table_name in ["benchmark_test_1m", "benchmark_test_10m"]:
        row_label = "1M" if "1m" in table_name else "10M"
        print(f"\n{'=' * 60}")
        print(f"TABLE: {table_name} ({row_label} rows)")
        print(f"{'=' * 60}")

        # Baseline
        print(f"\n--- BASELINE (direct fetch) ---")
        baseline = run_baseline(conn, table_name)

        # Bulk
        print(f"\n--- BULK (UNLOAD -> S3 -> Parquet -> Arrow) ---")
        bulk = run_bulk_diagnostic(conn, table_name, cfg)

        # Comparison
        speedup = baseline["duration_s"] / bulk["total_s"]
        print(f"\n--- COMPARISON ({row_label}) ---")
        print(f"  Baseline:    {baseline['duration_s']:.2f}s")
        print(f"  Bulk total:  {bulk['total_s']:.2f}s")
        print(f"    UNLOAD:    {bulk['p_unload']:.2f}s")
        print(f"    List:      {bulk['p_list']:.2f}s")
        print(f"    Read:      {bulk['p_read']:.2f}s")
        print(f"  Files:       {bulk['files']}")
        print(f"  Staged:      {bulk['staged_mb']:.1f} MB")
        print(f"  Speedup:     {speedup:.2f}x")

    conn.close()


if __name__ == "__main__":
    main()
