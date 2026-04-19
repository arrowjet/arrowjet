"""
Diagnostic benchmark: breaks down the bulk read path into individual phases
to identify where time is spent.

Phases:
  1. UNLOAD execution (Redshift -> S3)
  2. S3 file listing (discover UNLOAD output)
  3. Parquet download + read (S3 -> Arrow)
  4. Cleanup (delete staged files)

Also measures:
  - Number and size of UNLOAD output files
  - S3 throughput
  - Memory usage
  - Redshift cluster node count and type
"""

import io
import os
import time
import uuid
import tracemalloc

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs
import redshift_connector

from config_loader import load_config


def get_cluster_info(conn):
    """Get Redshift cluster details."""
    cursor = conn.cursor()
    cursor.execute("SELECT version()")
    version = cursor.fetchone()[0]
    cursor.execute("""
        SELECT COUNT(*) as node_count
        FROM stv_slices
        WHERE node >= 0
        GROUP BY node
    """)
    slices = cursor.fetchall()
    node_count = len(slices)
    slices_per_node = slices[0][0] if slices else 0
    return {
        "version": version[:80],
        "node_count": node_count,
        "slices_per_node": slices_per_node,
    }


def get_table_info(conn, table_name):
    """Get table size and row count."""
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    row_count = cursor.fetchone()[0]

    cursor.execute(f"""
        SELECT size AS megabytes
        FROM svv_table_info
        WHERE "table" = '{table_name}'
    """)
    result = cursor.fetchone()
    size_mb = result[0] if result else "unknown"

    return {"row_count": row_count, "size_mb": size_mb}


def diagnostic_read(conn, table_name, bucket, prefix, iam_role, region):
    """Run bulk read with detailed phase timing."""
    op_id = uuid.uuid4().hex[:12]
    key_prefix = f"{prefix}/diag_{op_id}/"
    s3_uri = f"s3://{bucket}/{key_prefix}"

    results = {}

    # Phase 1: UNLOAD
    print("\n  Phase 1: UNLOAD (Redshift -> S3)...")
    unload_sql = (
        f"UNLOAD ($$SELECT * FROM {table_name}$$) "
        f"TO '{s3_uri}' "
        f"IAM_ROLE '{iam_role}' "
        f"FORMAT PARQUET "
        f"ALLOWOVERWRITE "
        f"PARALLEL ON"
    )
    start = time.perf_counter()
    cursor = conn.cursor()
    cursor.execute(unload_sql)
    results["unload_s"] = time.perf_counter() - start
    print(f"    {results['unload_s']:.2f}s")

    # Phase 2: List S3 files
    print("  Phase 2: List S3 files...")
    s3 = boto3.client("s3", region_name=region)
    start = time.perf_counter()
    files = []
    total_bytes = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                files.append(obj)
                total_bytes += obj["Size"]
    results["list_s"] = time.perf_counter() - start
    results["file_count"] = len(files)
    results["total_bytes"] = total_bytes
    results["total_mb"] = round(total_bytes / 1024 / 1024, 1)
    print(f"    {results['list_s']:.2f}s — {len(files)} files, {results['total_mb']} MB total")

    for f in files:
        size_mb = f["Size"] / 1024 / 1024
        print(f"      {f['Key'].split('/')[-1]}: {size_mb:.1f} MB")

    # Phase 3a: Read via PyArrow S3FileSystem (current approach)
    print("  Phase 3a: Read via PyArrow S3FileSystem...")
    tracemalloc.start()
    start = time.perf_counter()
    s3fs = pafs.S3FileSystem(region=region)
    dataset = pq.ParquetDataset(f"{bucket}/{key_prefix}", filesystem=s3fs)
    table_a = dataset.read()
    results["read_pyarrow_s3fs_s"] = time.perf_counter() - start
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    results["read_pyarrow_s3fs_peak_mb"] = round(peak / 1024 / 1024, 1)
    print(f"    {results['read_pyarrow_s3fs_s']:.2f}s — {table_a.num_rows:,} rows, peak mem: {results['read_pyarrow_s3fs_peak_mb']} MB")

    # Phase 3b: Read via boto3 download (for comparison)
    print("  Phase 3b: Read via boto3 sequential download...")
    start = time.perf_counter()
    tables = []
    for f in files:
        response = s3.get_object(Bucket=bucket, Key=f["Key"])
        buf = io.BytesIO(response["Body"].read())
        tables.append(pq.read_table(buf))
    table_b = pa.concat_tables(tables) if tables else pa.table({})
    results["read_boto3_seq_s"] = time.perf_counter() - start
    print(f"    {results['read_boto3_seq_s']:.2f}s — {table_b.num_rows:,} rows")

    # Phase 4: Cleanup
    print("  Phase 4: Cleanup...")
    start = time.perf_counter()
    for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
        objects = page.get("Contents", [])
        if objects:
            s3.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": obj["Key"]} for obj in objects]},
            )
    results["cleanup_s"] = time.perf_counter() - start
    print(f"    {results['cleanup_s']:.2f}s")

    # Summary
    results["total_s"] = (
        results["unload_s"]
        + results["list_s"]
        + results["read_pyarrow_s3fs_s"]
        + results["cleanup_s"]
    )
    results["rows"] = table_a.num_rows
    results["arrow_memory_mb"] = round(table_a.nbytes / 1024 / 1024, 1)

    return results


def diagnostic_baseline(conn, table_name):
    """Baseline direct fetch with timing."""
    print("\n  Baseline: direct fetch via PG wire...")
    cursor = conn.cursor()

    start = time.perf_counter()
    cursor.execute(f"SELECT * FROM {table_name}")
    df = cursor.fetch_dataframe()
    elapsed = time.perf_counter() - start

    return {
        "method": "direct_fetch",
        "rows": len(df),
        "duration_s": round(elapsed, 2),
        "rows_per_sec": round(len(df) / elapsed),
        "memory_mb": round(df.memory_usage(deep=True).sum() / 1024 / 1024, 1),
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
    conn.autocommit = True

    # Cluster info
    print("=" * 60)
    print("CLUSTER INFO")
    print("=" * 60)
    info = get_cluster_info(conn)
    for k, v in info.items():
        print(f"  {k}: {v}")

    for table_name in ["benchmark_test_1m", "benchmark_test_10m"]:
        print(f"\n{'=' * 60}")
        print(f"TABLE: {table_name}")
        print(f"{'=' * 60}")

        tinfo = get_table_info(conn, table_name)
        print(f"  rows: {tinfo['row_count']:,}")
        print(f"  size: {tinfo['size_mb']} MB (on disk in Redshift)")

        # Baseline
        print(f"\n--- BASELINE ---")
        baseline = diagnostic_baseline(conn, table_name)
        print(f"  {baseline['rows']:,} rows in {baseline['duration_s']}s")
        print(f"  {baseline['rows_per_sec']:,} rows/s")
        print(f"  DataFrame memory: {baseline['memory_mb']} MB")

        # Bulk diagnostic
        print(f"\n--- BULK (diagnostic) ---")
        bulk = diagnostic_read(
            conn, table_name,
            stg["bucket"], stg["prefix"], stg["iam_role"], stg["region"]
        )

        # Comparison
        print(f"\n--- COMPARISON ---")
        print(f"  Baseline:  {baseline['duration_s']:.2f}s")
        print(f"  Bulk total: {bulk['total_s']:.2f}s")
        print(f"    UNLOAD:   {bulk['unload_s']:.2f}s")
        print(f"    List:     {bulk['list_s']:.2f}s")
        print(f"    Read:     {bulk['read_pyarrow_s3fs_s']:.2f}s (pyarrow s3fs)")
        print(f"    Read:     {bulk['read_boto3_seq_s']:.2f}s (boto3 sequential)")
        print(f"    Cleanup:  {bulk['cleanup_s']:.2f}s")
        print(f"  Files:     {bulk['file_count']}")
        print(f"  Staged:    {bulk['total_mb']} MB")
        print(f"  Arrow mem: {bulk['arrow_memory_mb']} MB")
        if baseline['duration_s'] > 0:
            speedup = baseline['duration_s'] / bulk['total_s']
            print(f"  Speedup:   {speedup:.2f}x")

    conn.close()


if __name__ == "__main__":
    main()
