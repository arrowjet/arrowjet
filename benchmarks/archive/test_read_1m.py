"""Quick test: just the 1M read benchmark to validate the v2 bulk_ops fix."""

import time
import redshift_connector
from config_loader import load_config
from bulk_ops import read_bulk


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

    table_name = "benchmark_test_1m"

    # Baseline: direct fetch
    print("--- BASELINE: direct fetch ---")
    cursor = conn.cursor()
    start = time.perf_counter()
    cursor.execute(f"SELECT * FROM {table_name}")
    df = cursor.fetch_dataframe()
    baseline_time = time.perf_counter() - start
    print(f"  {len(df):,} rows in {baseline_time:.2f}s ({len(df)/baseline_time:,.0f} rows/s)")

    # Bulk: UNLOAD -> S3 -> Parquet -> Arrow
    print("\n--- BULK: UNLOAD -> S3 -> Parquet -> Arrow ---")
    start = time.perf_counter()
    arrow_table = read_bulk(
        conn=conn,
        query=f"SELECT * FROM {table_name}",
        bucket=stg["bucket"],
        prefix=stg["prefix"],
        iam_role=stg["iam_role"],
        region=stg["region"],
    )
    bulk_time = time.perf_counter() - start
    print(f"  {arrow_table.num_rows:,} rows in {bulk_time:.2f}s ({arrow_table.num_rows/bulk_time:,.0f} rows/s)")

    speedup = baseline_time / bulk_time
    print(f"\n>> Speedup: {speedup:.1f}x")

    conn.close()


if __name__ == "__main__":
    main()
