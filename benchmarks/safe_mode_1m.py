"""
Safe mode benchmark: 1M rows only — ADBC vs redshift_connector vs pyodbc.
Focused on the scale where driver differences matter.
"""

import sys
import os
import time
import statistics

sys.stdout.reconfigure(line_buffering=True)

QUERIES = {
    "1M rows": "SELECT * FROM benchmark_test_1m",
    "10M rows": "SELECT * FROM benchmark_test_10m",
}
ITERATIONS = 3


def main():
    import adbc_driver_postgresql.dbapi
    import redshift_connector
    import pyodbc

    host = os.environ["REDSHIFT_HOST"]
    pwd = os.environ["REDSHIFT_PASS"]

    print("=" * 70)
    print("SAFE MODE BENCHMARK: ADBC vs redshift_connector vs pyodbc")
    print("=" * 70)

    # Connect
    conn_adbc = adbc_driver_postgresql.dbapi.connect(f"postgresql://awsuser:{pwd}@{host}:5439/dev")
    conn_rs = redshift_connector.connect(host=host, port=5439, database="dev", user="awsuser", password=pwd)
    conn_odbc = pyodbc.connect(f"Driver=/opt/amazon/redshiftodbcx64/librsodbc64.dylib;Server={host};Port=5439;Database=dev;UID=awsuser;PWD={pwd};LogLevel=0;")

    # Disable cache
    conn_adbc.cursor().execute("SET enable_result_cache_for_session = off")
    conn_rs.cursor().execute("SET enable_result_cache_for_session = off")
    conn_odbc.cursor().execute("SET enable_result_cache_for_session = off")

    for label, query in QUERIES.items():
        print(f"\n{'=' * 70}")
        print(f"  {label} (15 columns)")
        print(f"{'=' * 70}")

        results = {}

        # 1. ADBC fetch_arrow_table (native)
        print(f"\n[adbc] fetch_arrow_table (native Arrow)...")
        times = []
        for i in range(ITERATIONS):
            cursor = conn_adbc.cursor()
            t = time.perf_counter()
            cursor.execute(query)
            table = cursor.fetch_arrow_table()
            elapsed = time.perf_counter() - t
            times.append(elapsed)
            print(f"  run {i+1}: {elapsed:.2f}s ({table.num_rows:,} rows, {table.nbytes/1024/1024:.0f} MB)")
            cursor.close()
        results["adbc_arrow"] = statistics.median(times)
        print(f"  median: {results['adbc_arrow']:.2f}s")

        # 2. ADBC to_dataframe
        print(f"\n[adbc] fetch_arrow → to_pandas...")
        times = []
        for i in range(ITERATIONS):
            cursor = conn_adbc.cursor()
            t = time.perf_counter()
            cursor.execute(query)
            table = cursor.fetch_arrow_table()
            df = table.to_pandas()
            elapsed = time.perf_counter() - t
            times.append(elapsed)
            print(f"  run {i+1}: {elapsed:.2f}s ({len(df):,} rows)")
            cursor.close()
            del df
        results["adbc_df"] = statistics.median(times)
        print(f"  median: {results['adbc_df']:.2f}s")

        # 3. redshift_connector fetch_dataframe
        print(f"\n[redshift_connector] fetch_dataframe...")
        times = []
        for i in range(ITERATIONS):
            cursor = conn_rs.cursor()
            t = time.perf_counter()
            cursor.execute(query)
            df = cursor.fetch_dataframe()
            elapsed = time.perf_counter() - t
            times.append(elapsed)
            print(f"  run {i+1}: {elapsed:.2f}s ({len(df):,} rows)")
            del df
        results["rs_df"] = statistics.median(times)
        print(f"  median: {results['rs_df']:.2f}s")

        # 4. pyodbc fetchall → DataFrame
        print(f"\n[pyodbc] fetchall → DataFrame...")
        import pandas as pd
        times = []
        for i in range(ITERATIONS):
            cursor = conn_odbc.cursor()
            t = time.perf_counter()
            cursor.execute(query)
            cols = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            df = pd.DataFrame.from_records(data, columns=cols)
            elapsed = time.perf_counter() - t
            times.append(elapsed)
            print(f"  run {i+1}: {elapsed:.2f}s ({len(df):,} rows)")
            cursor.close()
            del df, data
        results["odbc_df"] = statistics.median(times)
        print(f"  median: {results['odbc_df']:.2f}s")

        # Summary for this query
        print(f"\n  SUMMARY: {label} to DataFrame (median)")
        print(f"  {'─' * 50}")
        print(f"  ADBC (Arrow native):       {results['adbc_arrow']:.2f}s")
        print(f"  ADBC (Arrow → pandas):     {results['adbc_df']:.2f}s")
        print(f"  redshift_connector:        {results['rs_df']:.2f}s")
        print(f"  pyodbc:                    {results['odbc_df']:.2f}s")
        print()
        print(f"  ADBC Arrow vs RS connector: {results['rs_df']/results['adbc_arrow']:.2f}x faster")
        print(f"  ADBC DataFrame vs RS:       {results['rs_df']/results['adbc_df']:.2f}x faster")
        print(f"  ADBC Arrow vs pyodbc:       {results['odbc_df']/results['adbc_arrow']:.2f}x faster")

    conn_adbc.close()
    conn_rs.close()
    conn_odbc.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
