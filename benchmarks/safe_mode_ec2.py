"""
Safe mode benchmark on EC2: ADBC vs redshift_connector vs pyodbc.
1M and 10M rows. Same-region, no VPN overhead.
"""

import sys
import os
import time
import statistics
import platform

sys.stdout.reconfigure(line_buffering=True)

ODBC_DRIVER = "/opt/amazon/redshiftodbcx64/librsodbc64.so"
ITERATIONS = 3


def main():
    import adbc_driver_postgresql.dbapi
    import redshift_connector
    import pyodbc
    import pandas as pd

    host = os.environ["REDSHIFT_HOST"]
    pwd = os.environ["REDSHIFT_PASS"]

    print("=" * 70)
    print("SAFE MODE BENCHMARK (EC2, same-region)")
    print("=" * 70)
    print(f"  Platform: {platform.system()} {platform.machine()}")
    print(f"  Python: {platform.python_version()}")
    print(f"  Iterations: {ITERATIONS}")

    # Connect
    conn_adbc = adbc_driver_postgresql.dbapi.connect(f"postgresql://awsuser:{pwd}@{host}:5439/dev")
    conn_rs = redshift_connector.connect(host=host, port=5439, database="dev", user="awsuser", password=pwd)
    conn_odbc = pyodbc.connect(f"Driver={ODBC_DRIVER};Server={host};Port=5439;Database=dev;UID=awsuser;PWD={pwd};LogLevel=0;")

    # Disable cache
    conn_adbc.cursor().execute("SET enable_result_cache_for_session = off")
    conn_rs.cursor().execute("SET enable_result_cache_for_session = off")
    conn_odbc.cursor().execute("SET enable_result_cache_for_session = off")
    print("  Result cache: disabled\n")

    for table_name, label in [("benchmark_test_1m", "1M rows"), ("benchmark_test_10m", "10M rows")]:
        query = f"SELECT * FROM {table_name}"
        print(f"\n{'=' * 70}")
        print(f"{label} ({table_name}, 15 columns)")
        print(f"{'=' * 70}")

        # ADBC Arrow native
        print(f"\n  [adbc] fetch_arrow_table...")
        times = []
        for i in range(ITERATIONS):
            cursor = conn_adbc.cursor()
            t = time.perf_counter()
            cursor.execute(query)
            tbl = cursor.fetch_arrow_table()
            elapsed = time.perf_counter() - t
            times.append(elapsed)
            print(f"    run {i+1}: {elapsed:.2f}s ({tbl.num_rows:,} rows, {tbl.nbytes/1024/1024:.0f} MB)")
            cursor.close()
            del tbl
        adbc_arrow = statistics.median(times)
        print(f"    median: {adbc_arrow:.2f}s")

        # ADBC Arrow -> pandas
        print(f"\n  [adbc] fetch_arrow -> to_pandas...")
        times = []
        for i in range(ITERATIONS):
            cursor = conn_adbc.cursor()
            t = time.perf_counter()
            cursor.execute(query)
            tbl = cursor.fetch_arrow_table()
            df = tbl.to_pandas()
            elapsed = time.perf_counter() - t
            times.append(elapsed)
            print(f"    run {i+1}: {elapsed:.2f}s ({len(df):,} rows)")
            cursor.close()
            del tbl, df
        adbc_df = statistics.median(times)
        print(f"    median: {adbc_df:.2f}s")

        # redshift_connector fetch_dataframe
        print(f"\n  [redshift_connector] fetch_dataframe...")
        times = []
        for i in range(ITERATIONS):
            cursor = conn_rs.cursor()
            t = time.perf_counter()
            cursor.execute(query)
            df = cursor.fetch_dataframe()
            elapsed = time.perf_counter() - t
            times.append(elapsed)
            print(f"    run {i+1}: {elapsed:.2f}s ({len(df):,} rows)")
            del df
        rs_df = statistics.median(times)
        print(f"    median: {rs_df:.2f}s")

        # pyodbc fetchall -> DataFrame
        print(f"\n  [pyodbc] fetchall -> DataFrame...")
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
            print(f"    run {i+1}: {elapsed:.2f}s ({len(df):,} rows)")
            cursor.close()
            del data, df
        odbc_df = statistics.median(times)
        print(f"    median: {odbc_df:.2f}s")

        # Summary for this table
        print(f"\n  --- {label} SUMMARY (median) ---")
        print(f"  ADBC Arrow native:     {adbc_arrow:.2f}s")
        print(f"  ADBC Arrow -> pandas:   {adbc_df:.2f}s")
        print(f"  redshift_connector:    {rs_df:.2f}s")
        print(f"  pyodbc:                {odbc_df:.2f}s")
        print(f"  ADBC Arrow vs RS:      {rs_df/adbc_arrow:.2f}x")
        print(f"  ADBC DataFrame vs RS:  {rs_df/adbc_df:.2f}x")
        print(f"  ADBC Arrow vs pyodbc:  {odbc_df/adbc_arrow:.2f}x")

    conn_adbc.close()
    conn_rs.close()
    conn_odbc.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
