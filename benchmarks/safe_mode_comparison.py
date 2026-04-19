"""
Safe mode benchmark: ADBC vs redshift_connector vs pyodbc.

Compares the three Python drivers for standard query execution (safe mode).
This is NOT about bulk operations — it's about the baseline driver performance
that tools like SQLAlchemy, dbt, and BI tools would see.

Measures:
  - Small query (SELECT 1)
  - Medium query (1K rows)
  - Large query (100K rows)
  - Arrow-native fetch (ADBC only)

Run with:
  set -a && source .env && set +a
  PYTHONPATH=src .venv/bin/python benchmarks/safe_mode_comparison.py
"""

import sys
import os
import time
import statistics

sys.stdout.reconfigure(line_buffering=True)

# ── Drivers ───────────────────────────────────────────────

def connect_adbc():
    import adbc_driver_postgresql.dbapi
    host = os.environ["REDSHIFT_HOST"]
    pwd = os.environ["REDSHIFT_PASS"]
    return adbc_driver_postgresql.dbapi.connect(
        f"postgresql://awsuser:{pwd}@{host}:5439/dev"
    )


def connect_redshift_connector():
    import redshift_connector
    return redshift_connector.connect(
        host=os.environ["REDSHIFT_HOST"],
        port=5439, database="dev",
        user="awsuser", password=os.environ["REDSHIFT_PASS"],
    )


def connect_pyodbc():
    import pyodbc
    return pyodbc.connect(
        f"Driver=/opt/amazon/redshiftodbcx64/librsodbc64.dylib;"
        f"Server={os.environ['REDSHIFT_HOST']};"
        f"Port=5439;Database=dev;"
        f"UID=awsuser;PWD={os.environ['REDSHIFT_PASS']};"
        f"LogLevel=0;"
    )


# ── Benchmark functions ───────────────────────────────────

def bench_fetchall(conn, query, driver_name, iterations=5):
    """Benchmark: execute + fetchall (rows as tuples)."""
    times = []
    rows = 0
    for i in range(iterations):
        cursor = conn.cursor()
        t = time.perf_counter()
        cursor.execute(query)
        result = cursor.fetchall()
        elapsed = time.perf_counter() - t
        times.append(elapsed)
        rows = len(result)
        cursor.close()
    return {
        "driver": driver_name,
        "method": "fetchall",
        "rows": rows,
        "times": times,
        "median_s": round(statistics.median(times), 4),
        "min_s": round(min(times), 4),
        "max_s": round(max(times), 4),
    }


def bench_fetch_dataframe(conn, query, driver_name, iterations=5):
    """Benchmark: execute + fetch as DataFrame."""
    times = []
    rows = 0
    for i in range(iterations):
        if driver_name == "adbc":
            cursor = conn.cursor()
            t = time.perf_counter()
            cursor.execute(query)
            table = cursor.fetch_arrow_table()
            df = table.to_pandas()
            elapsed = time.perf_counter() - t
            rows = len(df)
            cursor.close()
        elif driver_name == "redshift_connector":
            cursor = conn.cursor()
            t = time.perf_counter()
            cursor.execute(query)
            df = cursor.fetch_dataframe()
            elapsed = time.perf_counter() - t
            rows = len(df)
        elif driver_name == "pyodbc":
            import pandas as pd
            cursor = conn.cursor()
            t = time.perf_counter()
            cursor.execute(query)
            cols = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            df = pd.DataFrame.from_records(data, columns=cols)
            elapsed = time.perf_counter() - t
            rows = len(df)
            cursor.close()
        times.append(elapsed)
    return {
        "driver": driver_name,
        "method": "to_dataframe",
        "rows": rows,
        "times": times,
        "median_s": round(statistics.median(times), 4),
        "min_s": round(min(times), 4),
        "max_s": round(max(times), 4),
    }


def bench_fetch_arrow(conn, query, iterations=5):
    """Benchmark: ADBC-only Arrow-native fetch (zero-copy)."""
    times = []
    rows = 0
    for i in range(iterations):
        cursor = conn.cursor()
        t = time.perf_counter()
        cursor.execute(query)
        table = cursor.fetch_arrow_table()
        elapsed = time.perf_counter() - t
        rows = table.num_rows
        cursor.close()
        times.append(elapsed)
    return {
        "driver": "adbc",
        "method": "fetch_arrow (native)",
        "rows": rows,
        "times": times,
        "median_s": round(statistics.median(times), 4),
        "min_s": round(min(times), 4),
        "max_s": round(max(times), 4),
    }


# ── Main ──────────────────────────────────────────────────

QUERIES = {
    "tiny (SELECT 1)": "SELECT 1 AS id, 'hello' AS name",
    "1K rows": "SELECT n AS id, RANDOM() * 1000 AS value FROM (SELECT ROW_NUMBER() OVER () AS n FROM stv_blocklist LIMIT 1000)",
    "10K rows": "SELECT n AS id, RANDOM() * 1000 AS value FROM (SELECT ROW_NUMBER() OVER () AS n FROM stv_blocklist LIMIT 10000)",
    "1M rows": "SELECT * FROM benchmark_test_1m",
}

ITERATIONS = 3  # reduced from 5 — 1M row tests take ~15s each


def main():
    print("=" * 70)
    print("SAFE MODE BENCHMARK: ADBC vs redshift_connector vs pyodbc")
    print("=" * 70)
    print(f"Iterations per test: {ITERATIONS}")

    # Connect all three
    print("\nConnecting...")
    conn_adbc = connect_adbc()
    conn_rs = connect_redshift_connector()
    conn_odbc = connect_pyodbc()

    # Disable result cache
    conn_adbc.cursor().execute("SET enable_result_cache_for_session = off")
    conn_rs.cursor().execute("SET enable_result_cache_for_session = off")
    conn_odbc.cursor().execute("SET enable_result_cache_for_session = off")

    print("Connected. Result cache disabled.\n")

    all_results = []

    for label, query in QUERIES.items():
        print(f"\n{'─' * 70}")
        print(f"Query: {label}")
        print(f"{'─' * 70}")

        # fetchall comparison
        for name, conn in [("adbc", conn_adbc), ("redshift_connector", conn_rs), ("pyodbc", conn_odbc)]:
            r = bench_fetchall(conn, query, name, ITERATIONS)
            all_results.append({**r, "query": label})
            print(f"  [{name:<22}] fetchall:     {r['median_s']:.4f}s (min={r['min_s']}, max={r['max_s']}, rows={r['rows']})")

        # DataFrame comparison
        for name, conn in [("adbc", conn_adbc), ("redshift_connector", conn_rs), ("pyodbc", conn_odbc)]:
            r = bench_fetch_dataframe(conn, query, name, ITERATIONS)
            all_results.append({**r, "query": label})
            print(f"  [{name:<22}] to_dataframe: {r['median_s']:.4f}s (min={r['min_s']}, max={r['max_s']}, rows={r['rows']})")

        # Arrow-native (ADBC only)
        r = bench_fetch_arrow(conn_adbc, query, ITERATIONS)
        all_results.append({**r, "query": label})
        print(f"  [{'adbc':<22}] fetch_arrow:  {r['median_s']:.4f}s (min={r['min_s']}, max={r['max_s']}, rows={r['rows']})")

    # Summary table
    print(f"\n{'=' * 70}")
    print("SUMMARY (median times)")
    print(f"{'=' * 70}")
    print(f"{'Query':<20} {'Method':<18} {'ADBC':>8} {'RS Conn':>8} {'pyodbc':>8} {'ADBC vs RS':>10}")
    print("─" * 74)

    for label in QUERIES:
        for method in ["fetchall", "to_dataframe"]:
            adbc_t = next((r["median_s"] for r in all_results if r["query"] == label and r["driver"] == "adbc" and r["method"] == method), None)
            rs_t = next((r["median_s"] for r in all_results if r["query"] == label and r["driver"] == "redshift_connector" and r["method"] == method), None)
            odbc_t = next((r["median_s"] for r in all_results if r["query"] == label and r["driver"] == "pyodbc" and r["method"] == method), None)

            ratio = f"{rs_t/adbc_t:.2f}x" if adbc_t and rs_t and adbc_t > 0 else "N/A"
            print(f"{label:<20} {method:<18} {adbc_t or 'N/A':>8} {rs_t or 'N/A':>8} {odbc_t or 'N/A':>8} {ratio:>10}")

        # Arrow row
        arrow_t = next((r["median_s"] for r in all_results if r["query"] == label and r["method"] == "fetch_arrow (native)"), None)
        if arrow_t:
            print(f"{label:<20} {'fetch_arrow':<18} {arrow_t:>8} {'N/A':>8} {'N/A':>8} {'(native)':>10}")

    conn_adbc.close()
    conn_rs.close()
    conn_odbc.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
