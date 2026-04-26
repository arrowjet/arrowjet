"""
PostgreSQL bulk benchmark: COPY protocol vs INSERT.

Compares:
  1. Row-by-row INSERT (executemany)  - the slow path everyone uses
  2. to_sql with chunksize (what pandas/awswrangler do)
  3. Arrowjet PostgreSQLEngine (COPY FROM STDIN / COPY TO STDOUT)

Usage:
    PG_HOST=... PG_PASS=... PYTHONPATH=src python -u benchmarks/pg_benchmark.py
"""

import os
import sys
import time

import pandas as pd
import pyarrow as pa
import psycopg2

sys.stdout.reconfigure(line_buffering=True)

# --- Config ---
HOST = os.environ.get("PG_HOST")
PASS = os.environ.get("PG_PASS")
PORT = int(os.environ.get("PG_PORT", "5432"))
DB = os.environ.get("PG_DATABASE", "dev")
USER = os.environ.get("PG_USER", "awsuser")

if not HOST or not PASS:
    sys.exit("Error: PG_HOST and PG_PASS environment variables required.")

ROWS = int(os.environ.get("BENCH_ROWS", "100000"))

def get_conn():
    return psycopg2.connect(host=HOST, port=PORT, dbname=DB, user=USER, password=PASS, connect_timeout=10)

def setup_table(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    cursor.execute(f"""
        CREATE TABLE {table_name} (
            id BIGINT,
            value DOUBLE PRECISION,
            label VARCHAR(32)
        )
    """)
    conn.commit()

def teardown_table(conn, table_name):
    conn.cursor().execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.commit()

def verify_count(conn, table_name, expected):
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    actual = cursor.fetchone()[0]
    assert actual == expected, f"Expected {expected}, got {actual}"
    return actual

# --- Generate test data ---
print(f"Generating {ROWS:,} rows...", flush=True)
data = [(i, i * 1.1, f"label_{i % 100}") for i in range(ROWS)]
df = pd.DataFrame(data, columns=["id", "value", "label"])
arrow_table = pa.Table.from_pandas(df, preserve_index=False)
print(f"Data ready.\n", flush=True)

results = {}

# ============================================================
# Lane 1: executemany (batch 1000)
# ============================================================
BATCH = 1000
# Cap at 10K rows for executemany (extrapolate)  - it's too slow for 100K
EXEC_ROWS = min(ROWS, 10000)
print(f"[1/4] executemany (batch={BATCH}, {EXEC_ROWS:,} rows, extrapolate to {ROWS:,})...", flush=True)

conn = get_conn()
table_name = "bench_pg_executemany"
setup_table(conn, table_name)
cursor = conn.cursor()

start = time.perf_counter()
for i in range(0, EXEC_ROWS, BATCH):
    batch = data[i:i+BATCH]
    cursor.executemany(
        f"INSERT INTO {table_name} (id, value, label) VALUES (%s, %s, %s)",
        batch,
    )
conn.commit()
exec_time = time.perf_counter() - start
exec_rate = EXEC_ROWS / exec_time
exec_extrapolated = ROWS / exec_rate

verify_count(conn, table_name, EXEC_ROWS)
teardown_table(conn, table_name)
conn.close()

results["executemany"] = {"rows": EXEC_ROWS, "time": exec_time, "rate": exec_rate, "extrapolated": exec_extrapolated}
print(f"  {EXEC_ROWS:,} rows in {exec_time:.2f}s ({exec_rate:.0f} rows/s)", flush=True)
print(f"  Extrapolated {ROWS:,} rows: {exec_extrapolated:.1f}s\n", flush=True)

# ============================================================
# Lane 2: Multi-row VALUES INSERT (batch 1000)
# ============================================================
VALUES_ROWS = min(ROWS, 10000)
print(f"[2/4] Multi-row VALUES (batch={BATCH}, {VALUES_ROWS:,} rows, extrapolate to {ROWS:,})...", flush=True)

conn = get_conn()
table_name = "bench_pg_values"
setup_table(conn, table_name)
cursor = conn.cursor()

start = time.perf_counter()
for i in range(0, VALUES_ROWS, BATCH):
    batch = data[i:i+BATCH]
    values_str = ",".join(f"({r[0]},{r[1]},'{r[2]}')" for r in batch)
    cursor.execute(f"INSERT INTO {table_name} (id, value, label) VALUES {values_str}")
conn.commit()
values_time = time.perf_counter() - start
values_rate = VALUES_ROWS / values_time
values_extrapolated = ROWS / values_rate

verify_count(conn, table_name, VALUES_ROWS)
teardown_table(conn, table_name)
conn.close()

results["values"] = {"rows": VALUES_ROWS, "time": values_time, "rate": values_rate, "extrapolated": values_extrapolated}
print(f"  {VALUES_ROWS:,} rows in {values_time:.2f}s ({values_rate:.0f} rows/s)", flush=True)
print(f"  Extrapolated {ROWS:,} rows: {values_extrapolated:.1f}s\n", flush=True)

# ============================================================
# Lane 3: Arrowjet COPY write (full rows)
# ============================================================
print(f"[3/4] Arrowjet COPY write ({ROWS:,} rows)...", flush=True)

from arrowjet.engine import PostgreSQLEngine
engine = PostgreSQLEngine()

conn = get_conn()
table_name = "bench_pg_copy_write"
setup_table(conn, table_name)

start = time.perf_counter()
write_result = engine.write_dataframe(conn, df, table_name)
copy_write_time = time.perf_counter() - start

verify_count(conn, table_name, ROWS)
teardown_table(conn, table_name)
conn.close()

results["arrowjet_write"] = {"rows": ROWS, "time": copy_write_time, "rate": ROWS / copy_write_time}
print(f"  {ROWS:,} rows in {copy_write_time:.2f}s ({ROWS/copy_write_time:.0f} rows/s)\n", flush=True)

# ============================================================
# Lane 4: Arrowjet COPY read (full rows)
# ============================================================
print(f"[4/4] Arrowjet COPY read ({ROWS:,} rows)...", flush=True)

# Setup: write data first using arrowjet (fast)
conn = get_conn()
table_name = "bench_pg_copy_read"
setup_table(conn, table_name)
engine.write_dataframe(conn, df, table_name)

# Now benchmark the read
start = time.perf_counter()
read_result = engine.read_bulk(conn, f"SELECT * FROM {table_name}")
copy_read_time = time.perf_counter() - start

assert read_result.rows == ROWS
teardown_table(conn, table_name)
conn.close()

results["arrowjet_read"] = {"rows": ROWS, "time": copy_read_time, "rate": ROWS / copy_read_time}
print(f"  {ROWS:,} rows in {copy_read_time:.2f}s ({ROWS/copy_read_time:.0f} rows/s)\n", flush=True)

# Also benchmark cursor.fetchall for read comparison
print(f"[bonus] cursor.fetchall read ({ROWS:,} rows)...", flush=True)
conn = get_conn()
table_name = "bench_pg_fetchall"
setup_table(conn, table_name)
engine.write_dataframe(conn, df, table_name)

cursor = conn.cursor()
start = time.perf_counter()
cursor.execute(f"SELECT * FROM {table_name}")
rows_fetched = cursor.fetchall()
fetchall_time = time.perf_counter() - start

assert len(rows_fetched) == ROWS
teardown_table(conn, table_name)
conn.close()

results["fetchall"] = {"rows": ROWS, "time": fetchall_time, "rate": ROWS / fetchall_time}
print(f"  {ROWS:,} rows in {fetchall_time:.2f}s ({ROWS/fetchall_time:.0f} rows/s)\n", flush=True)

# ============================================================
# Summary
# ============================================================
print("=" * 70, flush=True)
print(f"{'WRITE BENCHMARK':^70}", flush=True)
print(f"{'(' + f'{ROWS:,} rows, RDS PostgreSQL 16.6' + ')':^70}", flush=True)
print("=" * 70, flush=True)
print(f"{'Approach':<35} {'Time':>10} {'Rate':>15} {'vs Arrowjet':>10}", flush=True)
print("-" * 70, flush=True)

aw = results["arrowjet_write"]
for name, key in [("executemany (batch 1000)", "executemany"), ("Multi-row VALUES (batch 1000)", "values")]:
    r = results[key]
    ratio = r["extrapolated"] / aw["time"]
    print(f"{name:<35} {r['extrapolated']:>9.1f}s {r['rate']:>12.0f}/s {ratio:>9.1f}x", flush=True)

print(f"{'Arrowjet COPY write':<35} {aw['time']:>9.2f}s {aw['rate']:>12.0f}/s {'baseline':>10}", flush=True)

print()
print("=" * 70, flush=True)
print(f"{'READ BENCHMARK':^70}", flush=True)
print("=" * 70, flush=True)
print(f"{'Approach':<35} {'Time':>10} {'Rate':>15} {'vs Arrowjet':>10}", flush=True)
print("-" * 70, flush=True)

ar = results["arrowjet_read"]
fa = results["fetchall"]
ratio = fa["time"] / ar["time"]
print(f"{'cursor.fetchall()':<35} {fa['time']:>9.2f}s {fa['rate']:>12.0f}/s {ratio:>9.1f}x", flush=True)
print(f"{'Arrowjet COPY read':<35} {ar['time']:>9.2f}s {ar['rate']:>12.0f}/s {'baseline':>10}", flush=True)

print("=" * 70, flush=True)
