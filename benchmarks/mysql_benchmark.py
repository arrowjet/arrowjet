"""
MySQL bulk benchmark: LOAD DATA LOCAL INFILE vs INSERT.

Usage:
    MYSQL_HOST=... MYSQL_PASS=... PYTHONPATH=src python -u benchmarks/mysql_benchmark.py
"""

import os, sys, time
import pandas as pd
import pyarrow as pa
import pymysql

sys.stdout.reconfigure(line_buffering=True)

HOST = os.environ.get("MYSQL_HOST")
PASS = os.environ.get("MYSQL_PASS")
PORT = int(os.environ.get("MYSQL_PORT", "3306"))
DB = os.environ.get("MYSQL_DATABASE", "dev")
USER = os.environ.get("MYSQL_USER", "awsuser")

if not HOST or not PASS:
    sys.exit("Error: MYSQL_HOST and MYSQL_PASS environment variables required.")

ROWS = int(os.environ.get("BENCH_ROWS", "1000000"))

def get_conn():
    return pymysql.connect(host=HOST, port=PORT, database=DB, user=USER,
                           password=PASS, connect_timeout=10, local_infile=True)

def setup_table(conn, name):
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {name}")
    cursor.execute(f"CREATE TABLE {name} (id BIGINT, value DOUBLE, label VARCHAR(32))")
    conn.commit()

def teardown(conn, name):
    conn.cursor().execute(f"DROP TABLE IF EXISTS {name}")
    conn.commit()

def verify(conn, name, expected):
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {name}")
    actual = cursor.fetchone()[0]
    assert actual == expected, f"Expected {expected}, got {actual}"

print(f"Generating {ROWS:,} rows...", flush=True)
data = [(i, i * 1.1, f"label_{i % 100}") for i in range(ROWS)]
df = pd.DataFrame(data, columns=["id", "value", "label"])
print(f"Data ready.\n", flush=True)

results = {}

# Lane 1: executemany (capped at 10K, extrapolate)
BATCH = 1000
EXEC_ROWS = min(ROWS, 10000)
print(f"[1/4] executemany (batch={BATCH}, {EXEC_ROWS:,} rows)...", flush=True)
conn = get_conn()
setup_table(conn, "bench_mysql_exec")
cursor = conn.cursor()
start = time.perf_counter()
for i in range(0, EXEC_ROWS, BATCH):
    batch = data[i:i+BATCH]
    cursor.executemany("INSERT INTO bench_mysql_exec (id, value, label) VALUES (%s, %s, %s)", batch)
conn.commit()
t = time.perf_counter() - start
rate = EXEC_ROWS / t
results["executemany"] = {"time": t, "rate": rate, "extrapolated": ROWS / rate}
print(f"  {EXEC_ROWS:,} rows in {t:.2f}s ({rate:.0f}/s), extrapolated {ROWS:,}: {ROWS/rate:.1f}s\n", flush=True)
teardown(conn, "bench_mysql_exec")
conn.close()

# Lane 2: Multi-row VALUES (capped at 10K)
VALUES_ROWS = min(ROWS, 10000)
print(f"[2/4] Multi-row VALUES (batch={BATCH}, {VALUES_ROWS:,} rows)...", flush=True)
conn = get_conn()
setup_table(conn, "bench_mysql_vals")
cursor = conn.cursor()
start = time.perf_counter()
for i in range(0, VALUES_ROWS, BATCH):
    batch = data[i:i+BATCH]
    vals = ",".join(f"({r[0]},{r[1]},'{r[2]}')" for r in batch)
    cursor.execute(f"INSERT INTO bench_mysql_vals (id, value, label) VALUES {vals}")
conn.commit()
t = time.perf_counter() - start
rate = VALUES_ROWS / t
results["values"] = {"time": t, "rate": rate, "extrapolated": ROWS / rate}
print(f"  {VALUES_ROWS:,} rows in {t:.2f}s ({rate:.0f}/s), extrapolated {ROWS:,}: {ROWS/rate:.1f}s\n", flush=True)
teardown(conn, "bench_mysql_vals")
conn.close()

# Lane 3: Arrowjet LOAD DATA write (full rows)
print(f"[3/4] Arrowjet LOAD DATA write ({ROWS:,} rows)...", flush=True)
from arrowjet.engine import Engine
engine = Engine(provider="mysql")
conn = get_conn()
setup_table(conn, "bench_mysql_load")
start = time.perf_counter()
engine.write_dataframe(conn, df, "bench_mysql_load")
t = time.perf_counter() - start
verify(conn, "bench_mysql_load", ROWS)
results["arrowjet_write"] = {"time": t, "rate": ROWS / t}
print(f"  {ROWS:,} rows in {t:.2f}s ({ROWS/t:.0f}/s)\n", flush=True)
teardown(conn, "bench_mysql_load")
conn.close()

# Lane 4: Arrowjet read (full rows)
print(f"[4/4] Arrowjet read ({ROWS:,} rows)...", flush=True)
conn = get_conn()
setup_table(conn, "bench_mysql_read")
engine.write_dataframe(conn, df, "bench_mysql_read")
start = time.perf_counter()
result = engine.read_bulk(conn, "SELECT * FROM bench_mysql_read")
t = time.perf_counter() - start
assert result.rows == ROWS
results["arrowjet_read"] = {"time": t, "rate": ROWS / t}
print(f"  {ROWS:,} rows in {t:.2f}s ({ROWS/t:.0f}/s)\n", flush=True)

# Bonus: cursor.fetchall
print(f"[bonus] cursor.fetchall ({ROWS:,} rows)...", flush=True)
cursor = conn.cursor()
start = time.perf_counter()
cursor.execute("SELECT * FROM bench_mysql_read")
rows = cursor.fetchall()
t = time.perf_counter() - start
assert len(rows) == ROWS
results["fetchall"] = {"time": t, "rate": ROWS / t}
print(f"  {ROWS:,} rows in {t:.2f}s ({ROWS/t:.0f}/s)\n", flush=True)
teardown(conn, "bench_mysql_read")
conn.close()

# Summary
aw = results["arrowjet_write"]
ar = results["arrowjet_read"]
print("=" * 70, flush=True)
print(f"{'MYSQL WRITE BENCHMARK':^70}", flush=True)
print(f"{'(' + f'{ROWS:,} rows, RDS MySQL 8.0' + ')':^70}", flush=True)
print("=" * 70, flush=True)
print(f"{'Approach':<35} {'Time':>10} {'Rate':>15} {'vs Arrowjet':>10}", flush=True)
print("-" * 70, flush=True)
for name, key in [("executemany (batch 1000)", "executemany"), ("Multi-row VALUES (batch 1000)", "values")]:
    r = results[key]
    ratio = r["extrapolated"] / aw["time"]
    print(f"{name:<35} {r['extrapolated']:>9.1f}s {r['rate']:>12.0f}/s {ratio:>9.1f}x", flush=True)
print(f"{'Arrowjet LOAD DATA':<35} {aw['time']:>9.2f}s {aw['rate']:>12.0f}/s {'baseline':>10}", flush=True)
print()
print("=" * 70, flush=True)
print(f"{'MYSQL READ BENCHMARK':^70}", flush=True)
print("=" * 70, flush=True)
print(f"{'Approach':<35} {'Time':>10} {'Rate':>15} {'vs Arrowjet':>10}", flush=True)
print("-" * 70, flush=True)
fa = results["fetchall"]
ratio = fa["time"] / ar["time"]
print(f"{'cursor.fetchall()':<35} {fa['time']:>9.2f}s {fa['rate']:>12.0f}/s {ratio:>9.1f}x", flush=True)
print(f"{'Arrowjet read':<35} {ar['time']:>9.2f}s {ar['rate']:>12.0f}/s {'baseline':>10}", flush=True)
print("=" * 70, flush=True)
