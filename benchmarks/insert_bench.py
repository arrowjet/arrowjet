"""
Fastest non-COPY INSERT approaches benchmark.
Lanes: multi-row VALUES, executemany, parallel inserts (4 threads)

Usage:
    REDSHIFT_HOST=... REDSHIFT_PASS=... python -u insert_bench.py
"""
import os, sys, time, uuid, random
from concurrent.futures import ThreadPoolExecutor
import redshift_connector

HOST = os.environ.get("REDSHIFT_HOST")
PASS = os.environ.get("REDSHIFT_PASS")
if not HOST or not PASS:
    sys.exit("Error: REDSHIFT_HOST and REDSHIFT_PASS environment variables are required.")
ROWS = int(os.environ.get("BENCH_ROWS", "1000000"))
TARGET = f"insert_bench_{uuid.uuid4().hex[:8]}"
BATCH = 5000

def get_conn():
    conn = redshift_connector.connect(host=HOST, port=5439, database="dev", user="awsuser", password=PASS)
    conn.autocommit = False
    return conn

def setup(conn):
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {TARGET}")
    cursor.execute(f"CREATE TABLE {TARGET} (id BIGINT, value DOUBLE PRECISION, label VARCHAR(32))")
    conn.autocommit = False

def teardown(conn):
    conn.autocommit = True
    conn.cursor().execute(f"DROP TABLE IF EXISTS {TARGET}")

print(f"Generating {ROWS:,} rows...", flush=True)
data = [(i, random.uniform(0, 1000), f"label_{i%100}") for i in range(ROWS)]
print(f"Data ready\n", flush=True)

# --- Lane 1: Multi-row VALUES (batch 5000) --- DONE: 195.77s
# Commented out — result already captured
# print(f"[1/3] Multi-row VALUES INSERT (batch={BATCH})...", flush=True)
# conn = get_conn()
# setup(conn)
# cursor = conn.cursor()
# start = time.perf_counter()
# for i in range(0, ROWS, BATCH):
#     batch = data[i:i+BATCH]
#     values = ",".join(f"({r[0]},{r[1]},'{r[2]}')" for r in batch)
#     cursor.execute(f"INSERT INTO {TARGET} VALUES {values}")
# conn.commit()
# values_time = time.perf_counter() - start
# print(f"  Multi-row VALUES: {ROWS:,} rows in {values_time:.2f}s", flush=True)
# teardown(conn)
# conn.close()
values_time = 195.77  # from previous run

# --- Lane 2: executemany (batch 5000) ---
print(f"\n[2/3] executemany (batch={BATCH})...", flush=True)
conn = get_conn()
setup(conn)
cursor = conn.cursor()
start = time.perf_counter()
for i in range(0, ROWS, BATCH):
    batch = data[i:i+BATCH]
    cursor.executemany(f"INSERT INTO {TARGET} VALUES (%s, %s, %s)", batch)
conn.commit()
execmany_time = time.perf_counter() - start
print(f"  executemany: {ROWS:,} rows in {execmany_time:.2f}s", flush=True)
teardown(conn)
conn.close()

# --- Lane 3: Parallel multi-row VALUES (4 threads) ---
THREADS = 4
print(f"\n[3/3] Parallel multi-row VALUES ({THREADS} threads, batch={BATCH})...", flush=True)
chunk_size = ROWS // THREADS

def insert_chunk(chunk_data):
    c = get_conn()
    c.autocommit = False
    cur = c.cursor()
    for i in range(0, len(chunk_data), BATCH):
        batch = chunk_data[i:i+BATCH]
        values = ",".join(f"({r[0]},{r[1]},'{r[2]}')" for r in batch)
        cur.execute(f"INSERT INTO {TARGET} VALUES {values}")
    c.commit()
    c.close()
    return len(chunk_data)

conn = get_conn()
setup(conn)
conn.close()

chunks = [data[i*chunk_size:(i+1)*chunk_size] for i in range(THREADS)]
start = time.perf_counter()
with ThreadPoolExecutor(max_workers=THREADS) as pool:
    results = list(pool.map(insert_chunk, chunks))
parallel_time = time.perf_counter() - start
print(f"  Parallel ({THREADS} threads): {ROWS:,} rows in {parallel_time:.2f}s", flush=True)

conn = get_conn()
teardown(conn)
conn.close()

# --- Summary ---
print("\n" + "=" * 60, flush=True)
print(f"{'Approach':<40} {'Time':>10}", flush=True)
print("-" * 60, flush=True)
print(f"{'write_dataframe (from earlier)':<40} {'48361s':>10}", flush=True)
print(f"{'Multi-row VALUES (batch 5000)':<40} {values_time:>9.2f}s", flush=True)
print(f"{'executemany (batch 5000)':<40} {execmany_time:>9.2f}s", flush=True)
print(f"{'Parallel VALUES (4 threads)':<40} {parallel_time:>9.2f}s", flush=True)
print(f"{'Manual COPY (from earlier)':<40} {'4.06s':>10}", flush=True)
print(f"{'Arrowjet write_bulk (from earlier)':<40} {'3.33s':>10}", flush=True)
print("=" * 60, flush=True)
