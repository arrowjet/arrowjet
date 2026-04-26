"""
Three-way write benchmark: write_dataframe (INSERT) vs manual COPY vs arrowjet write_bulk.
Run on EC2 (same region as Redshift) for accurate numbers.

Lane 1: redshift_connector write_dataframe()  - INSERT under the hood (naive baseline)
Lane 2: Manual COPY  - upload Parquet to S3, run COPY (50 lines of boilerplate)
Lane 3: Arrowjet write_bulk()  - same COPY path, automatic staging

Usage:
    export REDSHIFT_HOST=... REDSHIFT_PASS=... STAGING_BUCKET=... STAGING_IAM_ROLE=...
    BENCH_ROWS=1000000 python -u write_benchmark.py
"""

import os, sys, time, uuid, tempfile
import pyarrow as pa
import pyarrow.parquet as pq
import redshift_connector
import boto3

sys.path.insert(0, os.path.expanduser("~/arrowjet/src"))

REDSHIFT_HOST = os.environ["REDSHIFT_HOST"]
REDSHIFT_PASS = os.environ["REDSHIFT_PASS"]
STAGING_BUCKET = os.environ["STAGING_BUCKET"]
STAGING_IAM_ROLE = os.environ["STAGING_IAM_ROLE"]
STAGING_REGION = os.environ.get("STAGING_REGION", "us-east-1")
DB = os.environ.get("REDSHIFT_DATABASE", "dev")
USER = os.environ.get("REDSHIFT_USER", "awsuser")
ROWS = int(os.environ.get("BENCH_ROWS", "1000000"))

TARGET = f"write_bench_{uuid.uuid4().hex[:8]}"

print("=" * 60, flush=True)
print(f"Three-way write benchmark: {ROWS:,} rows, same-region EC2", flush=True)
print("=" * 60, flush=True)


def get_conn(autocommit=True):
    conn = redshift_connector.connect(
        host=REDSHIFT_HOST, port=5439, database=DB, user=USER, password=REDSHIFT_PASS
    )
    conn.autocommit = autocommit
    return conn


def make_table(rows):
    import random
    return pa.table({
        "id": list(range(rows)),
        "value": [random.uniform(0, 1000) for _ in range(rows)],
        "label": [f"label_{i % 100}" for i in range(rows)],
    })


def setup_target(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    cursor.execute(f"""
        CREATE TABLE {table_name} (
            id BIGINT, value DOUBLE PRECISION, label VARCHAR(32)
        )
    """)


def teardown(conn, table_name):
    try:
        conn.cursor().execute(f"DROP TABLE IF EXISTS {table_name}")
    except Exception:
        pass


print(f"\nGenerating {ROWS:,} rows of test data...", flush=True)
arrow_table = make_table(ROWS)
print(f"Data ready: {arrow_table.nbytes / 1024 / 1024:.1f} MB in memory\n", flush=True)

# --- Lane 1: redshift_connector write_dataframe (INSERT) ---
print(f"[1/3] redshift_connector write_dataframe (INSERT)...", flush=True)
conn = get_conn(autocommit=False)
setup_target(conn, TARGET)
conn.commit()

start = time.perf_counter()
df = arrow_table.to_pandas()
conn.cursor().write_dataframe(df, TARGET)
conn.commit()
insert_time = time.perf_counter() - start
print(f"  write_dataframe: {ROWS:,} rows in {insert_time:.2f}s", flush=True)
teardown(conn, TARGET)
conn.commit()
conn.close()

# --- Lane 2: Manual COPY ---
print(f"\n[2/3] Manual COPY (upload Parquet to S3, run COPY)...", flush=True)
conn = get_conn(autocommit=True)
setup_target(conn, TARGET)

start = time.perf_counter()
with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
    tmp_path = f.name
pq.write_table(arrow_table, tmp_path, compression="snappy")

prefix = f"write-bench-manual/{uuid.uuid4().hex[:12]}/"
s3_key = f"{prefix}data.parquet"
s3 = boto3.client("s3", region_name=STAGING_REGION)
s3.upload_file(tmp_path, STAGING_BUCKET, s3_key)
upload_time = time.perf_counter() - start

conn.cursor().execute(f"""
    COPY {TARGET}
    FROM 's3://{STAGING_BUCKET}/{s3_key}'
    IAM_ROLE '{STAGING_IAM_ROLE}'
    FORMAT PARQUET
""")
manual_time = time.perf_counter() - start
print(f"  Manual COPY: {ROWS:,} rows in {manual_time:.2f}s (upload={upload_time:.2f}s)", flush=True)

s3.delete_object(Bucket=STAGING_BUCKET, Key=s3_key)
os.unlink(tmp_path)
teardown(conn, TARGET)
conn.close()

# --- Lane 3: Arrowjet write_bulk ---
print(f"\n[3/3] Arrowjet write_bulk...", flush=True)
conn = get_conn(autocommit=True)
setup_target(conn, TARGET)
conn.close()

import arrowjet
aj_conn = arrowjet.connect(
    host=REDSHIFT_HOST, database=DB, user=USER, password=REDSHIFT_PASS,
    staging_bucket=STAGING_BUCKET, staging_iam_role=STAGING_IAM_ROLE,
    staging_region=STAGING_REGION,
)

start = time.perf_counter()
result = aj_conn.write_bulk(arrow_table, TARGET)
arrowjet_time = time.perf_counter() - start
print(f"  Arrowjet: {ROWS:,} rows in {arrowjet_time:.2f}s", flush=True)
aj_conn.close()

conn = get_conn(autocommit=True)
teardown(conn, TARGET)
conn.close()

# --- Summary ---
print("\n" + "=" * 60, flush=True)
print(f"{'Approach':<35} {'Time':>10} {'vs INSERT':>12}", flush=True)
print("-" * 60, flush=True)
print(f"{'write_dataframe (INSERT)':<35} {insert_time:>9.2f}s {'baseline':>12}", flush=True)
print(f"{'Manual COPY':<35} {manual_time:>9.2f}s {f'{insert_time/manual_time:.0f}x faster':>12}", flush=True)
print(f"{'Arrowjet write_bulk':<35} {arrowjet_time:>9.2f}s {f'{insert_time/arrowjet_time:.0f}x faster':>12}", flush=True)
print("=" * 60, flush=True)
