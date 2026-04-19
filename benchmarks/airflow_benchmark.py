"""
Three-way benchmark: naive fetch vs manual UNLOAD vs arrowjet.
Run on EC2 (same region as Redshift) for accurate numbers.
"""
import os, sys, time, uuid

sys.path.insert(0, os.path.expanduser("~/arrowjet/src"))

REDSHIFT_HOST = os.environ["REDSHIFT_HOST"]
REDSHIFT_PASS = os.environ["REDSHIFT_PASS"]
STAGING_BUCKET = os.environ["STAGING_BUCKET"]
STAGING_IAM_ROLE = os.environ["STAGING_IAM_ROLE"]
STAGING_REGION = os.environ.get("STAGING_REGION", "us-east-1")
DB = os.environ.get("REDSHIFT_DATABASE", "dev")
USER = os.environ.get("REDSHIFT_USER", "awsuser")

TABLE = os.environ.get("BENCH_TABLE", "benchmark_test_1m")
QUERY = f"SELECT * FROM {TABLE}"

print("=" * 60)
print(f"Three-way benchmark: {TABLE}, same-region EC2")
print("=" * 60)

# --- 1. Naive fetch ---
print("\n[1/3] Naive fetch (cursor.fetchall)...")
import redshift_connector
import pyarrow as pa

conn = redshift_connector.connect(host=REDSHIFT_HOST, port=5439, database=DB, user=USER, password=REDSHIFT_PASS)
conn.autocommit = True
conn.cursor().execute("SET enable_result_cache_for_session = off")

start = time.perf_counter()
cursor = conn.cursor()
cursor.execute(QUERY)
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
naive_time = time.perf_counter() - start
print(f"  Naive fetch: {len(rows):,} rows in {naive_time:.2f}s")
conn.close()

# --- 2. Manual UNLOAD ---
print("\n[2/3] Manual UNLOAD...")
import boto3

conn = redshift_connector.connect(host=REDSHIFT_HOST, port=5439, database=DB, user=USER, password=REDSHIFT_PASS)
conn.autocommit = True
conn.cursor().execute("SET enable_result_cache_for_session = off")

prefix = f"benchmark-manual/{uuid.uuid4().hex[:12]}/"
s3_uri = f"s3://{STAGING_BUCKET}/{prefix}"

start = time.perf_counter()
cursor = conn.cursor()
cursor.execute(f"""
    UNLOAD ($$SELECT * FROM {TABLE}$$)
    TO '{s3_uri}'
    IAM_ROLE '{STAGING_IAM_ROLE}'
    FORMAT PARQUET
    ALLOWOVERWRITE
    PARALLEL ON
""")
unload_phase = time.perf_counter() - start

import pyarrow.parquet as pq
import io
s3 = boto3.client("s3", region_name=STAGING_REGION)
files = []
paginator = s3.get_paginator("list_objects_v2")
for page in paginator.paginate(Bucket=STAGING_BUCKET, Prefix=prefix):
    for obj in page.get("Contents", []):
        if obj["Key"].endswith(".parquet"):
            files.append(obj["Key"])

tables = []
for key in files:
    obj = s3.get_object(Bucket=STAGING_BUCKET, Key=key)
    tables.append(pq.read_table(io.BytesIO(obj["Body"].read())))
table = pa.concat_tables(tables)
manual_time = time.perf_counter() - start
print(f"  Manual UNLOAD: {table.num_rows:,} rows in {manual_time:.2f}s (unload={unload_phase:.2f}s)")

# cleanup
for page in paginator.paginate(Bucket=STAGING_BUCKET, Prefix=prefix):
    for obj in page.get("Contents", []):
        s3.delete_object(Bucket=STAGING_BUCKET, Key=obj["Key"])
conn.close()

# --- 3. Arrowjet ---
print("\n[3/3] Arrowjet...")
import arrowjet

bconn = arrowjet.connect(
    host=REDSHIFT_HOST, database=DB, user=USER, password=REDSHIFT_PASS,
    staging_bucket=STAGING_BUCKET, staging_iam_role=STAGING_IAM_ROLE,
    staging_region=STAGING_REGION,
)
# disable result cache via the internal rs connection
bconn._rs_conn.cursor().execute("SET enable_result_cache_for_session = off")

start = time.perf_counter()
result = bconn.read_bulk(QUERY)
arrowjet_time = time.perf_counter() - start
print(f"  Arrowjet: {result.rows:,} rows in {arrowjet_time:.2f}s")
bconn.close()

# --- Summary ---
print("\n" + "=" * 60)
print(f"{'Approach':<30} {'Time':>10} {'vs Naive':>10}")
print("-" * 60)
print(f"{'Naive fetch':<30} {naive_time:>9.2f}s {'baseline':>10}")
print(f"{'Manual UNLOAD':<30} {manual_time:>9.2f}s {f'{naive_time/manual_time:.1f}x':>10}")
print(f"{'Arrowjet':<30} {arrowjet_time:>9.2f}s {f'{naive_time/arrowjet_time:.1f}x':>10}")
print("=" * 60)
