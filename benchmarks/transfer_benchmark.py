"""
Cross-database transfer benchmark.

Tests: PG->MySQL, MySQL->PG, PG->Redshift, Redshift->PG

Usage:
    PG_HOST=... MYSQL_HOST=... REDSHIFT_HOST=... PYTHONPATH=src python -u benchmarks/transfer_benchmark.py
"""

import os, sys, time
import pandas as pd
import pyarrow as pa

sys.stdout.reconfigure(line_buffering=True)

PG_HOST = os.environ.get("PG_HOST")
PG_PASS = os.environ.get("PG_PASS")
MYSQL_HOST = os.environ.get("MYSQL_HOST")
MYSQL_PASS = os.environ.get("MYSQL_PASS")
RS_HOST = os.environ.get("REDSHIFT_HOST")
RS_PASS = os.environ.get("REDSHIFT_PASS")
STAGING_BUCKET = os.environ.get("STAGING_BUCKET")
STAGING_IAM_ROLE = os.environ.get("STAGING_IAM_ROLE")
STAGING_REGION = os.environ.get("STAGING_REGION", "us-east-1")

ROWS = int(os.environ.get("BENCH_ROWS", "100000"))

from arrowjet.engine import Engine
from arrowjet.transfer import transfer

def get_pg():
    import psycopg2
    return psycopg2.connect(host=PG_HOST, port=5432, dbname="dev",
                            user="awsuser", password=PG_PASS, connect_timeout=10)

def get_mysql():
    import pymysql
    return pymysql.connect(host=MYSQL_HOST, port=3306, database="dev",
                           user="awsuser", password=MYSQL_PASS,
                           connect_timeout=10, local_infile=True)

def get_rs():
    import redshift_connector
    conn = redshift_connector.connect(host=RS_HOST, port=5439, database="dev",
                                      user="awsuser", password=RS_PASS)
    conn.autocommit = True
    return conn

pg_engine = Engine(provider="postgresql")
mysql_engine = Engine(provider="mysql")
rs_engine = None
if STAGING_BUCKET and STAGING_IAM_ROLE:
    rs_engine = Engine(provider="redshift", staging_bucket=STAGING_BUCKET,
                       staging_iam_role=STAGING_IAM_ROLE, staging_region=STAGING_REGION)

# Setup: create source table in PG with test data
print(f"Setting up {ROWS:,} rows in PostgreSQL source...", flush=True)
pg = get_pg()
cursor = pg.cursor()
cursor.execute("DROP TABLE IF EXISTS xfer_bench_src")
cursor.execute("CREATE TABLE xfer_bench_src (id BIGINT, value DOUBLE PRECISION, label VARCHAR(32))")
df = pd.DataFrame({"id": range(ROWS), "value": [i * 1.1 for i in range(ROWS)],
                    "label": [f"label_{i % 100}" for i in range(ROWS)]})
pg_engine.write_dataframe(pg, df, "xfer_bench_src")
pg.commit()
print(f"Source ready.\n", flush=True)

results = []

def run_transfer(name, src_engine, src_conn_fn, query, dst_engine, dst_conn_fn, dst_table_ddl, dst_table):
    print(f"[{name}] Transferring {ROWS:,} rows...", flush=True)
    dst = dst_conn_fn()
    c = dst.cursor()
    c.execute(f"DROP TABLE IF EXISTS {dst_table}")
    c.execute(dst_table_ddl)
    if hasattr(dst, 'commit'):
        dst.commit()

    src = src_conn_fn()
    start = time.perf_counter()
    result = transfer(
        source_engine=src_engine, source_conn=src,
        query=query,
        dest_engine=dst_engine, dest_conn=dst,
        dest_table=dst_table,
    )
    t = time.perf_counter() - start

    print(f"  {result.rows:,} rows in {t:.2f}s (read={result.read_time_s}s, write={result.write_time_s}s)\n", flush=True)
    results.append({"name": name, "rows": result.rows, "time": t,
                     "read": result.read_time_s, "write": result.write_time_s})

    c.execute(f"DROP TABLE IF EXISTS {dst_table}")
    if hasattr(dst, 'commit'):
        dst.commit()
    src.close()
    dst.close()

# PG -> MySQL
if MYSQL_HOST and MYSQL_PASS:
    run_transfer("PG -> MySQL", pg_engine, get_pg,
                 "SELECT * FROM xfer_bench_src",
                 mysql_engine, get_mysql,
                 "CREATE TABLE xfer_bench_dst (id BIGINT, value DOUBLE, label VARCHAR(32))",
                 "xfer_bench_dst")

# MySQL -> PG
if MYSQL_HOST and MYSQL_PASS:
    # First put data in MySQL
    my = get_mysql()
    c = my.cursor()
    c.execute("DROP TABLE IF EXISTS xfer_bench_my_src")
    c.execute("CREATE TABLE xfer_bench_my_src (id BIGINT, value DOUBLE, label VARCHAR(32))")
    my.commit()
    mysql_engine.write_dataframe(my, df, "xfer_bench_my_src")
    my.commit()

    run_transfer("MySQL -> PG", mysql_engine, get_mysql,
                 "SELECT * FROM xfer_bench_my_src",
                 pg_engine, get_pg,
                 "CREATE TABLE xfer_bench_dst (id BIGINT, value DOUBLE PRECISION, label VARCHAR(32))",
                 "xfer_bench_dst")

    my2 = get_mysql()
    my2.cursor().execute("DROP TABLE IF EXISTS xfer_bench_my_src")
    my2.commit()
    my2.close()

# PG -> Redshift
if rs_engine and RS_HOST and RS_PASS:
    run_transfer("PG -> Redshift", pg_engine, get_pg,
                 "SELECT * FROM xfer_bench_src",
                 rs_engine, get_rs,
                 "CREATE TABLE xfer_bench_dst (id BIGINT, value DOUBLE PRECISION, label VARCHAR(32))",
                 "xfer_bench_dst")

# Redshift -> PG
if rs_engine and RS_HOST and RS_PASS:
    # Put data in Redshift first
    rs = get_rs()
    rs.cursor().execute("DROP TABLE IF EXISTS xfer_bench_rs_src")
    rs.cursor().execute("CREATE TABLE xfer_bench_rs_src (id BIGINT, value DOUBLE PRECISION, label VARCHAR(32))")
    rs_engine.write_dataframe(rs, df, "xfer_bench_rs_src")

    run_transfer("Redshift -> PG", rs_engine, get_rs,
                 "SELECT * FROM xfer_bench_rs_src",
                 pg_engine, get_pg,
                 "CREATE TABLE xfer_bench_dst (id BIGINT, value DOUBLE PRECISION, label VARCHAR(32))",
                 "xfer_bench_dst")

    rs2 = get_rs()
    rs2.cursor().execute("DROP TABLE IF EXISTS xfer_bench_rs_src")
    rs2.close()

# Cleanup PG source
pg2 = get_pg()
pg2.cursor().execute("DROP TABLE IF EXISTS xfer_bench_src")
pg2.commit()
pg2.close()

# Summary
print("=" * 70, flush=True)
print(f"{'CROSS-DATABASE TRANSFER BENCHMARK':^70}", flush=True)
print(f"{'(' + f'{ROWS:,} rows' + ')':^70}", flush=True)
print("=" * 70, flush=True)
print(f"{'Path':<25} {'Total':>8} {'Read':>8} {'Write':>8} {'Rate':>12}", flush=True)
print("-" * 70, flush=True)
for r in results:
    rate = f"{r['rows']/r['time']:.0f}/s"
    print(f"{r['name']:<25} {r['time']:>7.2f}s {r['read']:>7.2f}s {r['write']:>7.2f}s {rate:>12}", flush=True)
print("=" * 70, flush=True)
