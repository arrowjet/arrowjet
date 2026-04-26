"""Quick manual verification of cross-database transfer correctness."""
import os, sys
sys.stdout.reconfigure(line_buffering=True)

import arrowjet
import psycopg2
import pymysql
import pandas as pd

pg = psycopg2.connect(
    host=os.environ["PG_HOST"], port=5432, dbname="dev",
    user="awsuser", password=os.environ["PG_PASS"], connect_timeout=10,
)
my = pymysql.connect(
    host=os.environ["MYSQL_HOST"], port=3306, database="dev",
    user="awsuser", password=os.environ["MYSQL_PASS"],
    connect_timeout=10, local_infile=True,
)

pg_engine = arrowjet.Engine(provider="postgresql")
mysql_engine = arrowjet.Engine(provider="mysql")

# Setup source in PG
cursor = pg.cursor()
cursor.execute("DROP TABLE IF EXISTS verify_src")
cursor.execute("CREATE TABLE verify_src (id BIGINT, name VARCHAR(50), score DOUBLE PRECISION)")
pg.commit()

df = pd.DataFrame({
    "id": range(1000),
    "name": [f"user_{i}" for i in range(1000)],
    "score": [i * 1.5 for i in range(1000)],
})
pg_engine.write_dataframe(pg, df, "verify_src")
pg.commit()
print("Source: 1000 rows in PG")

# Setup dest in MySQL
my_cursor = my.cursor()
my_cursor.execute("DROP TABLE IF EXISTS verify_dst")
my_cursor.execute("CREATE TABLE verify_dst (id BIGINT, name VARCHAR(50), score DOUBLE)")
my.commit()

# Transfer PG -> MySQL
result = arrowjet.transfer(
    source_engine=pg_engine, source_conn=pg,
    query="SELECT * FROM verify_src",
    dest_engine=mysql_engine, dest_conn=my,
    dest_table="verify_dst",
)
print(f"Transfer: {result.rows} rows in {result.total_time_s}s")

# Verify row count
my_cursor.execute("SELECT COUNT(*) FROM verify_dst")
count = my_cursor.fetchone()[0]
print(f"Row count: {count}")
assert count == 1000, f"FAIL: expected 1000, got {count}"

# Verify first rows
my_cursor.execute("SELECT id, name, score FROM verify_dst ORDER BY id LIMIT 5")
rows = my_cursor.fetchall()
print(f"First 5: {rows}")
assert rows[0] == (0, "user_0", 0.0), f"FAIL row 0: {rows[0]}"
assert rows[4] == (4, "user_4", 6.0), f"FAIL row 4: {rows[4]}"

# Verify last row
my_cursor.execute("SELECT id, name, score FROM verify_dst WHERE id = 999")
last = my_cursor.fetchone()
print(f"Last row: {last}")
assert last == (999, "user_999", 1498.5), f"FAIL last: {last}"

# Verify aggregate
my_cursor.execute("SELECT SUM(score) FROM verify_dst")
total = my_cursor.fetchone()[0]
print(f"SUM(score): {total}")
assert abs(total - 749250.0) < 0.01, f"FAIL sum: {total}"

# Cleanup
cursor.execute("DROP TABLE IF EXISTS verify_src")
pg.commit()
my_cursor.execute("DROP TABLE IF EXISTS verify_dst")
my.commit()
pg.close()
my.close()

print("\nALL VERIFICATIONS PASSED")
