"""
Arrowjet Quickstart — run this as a script or in Jupyter.

Demonstrates:
  1. Safe mode (standard queries)
  2. Bulk read (UNLOAD — fast for large results)
  3. Bulk write (COPY — fast for loading data)
  4. Preview and validate

Prerequisites:
  pip install arrowjet
  arrowjet configure  # or set env vars
"""

# %% [markdown]
# # Arrowjet Quickstart

# %% Connect
import arrowjet

conn = arrowjet.connect(
    host="my-cluster.region.redshift.amazonaws.com",
    database="dev",
    user="awsuser",
    password="mypassword",
    staging_bucket="my-staging-bucket",
    staging_iam_role="arn:aws:iam::123456789:role/RedshiftS3Role",
    staging_region="us-east-1",
)

print(conn)  # ArrowjetConnection(host=..., mode=safe+bulk)

# %% Safe mode — standard queries
df = conn.fetch_dataframe("SELECT * FROM users LIMIT 10")
print(df)

# %% Bulk read — fast for large results
result = conn.read_bulk("SELECT * FROM events WHERE date > '2025-01-01'")
print(f"Read {result.rows:,} rows in {result.total_time_s}s")

# Convert to pandas
df_events = result.to_pandas()
print(df_events.head())

# %% Bulk write — fast for loading data
import pyarrow as pa
import numpy as np

# Create sample data
table = pa.table({
    "id": pa.array(np.arange(10000), type=pa.int64()),
    "value": pa.array(np.random.random(10000), type=pa.float64()),
    "name": pa.array([f"item_{i}" for i in range(10000)], type=pa.string()),
})

# Write to Redshift (Arrow → Parquet → S3 → COPY)
result = conn.write_bulk(table, "my_target_table")
print(f"Wrote {result.rows:,} rows in {result.total_time_s}s")

# %% Validate
tables = conn.get_tables()
print(f"Tables: {tables[:5]}")

cols = conn.get_columns("my_target_table")
for c in cols:
    print(f"  {c['name']}: {c['type']}")

# %% Metrics
metrics = conn.get_metrics()
print(metrics)

# %% Cleanup
conn.close()
