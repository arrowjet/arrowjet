"""
Airflow DAG: Export Redshift data  - NAIVE approach (no UNLOAD, no arrowjet).

This is what most people start with: cursor.execute() + fetchall().
Simple, but slow for large datasets  - every row travels over the wire.

Compare timing against without_arrowjet.py (manual UNLOAD) and
with_arrowjet.py (arrowjet) to see the difference.
"""

import os
import time
from datetime import datetime

from airflow import DAG
from airflow.decorators import task


with DAG("export_naive_fetch", start_date=datetime(2025, 1, 1), schedule=None):

    @task
    def export_sample():
        import redshift_connector
        import pyarrow as pa
        import pyarrow.parquet as pq

        conn = redshift_connector.connect(
            host=os.environ["REDSHIFT_HOST"],
            port=int(os.environ.get("REDSHIFT_PORT", "5439")),
            database=os.environ.get("REDSHIFT_DATABASE", "dev"),
            user=os.environ.get("REDSHIFT_USER", "awsuser"),
            password=os.environ["REDSHIFT_PASS"],
        )

        start = time.perf_counter()

        cursor = conn.cursor()
        cursor.execute("SELECT * FROM benchmark_test_1m")
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        fetch_time = time.perf_counter() - start

        # Convert to Arrow and save
        table = pa.table({col: [row[i] for row in rows] for i, col in enumerate(columns)})
        out_path = "/tmp/airflow_naive_fetch.parquet"
        pq.write_table(table, out_path)

        total_time = time.perf_counter() - start

        print(f"Naive fetch: {len(rows):,} rows in {total_time:.2f}s (fetch={fetch_time:.2f}s)")
        conn.close()
        return out_path

    export_sample()
