"""
Testable version of the Arrowjet Airflow example — two tasks in a DAG.

Task 1: Extract from Redshift (bulk + safe mode)
Task 2: Verify the exported file

Tasks pass data via file path (tasks run in separate processes).

Run with:
  set -a && source .env && set +a
  AIRFLOW_HOME=$(pwd)/airflow_test PYTHONPATH=src .venv/bin/python airflow_test/dags/test_with_arrowjet.py
"""

import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import task


with DAG("test_arrowjet_export", start_date=datetime(2025, 1, 1), schedule=None):

    @task
    def extract():
        """Task 1: Bulk read from Redshift, save to local Parquet."""
        import arrowjet
        import pyarrow.parquet as pq

        conn = arrowjet.connect(
            host=os.environ["REDSHIFT_HOST"],
            database=os.environ.get("REDSHIFT_DATABASE", "dev"),
            user=os.environ.get("REDSHIFT_USER", "awsuser"),
            password=os.environ["REDSHIFT_PASS"],
            staging_bucket=os.environ["STAGING_BUCKET"],
            staging_iam_role=os.environ["STAGING_IAM_ROLE"],
            staging_region=os.environ.get("STAGING_REGION", "us-east-1"),
        )

        result = conn.read_bulk("SELECT * FROM benchmark_test_1m LIMIT 1000")
        print(f"Bulk read: {result.rows:,} rows in {result.total_time_s}s")

        out_path = "/tmp/airflow_arrowjet_test.parquet"
        pq.write_table(result.table, out_path)
        print(f"Saved to {out_path}")

        conn.close()
        return out_path  # passed to next task

    @task
    def verify(parquet_path: str):
        """Task 2: Read the exported file and verify it."""
        import pyarrow.parquet as pq

        table = pq.read_table(parquet_path)
        print(f"Verified: {table.num_rows:,} rows, {table.num_columns} columns")
        assert table.num_rows == 1000, f"Expected 1000 rows, got {table.num_rows}"
        print("All good.")

    # Task 1 → Task 2 (extract passes file path to verify)
    verify(extract())
