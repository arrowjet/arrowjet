"""
Airflow DAG: Export Redshift data to S3  - WITH Arrowjet CLI.

Zero Python. Just a bash command. Requires `arrowjet configure` to be
run once on the Airflow worker (or env vars set).

3 lines of task code.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG("export_with_arrowjet_cli", start_date=datetime(2025, 1, 1), schedule="@daily"):

    export_sales = BashOperator(
        task_id="export_sales",
        bash_command=(
            'arrowjet export '
            '--query "SELECT * FROM sales WHERE date > CURRENT_DATE - 7" '
            '--to s3://my-output-bucket/sales/latest.parquet '
            '--format parquet'
        ),
    )
