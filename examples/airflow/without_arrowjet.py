"""
Airflow DAG: Export Redshift data to S3 — WITHOUT Arrowjet.

This is what most teams write today: manual UNLOAD, S3 path management,
cleanup, error handling, and IAM configuration — all inline.

~50 lines of boilerplate for a simple export.
"""

import os
import uuid
from datetime import datetime

from airflow import DAG
from airflow.decorators import task

import boto3
import redshift_connector


with DAG("export_without_arrowjet", start_date=datetime(2025, 1, 1), schedule="@daily"):

    @task
    def export_sample():
        host = os.environ["REDSHIFT_HOST"]
        database = os.environ.get("REDSHIFT_DATABASE", "dev")
        user = os.environ.get("REDSHIFT_USER", "awsuser")
        password = os.environ["REDSHIFT_PASS"]
        bucket = os.environ["STAGING_BUCKET"]
        iam_role = os.environ["STAGING_IAM_ROLE"]
        region = os.environ.get("STAGING_REGION", "us-east-1")

        # Generate unique S3 path to avoid collisions
        op_id = uuid.uuid4().hex[:12]
        s3_prefix = f"exports/sales/{op_id}/"
        s3_uri = f"s3://{bucket}/{s3_prefix}"

        conn = redshift_connector.connect(
            host=host, port=5439,
            database=database, user=user, password=password,
        )

        try:
            # Execute UNLOAD — note: LIMIT requires subquery workaround
            cursor = conn.cursor()
            cursor.execute(f"""
                UNLOAD ($$SELECT * FROM (SELECT * FROM benchmark_test_1m LIMIT 1000)$$)
                TO '{s3_uri}'
                IAM_ROLE '{iam_role}'
                FORMAT PARQUET
                ALLOWOVERWRITE
                PARALLEL ON
            """)

            print(f"UNLOAD complete: {s3_uri}")

        except Exception as e:
            # Cleanup on failure
            print(f"UNLOAD failed: {e}")
            _cleanup_s3(bucket, s3_prefix, region)
            raise

        finally:
            conn.close()

    def _cleanup_s3(bucket, prefix, region):
        """Delete staged files — must handle manually."""
        s3 = boto3.client("s3", region_name=region)
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            objects = page.get("Contents", [])
            if objects:
                s3.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": [{"Key": obj["Key"]} for obj in objects]},
                )

    export_sample()
