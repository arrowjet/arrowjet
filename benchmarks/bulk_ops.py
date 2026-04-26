"""
Prototype bulk read/write operations via S3 staging.
This is throwaway code  - just enough to prove the concept and benchmark it.

v2: Uses pyarrow.fs.S3FileSystem for fast parallel Parquet reads,
    and pyarrow.parquet.ParquetDataset for reading UNLOAD output.
"""

import io
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs
import redshift_connector


def _s3_client(region: str):
    return boto3.client("s3", region_name=region)


def _generate_s3_path(bucket: str, prefix: str) -> tuple[str, str]:
    """Generate a unique S3 path for staging. Returns (s3_uri, key_prefix)."""
    op_id = uuid.uuid4().hex[:12]
    key_prefix = f"{prefix}/{op_id}/"
    s3_uri = f"s3://{bucket}/{key_prefix}"
    return s3_uri, key_prefix


def _cleanup_s3(bucket: str, key_prefix: str, region: str):
    """Delete all objects under a prefix."""
    s3 = _s3_client(region)
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
        objects = page.get("Contents", [])
        if objects:
            s3.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": obj["Key"]} for obj in objects]},
            )


def read_bulk(
    conn: redshift_connector.Connection,
    query: str,
    bucket: str,
    prefix: str,
    iam_role: str,
    region: str,
) -> pa.Table:
    """
    Bulk read via UNLOAD -> S3 -> Parquet -> Arrow.
    Uses PyArrow's S3FileSystem for fast parallel reads.
    """
    s3_uri, key_prefix = _generate_s3_path(bucket, prefix)
    cursor = conn.cursor()

    # Execute UNLOAD
    unload_sql = (
        f"UNLOAD ($${query}$$) "
        f"TO '{s3_uri}' "
        f"IAM_ROLE '{iam_role}' "
        f"FORMAT PARQUET "
        f"ALLOWOVERWRITE "
        f"PARALLEL ON"
    )
    cursor.execute(unload_sql)

    # Read Parquet files directly from S3 using PyArrow's native S3 filesystem
    # This is MUCH faster than boto3 get_object  - it reads in parallel and
    # avoids copying data through Python
    s3fs = pafs.S3FileSystem(region=region)
    dataset_path = f"{bucket}/{key_prefix}"

    try:
        dataset = pq.ParquetDataset(dataset_path, filesystem=s3fs)
        table = dataset.read()
    finally:
        # Cleanup
        _cleanup_s3(bucket, key_prefix, region)

    return table


def write_bulk(
    conn: redshift_connector.Connection,
    table: pa.Table,
    target_table: str,
    bucket: str,
    prefix: str,
    iam_role: str,
    region: str,
):
    """
    Bulk write via Arrow -> Parquet -> S3 -> COPY.
    Uses multipart upload for large files.
    """
    s3_uri, key_prefix = _generate_s3_path(bucket, prefix)
    s3 = _s3_client(region)

    # Write Arrow table as Parquet to S3
    # For large tables, write in row groups for better parallelism on COPY side
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    parquet_key = f"{key_prefix}data.parquet"
    s3.upload_fileobj(buf, bucket, parquet_key)

    # Execute COPY
    cursor = conn.cursor()
    copy_sql = (
        f"COPY {target_table} "
        f"FROM '{s3_uri}' "
        f"IAM_ROLE '{iam_role}' "
        f"FORMAT PARQUET"
    )
    cursor.execute(copy_sql)
    conn.commit()

    # Cleanup
    _cleanup_s3(bucket, key_prefix, region)
