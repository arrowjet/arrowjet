"""
S3 download engine  - reads staged Parquet files into Arrow tables.

Uses PyArrow's native S3FileSystem for best performance (5x faster than
boto3 sequential download, validated in M0 diagnostic).
"""

from __future__ import annotations

import logging
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs
import boto3
from botocore.config import Config as BotoConfig

from .config import StagingConfig
from .namespace import OperationPath

logger = logging.getLogger(__name__)

_S3_RETRY_CONFIG = BotoConfig(
    retries={"max_attempts": 3, "mode": "adaptive"}
)


class S3Downloader:
    """Downloads staged Parquet files from S3 into Arrow tables."""

    def __init__(self, config: StagingConfig):
        self._config = config
        self._client = boto3.client(
            "s3",
            config=_S3_RETRY_CONFIG,
            **config.s3_client_kwargs(),
        )
        self._s3fs = pafs.S3FileSystem(
            region=config.region,
            endpoint_override=config.s3_endpoint_url or "",
        )

    def list_parquet_files(self, path: OperationPath) -> list[StagedFile]:
        """List all Parquet files under an operation's S3 prefix."""
        files = []
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(
            Bucket=self._config.bucket, Prefix=path.key_prefix
        ):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    files.append(StagedFile(
                        key=obj["Key"],
                        size_bytes=obj["Size"],
                    ))

        logger.debug(
            "Listed %d parquet files under %s (%d bytes total)",
            len(files), path.key_prefix,
            sum(f.size_bytes for f in files),
        )
        return files

    def read_parquet(self, path: OperationPath) -> pa.Table:
        """
        Read all Parquet files under an operation prefix into a single Arrow table.

        Uses PyArrow's S3FileSystem + ParquetDataset for parallel, zero-copy reads.
        """
        dataset_path = f"{self._config.bucket}/{path.key_prefix}"

        try:
            dataset = pq.ParquetDataset(dataset_path, filesystem=self._s3fs)
            table = dataset.read()
        except Exception as e:
            raise StagingDownloadError(
                f"Failed to read Parquet from s3://{dataset_path}: {e}"
            ) from e

        logger.info(
            "Read %d rows (%d bytes) from %s",
            table.num_rows, table.nbytes, dataset_path,
        )
        return table


class StagedFile:
    __slots__ = ("key", "size_bytes")

    def __init__(self, key: str, size_bytes: int):
        self.key = key
        self.size_bytes = size_bytes


class StagingDownloadError(Exception):
    pass
