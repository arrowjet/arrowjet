"""
S3 upload engine — handles staging file uploads with retry and encryption.

Uses temp files + boto3 upload_file (transfer manager) for best performance,
as validated in M0 benchmarks.
"""

from __future__ import annotations

import os
import logging
import tempfile
from typing import Optional

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config as BotoConfig

from .config import StagingConfig
from .namespace import OperationPath

logger = logging.getLogger(__name__)

# Retry config for S3 operations
_S3_RETRY_CONFIG = BotoConfig(
    retries={"max_attempts": 3, "mode": "adaptive"}
)


class S3Uploader:
    """Uploads Arrow tables to S3 as Parquet files."""

    def __init__(self, config: StagingConfig):
        self._config = config
        self._client = boto3.client(
            "s3",
            config=_S3_RETRY_CONFIG,
            **config.s3_client_kwargs(),
        )

    def upload_parquet(
        self,
        table: pa.Table,
        path: OperationPath,
        filename: str = "data.parquet",
        compression: str = "snappy",
    ) -> UploadResult:
        """
        Write an Arrow table as Parquet and upload to S3.

        Uses temp file + upload_file for best performance (validated in M0).
        """
        s3_key = path.file_key(filename)
        tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)

        try:
            # Write Parquet to temp file
            pq.write_table(table, tmp.name, compression=compression)
            file_size = os.path.getsize(tmp.name)

            # Check size limit
            if file_size > self._config.max_staging_bytes:
                raise StagingUploadError(
                    f"Parquet file size ({file_size} bytes) exceeds "
                    f"max_staging_bytes ({self._config.max_staging_bytes})"
                )

            # Upload with encryption headers
            extra_args = self._config.encryption_headers()
            self._client.upload_file(
                tmp.name,
                self._config.bucket,
                s3_key,
                ExtraArgs=extra_args if extra_args else None,
            )

            logger.info(
                "Uploaded %s (%d bytes, %s) to s3://%s/%s",
                filename, file_size, compression,
                self._config.bucket, s3_key,
            )

            return UploadResult(
                key=s3_key,
                size_bytes=file_size,
                compression=compression,
            )

        except Exception as e:
            # Attempt to abort/clean up on failure
            try:
                self._client.delete_object(
                    Bucket=self._config.bucket, Key=s3_key
                )
            except Exception:
                logger.warning("Failed to clean up partial upload: %s", s3_key)
            if isinstance(e, StagingUploadError):
                raise
            raise StagingUploadError(f"Upload failed for {s3_key}: {e}") from e

        finally:
            # Always clean up temp file
            try:
                os.unlink(tmp.name)
            except OSError:
                pass

    def upload_bytes(
        self,
        data: bytes,
        path: OperationPath,
        filename: str,
    ) -> UploadResult:
        """Upload raw bytes to S3. Used for manifests or small files."""
        s3_key = path.file_key(filename)
        extra_args = self._config.encryption_headers()

        self._client.put_object(
            Bucket=self._config.bucket,
            Key=s3_key,
            Body=data,
            **(extra_args if extra_args else {}),
        )

        return UploadResult(key=s3_key, size_bytes=len(data))


class UploadResult:
    __slots__ = ("key", "size_bytes", "compression")

    def __init__(self, key: str, size_bytes: int, compression: str = ""):
        self.key = key
        self.size_bytes = size_bytes
        self.compression = compression


class StagingUploadError(Exception):
    pass
