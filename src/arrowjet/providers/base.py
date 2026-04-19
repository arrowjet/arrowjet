"""
BulkProvider — abstract interface for database-specific bulk operations.

Each database has its own way to export and import large datasets:
  - Redshift: UNLOAD (export) + COPY (import) via S3
  - Snowflake: COPY INTO <location> (export) + COPY INTO <table> (import) via S3/GCS/Azure
  - BigQuery: EXPORT DATA (export) + LOAD DATA (import) via GCS
  - Databricks: COPY INTO (export/import) via cloud storage

The BulkProvider interface abstracts these differences so the core engine
(BulkReader, BulkWriter) can work with any database without if-elses.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ExportCommand:
    """
    Result of build_export_sql() — the SQL to run on the database
    to export query results to cloud storage.
    """
    sql: str
    staging_path: str
    format: str  # "parquet", "csv", etc.


@dataclass(frozen=True)
class ImportCommand:
    """
    Result of build_import_sql() — the SQL to run on the database
    to import data from cloud storage into a table.
    """
    sql: str
    staging_path: str
    format: str


class BulkProvider(ABC):
    """
    Abstract interface for database-specific bulk operations.

    Implement this to add support for a new database.
    The core engine calls these methods — no database-specific
    logic lives in BulkReader or BulkWriter.
    """

    @abstractmethod
    def build_export_sql(
        self,
        query: str,
        staging_path: str,
        parallel: bool = True,
        compression: Optional[str] = None,
    ) -> ExportCommand:
        """
        Build the SQL command to export query results to cloud storage.

        Args:
            query: SELECT query whose results to export
            staging_path: Cloud storage path (s3://, gs://, abfss://)
            parallel: Enable parallel export across nodes/slices
            compression: Optional compression hint

        Returns:
            ExportCommand with the SQL to execute
        """

    @abstractmethod
    def build_import_sql(
        self,
        target_table: str,
        staging_path: str,
        compression: Optional[str] = None,
    ) -> ImportCommand:
        """
        Build the SQL command to import data from cloud storage into a table.

        Args:
            target_table: Fully qualified target table name
            staging_path: Cloud storage path containing the data
            compression: Optional compression hint

        Returns:
            ImportCommand with the SQL to execute
        """

    @property
    @abstractmethod
    def staging_format(self) -> str:
        """
        Preferred staging format for this database.
        e.g. "parquet", "csv"
        """

    @property
    @abstractmethod
    def staging_backend(self) -> str:
        """
        Required cloud storage backend.
        e.g. "s3", "gcs", "azure_blob"
        """

    @property
    def name(self) -> str:
        """Human-readable provider name."""
        return self.__class__.__name__
