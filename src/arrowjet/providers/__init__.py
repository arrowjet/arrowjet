"""
Arrowjet provider system — database-specific bulk operation implementations.

Each provider implements the BulkProvider interface for a specific database.
The core engine (BulkReader, BulkWriter) is provider-agnostic.

Supported providers:
  - RedshiftProvider: Amazon Redshift (UNLOAD/COPY via S3)

Planned providers:
  - SnowflakeProvider: Snowflake (COPY INTO via S3/GCS/Azure)
  - BigQueryProvider: BigQuery (Storage Read API / GCS export)
  - DatabricksProvider: Databricks (COPY INTO via cloud storage)
"""

from .base import BulkProvider, ExportCommand, ImportCommand
from .redshift import RedshiftProvider

__all__ = ["BulkProvider", "ExportCommand", "ImportCommand", "RedshiftProvider"]
