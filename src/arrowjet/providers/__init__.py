"""
Arrowjet provider system — database-specific bulk operation implementations.

Each provider implements bulk operations for a specific database.
The core engine is provider-agnostic.

Supported providers:
  - RedshiftProvider: Amazon Redshift (UNLOAD/COPY via S3)
  - PostgreSQLProvider: PostgreSQL / Aurora PostgreSQL / RDS PostgreSQL (COPY protocol)

Planned providers:
  - MySQLProvider: MySQL / Aurora MySQL (LOAD DATA)
  - SnowflakeProvider: Snowflake (COPY INTO via S3/GCS/Azure)
  - BigQueryProvider: BigQuery (Storage Read API / GCS export)
  - DatabricksProvider: Databricks (COPY INTO via cloud storage)
"""

from .base import BulkProvider, ExportCommand, ImportCommand
from .redshift import RedshiftProvider
from .postgresql import PostgreSQLProvider

__all__ = [
    "BulkProvider", "ExportCommand", "ImportCommand",
    "RedshiftProvider", "PostgreSQLProvider",
]
