"""
Arrowjet  - High-Performance Bulk Data Movement for Redshift.

Usage:
    import arrowjet

    # Path 1: Unified connection (greenfield)
    conn = arrowjet.connect(
        host="...", database="dev", user="awsuser", password="...",
        staging_bucket="my-bucket",
        staging_iam_role="arn:aws:iam::123:role/RedshiftS3",
        staging_region="us-east-1",
    )
    df = conn.fetch_dataframe("SELECT * FROM users")
    result = conn.read_bulk("SELECT * FROM events")

    # Path 2: Bring your own connection (existing codebases)
    engine = arrowjet.Engine(
        staging_bucket="my-bucket",
        staging_iam_role="arn:aws:iam::123:role/RedshiftS3",
        staging_region="us-east-1",
    )
    result = engine.read_bulk(existing_conn, "SELECT * FROM events")
    engine.write_bulk(existing_conn, arrow_table, "target_table")
"""

__version__ = "0.6.0"

from .connection import connect, ArrowjetConnection, ArrowjetError, S3Error, DataError, TransientError
from .hardening import ConnectionLostError
from .engine import Engine, PostgreSQLEngine
from .transfer import transfer, TransferResult

__all__ = [
    "connect", "ArrowjetConnection", "ArrowjetError",
    "S3Error", "DataError", "TransientError", "ConnectionLostError",
    "Engine", "PostgreSQLEngine",
    "transfer", "TransferResult",
    "__version__",
]

# Plugin auto-discovery: if extension packages are installed,
# they register hooks, providers, or CLI commands on import.
# Core is fully functional without any extensions.
try:
    import arrowjet_pro  # noqa: F401
except ImportError:
    pass
