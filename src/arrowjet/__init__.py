"""
Arrowjet — High-Performance Bulk Data Movement for Redshift.

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

__version__ = "0.4.0"

from .connection import connect, ArrowjetConnection, ArrowjetError
from .engine import Engine, PostgreSQLEngine
from .transfer import transfer, TransferResult

__all__ = [
    "connect", "ArrowjetConnection", "ArrowjetError",
    "Engine", "PostgreSQLEngine",
    "transfer", "TransferResult",
    "__version__",
]
