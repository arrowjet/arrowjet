"""
Arrowjet SQLAlchemy dialect for Redshift.

Safe mode only  - no UNLOAD, no COPY, no auto-routing through SQLAlchemy.
Bulk performance is accessed through the explicit Arrowjet API, not through SQLAlchemy.

Usage:
    from sqlalchemy import create_engine
    engine = create_engine("redshift+arrowjet://awsuser:pass@host:5439/dev")
"""

from .dialect import RedshiftArrowjetDialect

__all__ = ["RedshiftArrowjetDialect"]
