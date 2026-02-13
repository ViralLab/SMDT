"""Inspector module for analyzing database schema and data statistics."""

from .inspector import Inspector, report_schemas

__all__ = ["Inspector", "report_schemas"]
