"""
IO module for SMDT.

Exports discover and read functions from readers.
"""

from .readers import discover, read

__all__ = ["discover", "read"]
