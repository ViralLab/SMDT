"""
Readers package for SMDT.

Exports the registry functions and common processing utilities.
"""

from .registry import discover, read, get_reader

__all__ = ["discover", "read", "get_reader"]
