"""
Base classes for SMDT readers.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Iterable, Mapping, Any, Optional, BinaryIO


class ReaderError(Exception):
    """Base class for reader errors."""

    ...


class MissingOptionalDependency(ReaderError):
    """Raised when an optional dependency is missing."""

    ...


class Reader(ABC):
    """Runtime-enforced interface with sensible defaults."""

    name: str

    @abstractmethod
    def supports(self, uri: str, *, content_type: Optional[str] = None) -> bool:
        """Check if the reader supports the given URI.

        Args:
            uri: URI to check.
            content_type: Optional content type hint.

        Returns:
            True if supported, False otherwise.
        """
        ...

    @abstractmethod
    def stream(self, uri: str, **kwargs) -> Iterable[Mapping[str, Any]]:
        """Stream records from the given URI.

        Args:
            uri: URI to read from.
            **kwargs: Additional arguments for the reader.

        Yields:
            Dictionary representing a record.
        """
        ...

    def stream_from_filelike(
        self, f: BinaryIO, **kwargs
    ) -> Iterable[Mapping[str, Any]]:
        """Stream records from a file-like object.

        Optional hook for file-like input (e.g., zip members).
        Default: not implemented; callers should fall back to a temp-file strategy.

        Args:
            f: File-like object.
            **kwargs: Additional arguments.

        Yields:
            Dictionary representing a record.

        Raises:
            NotImplementedError: If not implemented by the subclass.
        """
        raise NotImplementedError(
            "stream_from_filelike() not implemented for this reader"
        )
