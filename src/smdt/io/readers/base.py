from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Iterable, Mapping, Any, Optional, BinaryIO


class ReaderError(Exception): ...


class MissingOptionalDependency(ReaderError): ...


class Reader(ABC):
    """Runtime-enforced interface with sensible defaults."""

    name: str

    @abstractmethod
    def supports(self, uri: str, *, content_type: Optional[str] = None) -> bool: ...

    @abstractmethod
    def stream(self, uri: str, **kwargs) -> Iterable[Mapping[str, Any]]: ...

    def stream_from_filelike(
        self, f: BinaryIO, **kwargs
    ) -> Iterable[Mapping[str, Any]]:
        """
        Optional hook for file-like input (e.g., zip members).
        Default: not implemented; callers should fall back to a temp-file strategy.
        """
        raise NotImplementedError(
            "stream_from_filelike() not implemented for this reader"
        )
