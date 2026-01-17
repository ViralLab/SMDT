from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, Mapping, Any, Optional, Dict, Type, Protocol


class DBModelLike(Protocol):
    """Protocol for database model objects.

    Models must provide methods to get column names and values for insertion.
    """

    def insert_columns(self, include_id: bool = False): ...
    def insert_values(self, include_id: bool = False): ...


@dataclass(frozen=True)
class SourceInfo:
    """Information about the source of a record.

    Attributes:
        path: File path or URI.
        member: Optional archive member name.
        hints: Optional metadata hints.
    """

    path: str
    member: Optional[str] = None
    hints: Optional[Dict[str, Any]] = None


class Standardizer:
    """A Standardizer turns raw records into one or more DB model objects."""

    name: str

    def standardize(self, input_record) -> Iterable[DBModelLike]:
        """Convert a raw record into 0..N DB model instances.

        Args:
            input_record: Raw input record.

        Returns:
            Iterable of DB model instances.

        Raises:
            NotImplementedError: Must be implemented by subclasses.
        """
        raise NotImplementedError
