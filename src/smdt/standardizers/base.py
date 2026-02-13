from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, Mapping, Any, Optional, Dict, Tuple, Type, Protocol


class DBModelLike(Protocol):
    """Protocol for database model objects.

    Models must provide methods to get column names and values for insertion.
    """

    def insert_columns(self, include_id: bool = False):
        """
        Get string column names (e.g. for SQL INSERT).

        Args:
            include_id (bool): Whether to include the primary key column.
        """
        ...

    def insert_values(self, include_id: bool = False):
        """
        Get values corresponding to columns (e.g. for SQL INSERT).

        Args:
            include_id (bool): Whether to include the primary key value.
        """
        ...


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

    def standardize(
        self, input_record: Tuple[dict, SourceInfo]
    ) -> Iterable[DBModelLike]:
        """Convert a raw record into 0..N DB model instances.

        Args:
            input_record: A tuple (raw_data_dict, SourceInfo).

        Returns:
            Iterable[DBModelLike]: Standardized schema models (Accounts, Posts, etc.).

        Raises:
            NotImplementedError: Must be implemented by subclasses.
        """
        raise NotImplementedError
