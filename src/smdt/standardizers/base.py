from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, Mapping, Any, Optional, Dict, Type, Protocol


class DBModelLike(Protocol):
    def insert_columns(self, include_id: bool = False): ...
    def insert_values(self, include_id: bool = False): ...


@dataclass(frozen=True)
class SourceInfo:
    path: str
    member: Optional[str] = None
    hints: Optional[Dict[str, Any]] = None


class Standardizer:
    """
    A Standardizer turns raw records into one or more DB model objects.
    """

    name: str

    def standardize(self, input_record) -> Iterable[DBModelLike]:
        """Convert a raw record into 0..N DB model instances."""
        raise NotImplementedError
