from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Mapping, Any, Tuple, List


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class HashMap:
    """
    Python model for `hash_map` table.
    Generic key-value store.
    """

    # Required fields
    hash_key: str = field()
    created_at: datetime = field()

    # Optional
    id: Optional[int] = None
    hash_value: Optional[str] = None

    # Metadata for StandardDB fallback
    __table_name__ = "hash_map"
    __jsonb_fields__ = set()

    # -------- Validation / normalization --------
    def __post_init__(self):
        """
        Validates the dataclass fields.

        Raises:
            ValueError: If hash_key is empty.
        """
        # hash_key required
        if not self.hash_key or not self.hash_key.strip():
            raise ValueError("hash_key is required and cannot be empty")
        object.__setattr__(self, "hash_key", self.hash_key.strip())

        # created_at must be tz-aware (default UTC)
        ca = self.created_at
        if ca.tzinfo is None or ca.tzinfo.utcoffset(ca) is None:
            object.__setattr__(self, "created_at", ca.replace(tzinfo=timezone.utc))

        # normalize empty strings to None
        if isinstance(self.hash_value, str) and self.hash_value.strip() == "":
            object.__setattr__(self, "hash_value", None)

    # -------- DB helpers --------
    @staticmethod
    def insert_columns(include_id: bool = False) -> Tuple[str, ...]:
        """
        Ordered column list for INSERT statements.
        """
        cols = ("hash_key", "hash_value", "created_at")
        return ("id",) + cols if include_id else cols

    def insert_values(self, include_id: bool = False) -> Tuple[Any, ...]:
        """
        Values tuple aligned with insert_columns().
        """
        vals = (
            self.hash_key,
            self.hash_value,
            self.created_at,
        )
        return ((self.id,) + vals) if include_id else vals

    @classmethod
    def from_db_row(cls, row: Mapping[str, Any]) -> "HashMap":
        """
        Hydrate from a dict-like DB row.
        """
        return cls(
            hash_key=row["hash_key"],  # NOT NULL
            created_at=row["created_at"],  # NOT NULL
            id=row.get("id"),
            hash_value=row.get("hash_value"),
        )

    @staticmethod
    def make_bulk_params(
        items: List["HashMap"], include_id: bool = False
    ) -> List[Tuple[Any, ...]]:
        """
        Convert a list of HashMap items to INSERT parameter tuples.
        """
        return [h.insert_values(include_id=include_id) for h in items]
