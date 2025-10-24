from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Mapping, Any, Tuple, List, Dict


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class PostEnrichments:
    model_id: str = field()
    post_id: str = field()
    body: Dict[str, Any] = field()
    created_at: datetime = field()

    id: Optional[int] = None
    retrieved_at: Optional[datetime] = None

    # Metadata for StandardDB fallback
    __table_name__ = "post_enrichments"
    __jsonb_fields__ = {"body"}

    # -------- Validation / normalization --------
    def __post_init__(self):
        # model_id required
        if not self.model_id or not self.model_id.strip():
            raise ValueError("model_id is required and cannot be empty")
        object.__setattr__(self, "model_id", self.model_id.strip())

        # post_id required
        if not self.post_id or not self.post_id.strip():
            raise ValueError("post_id is required and cannot be empty")
        object.__setattr__(self, "post_id", self.post_id.strip())

        # body must be dict
        if not isinstance(self.body, dict):
            raise ValueError("body must be a dict for JSONB column")

        # created_at must be tz-aware (UTC default)
        ca = self.created_at
        if ca.tzinfo is None or ca.tzinfo.utcoffset(ca) is None:
            object.__setattr__(self, "created_at", ca.replace(tzinfo=timezone.utc))

    # -------- DB helpers --------
    @staticmethod
    def insert_columns(include_id: bool = False) -> Tuple[str, ...]:
        """
        Ordered column list for INSERT statements.
        """
        cols = (
            "model_id",
            "post_id",
            "body",
            "created_at",
            "retrieved_at",
        )
        return ("id",) + cols if include_id else cols

    def insert_values(self, include_id: bool = False) -> Tuple[Any, ...]:
        """
        Values tuple aligned with insert_columns().
        """
        vals = (
            self.model_id,
            self.post_id,
            self.body,
            self.created_at,
            self.retrieved_at,
        )
        return ((self.id,) + vals) if include_id else vals

    @classmethod
    def from_db_row(cls, row: Mapping[str, Any]) -> "PostEnrichments":
        """
        Hydrate a PostEnrichments instance from a dict-like DB row.
        """
        return cls(
            model_id=row["model_id"],  # NOT NULL
            post_id=row["post_id"],  # NOT NULL
            body=row["body"],  # NOT NULL (psycopg JSONB→dict)
            created_at=row["created_at"],  # NOT NULL
            id=row.get("id"),
            retrieved_at=row.get("retrieved_at"),
        )

    @staticmethod
    def make_bulk_params(
        items: List["PostEnrichments"], include_id: bool = False
    ) -> List[Tuple[Any, ...]]:
        """
        Convert a list of PostEnrichments into INSERT parameter tuples.
        """
        return [e.insert_values(include_id=include_id) for e in items]
