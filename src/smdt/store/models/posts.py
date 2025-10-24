from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Mapping, Any, Tuple, List, Union


def _normalize_point(
    val: Optional[Union[str, Tuple[float, float], List[float]]],
) -> Optional[str]:
    """
    Accepts:
      - "(lon,lat)" string
      - (lon, lat) tuple/list
      - None
    Returns:
      - Canonical postgres 'point' literal: "(lon,lat)" or None
    """
    if val is None:
        return None
    if isinstance(val, str):
        s = val.strip()
        return s or None
    if isinstance(val, (tuple, list)) and len(val) == 2:
        lon, lat = val
        try:
            return f"({float(lon)},{float(lat)})"
        except (TypeError, ValueError):
            return None
    return None


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class Posts:
    """
    Python model for `posts` table.

    Notes:
      - `location` column is Postgres `point` → pass as "(lon,lat)".
        This class will also accept (lon, lat) and normalize it.
      - `post_id` is NOT NULL in SQL; this class allows Optional for flexibility,
        but you should provide it or you’ll hit a DB constraint error.
    """

    created_at: datetime = field()
    account_id: str = field()

    id: Optional[int] = None
    post_id: Optional[str] = None
    conversation_id: Optional[str] = None
    body: Optional[str] = None
    engagement_count: Optional[int] = None
    location: Optional[Union[str, Tuple[float, float], List[float]]] = None
    retrieved_at: Optional[datetime] = None

    __table_name__: str = "posts"
    __jsonb_fields__ = set()  # no JSONB columns here

    # -------- Validation / normalization --------
    def __post_init__(self):
        # account_id required, non-empty
        if not isinstance(self.account_id, str) or not self.account_id.strip():
            raise ValueError("account_id is required and cannot be empty")
        object.__setattr__(self, "account_id", self.account_id.strip())

        if self.created_at is None:
            raise ValueError("created_at is required and cannot be None")

        # created_at tz-aware (default to UTC if naive)
        ca = self.created_at
        if ca.tzinfo is None or ca.tzinfo.utcoffset(ca) is None:
            object.__setattr__(self, "created_at", ca.replace(tzinfo=timezone.utc))

        # non-negative engagement_count
        if self.engagement_count is not None and self.engagement_count < 0:
            raise ValueError(
                f"engagement_count must be >= 0 (got {self.engagement_count})"
            )

        # normalize empty strings to None for nullable text fields
        for name in ("post_id", "conversation_id", "body"):
            val = getattr(self, name)
            if isinstance(val, str) and val.strip() == "":
                object.__setattr__(self, name, None)

        # normalize location to postgres point literal "(lon,lat)"
        norm_loc = _normalize_point(self.location)
        object.__setattr__(self, "location", norm_loc)

    # -------- DB helpers --------
    @staticmethod
    def insert_columns(include_id: bool = False) -> Tuple[str, ...]:
        """
        Ordered column names for INSERT statements.
        Use include_id=True only for backfills with OVERRIDING SYSTEM VALUE.
        """
        cols = (
            "post_id",
            "account_id",
            "conversation_id",
            "body",
            "engagement_count",
            "location",
            "created_at",
            "retrieved_at",
        )
        return ("id",) + cols if include_id else cols

    def insert_values(self, include_id: bool = False) -> Tuple[Any, ...]:
        """
        Values tuple aligned with insert_columns().
        """
        vals = (
            self.post_id,
            self.account_id,
            self.conversation_id,
            self.body,
            self.engagement_count,
            self.location,  # "(lon,lat)" or None
            self.created_at,
            self.retrieved_at,
        )
        return ((self.id,) + vals) if include_id else vals

    @classmethod
    def from_db_row(cls, row: Mapping[str, Any]) -> "Posts":
        """
        Hydrate a Posts instance from a dict-like DB row (e.g., psycopg dict_row).
        Note: psycopg may return `point` as a tuple; we normalize it.
        """
        loc = row.get("location")
        return cls(
            created_at=row["created_at"],  # NOT NULL
            account_id=row["account_id"],  # NOT NULL
            id=row.get("id"),
            post_id=row.get("post_id"),
            conversation_id=row.get("conversation_id"),
            body=row.get("body"),
            engagement_count=row.get("engagement_count"),
            location=_normalize_point(loc),
            retrieved_at=row.get("retrieved_at"),
        )

    @staticmethod
    def make_bulk_params(
        items: List["Posts"], include_id: bool = False
    ) -> List[Tuple[Any, ...]]:
        """
        Convert a list of Posts into a list of INSERT parameter tuples.
        """
        return [p.insert_values(include_id=include_id) for p in items]
