from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Mapping, Any, Tuple, List, Dict, Union

try:
    from enum import StrEnum  # Python 3.11+
except ImportError:
    from enum import Enum

    class StrEnum(str, Enum):  # Fallback for Python <3.11
        pass


class EntityType(StrEnum):
    IMAGE = "IMAGE"
    VIDEO = "VIDEO"
    LINK = "LINK"
    USER_TAG = "USER_TAG"
    HASHTAG = "HASHTAG"
    EMAIL = "EMAIL"


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class Entities:
    created_at: datetime = field()
    entity_type: Union[str, EntityType] = field()

    id: Optional[int] = None
    account_id: Optional[str] = None
    community_id: Optional[str] = None
    post_id: Optional[str] = None
    body: Optional[str] = None
    retrieved_at: Optional[datetime] = None

    # Metadata for StandardDB fallback
    __table_name__ = "entities"
    __jsonb_fields__ = set()  # no JSONB columns here

    def __post_init__(self):
        # entity_type: accept str or Enum; normalize to Enum (uppercased for safety)
        et = self.entity_type
        if isinstance(et, str):
            try:
                et = EntityType(et.upper())
            except ValueError:
                raise ValueError(f"Invalid entity_type: {self.entity_type!r}")
            object.__setattr__(self, "entity_type", et)

        if self.created_at is None:
            raise ValueError("created_at is required and cannot be None")
        # created_at tz-aware (UTC if naive)
        ca = self.created_at
        if ca.tzinfo is None or ca.tzinfo.utcoffset(ca) is None:
            object.__setattr__(self, "created_at", ca.replace(tzinfo=timezone.utc))

        # normalize empty strings to None
        for name in ("account_id", "community_id", "post_id"):
            val = getattr(self, name)
            if isinstance(val, str) and val.strip() == "":
                object.__setattr__(self, name, None)

        if not (self.post_id or self.community_id):
            raise ValueError("Either post_id or community_id must be provided")

        # body must be a dict if provided (JSONB)
        if self.body is not None and not isinstance(self.body, str):
            raise ValueError("body must be a string (TEXT) or None")

    @staticmethod
    def insert_columns(include_id: bool = False) -> Tuple[str, ...]:
        cols = (
            "account_id",
            "community_id",
            "post_id",
            "body",
            "entity_type",
            "created_at",
            "retrieved_at",
        )
        return ("id",) + cols if include_id else cols

    def insert_values(self, include_id: bool = False) -> Tuple[Any, ...]:
        # ensure entity_type is str for the DB adapter
        et_str = (
            self.entity_type.value
            if isinstance(self.entity_type, EntityType)
            else str(self.entity_type)
        )
        vals = (
            self.account_id,
            self.community_id,
            self.post_id,
            self.body,
            et_str,
            self.created_at,
            self.retrieved_at,
        )
        return ((self.id,) + vals) if include_id else vals

    @classmethod
    def from_db_row(cls, row: Mapping[str, Any]) -> "Entities":
        return cls(
            id=row.get("id"),
            account_id=row.get("account_id"),
            community_id=row.get("community_id"),
            post_id=row.get("post_id"),
            body=row.get("body"),
            entity_type=row["entity_type"],  # NOT NULL (enum text)
            created_at=row["created_at"],  # NOT NULL
            retrieved_at=row.get("retrieved_at"),
        )

    @staticmethod
    def make_bulk_params(
        entities: List["Entities"], include_id: bool = False
    ) -> List[Tuple[Any, ...]]:
        return [e.insert_values(include_id=include_id) for e in entities]
