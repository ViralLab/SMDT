from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Mapping, Any, Tuple, List, Union

try:
    from enum import StrEnum  # Python 3.11+
except ImportError:
    from enum import Enum

    class StrEnum(str, Enum):
        pass


class ActionType(StrEnum):
    """
    Enumeration of supported action types.
    """

    UPVOTE = "UPVOTE"
    DOWNVOTE = "DOWNVOTE"
    SHARE = "SHARE"
    QUOTE = "QUOTE"
    UNFOLLOW = "UNFOLLOW"
    FOLLOW = "FOLLOW"
    COMMENT = "COMMENT"
    BLOCK = "BLOCK"
    LINK = "LINK"


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class Actions:
    """
    Python model for `actions` table.
    Represents an interaction between accounts, posts, or communities.
    """

    created_at: datetime = field()
    action_type: Union[str, ActionType] = field()

    id: Optional[int] = None
    originator_account_id: Optional[str] = None
    originator_post_id: Optional[str] = None
    target_account_id: Optional[str] = None
    target_post_id: Optional[str] = None
    originator_community_id: Optional[str] = None
    target_community_id: Optional[str] = None
    retrieved_at: Optional[datetime] = None

    # Metadata for StandardDB fallback
    __table_name__ = "actions"
    __jsonb_fields__ = set()

    # -------- Validation --------
    def __post_init__(self):
        """
        Validates the dataclass fields.
        Ensures action_type is valid and strings are normalized.
        """
        # Normalize action_type
        at = self.action_type
        if isinstance(at, str):
            try:
                at = ActionType(at.upper())
            except ValueError:
                raise ValueError(f"Invalid action_type: {self.action_type!r}")
            object.__setattr__(self, "action_type", at)

        # Ensure created_at is tz-aware
        ca = self.created_at
        if ca.tzinfo is None or ca.tzinfo.utcoffset(ca) is None:
            object.__setattr__(self, "created_at", ca.replace(tzinfo=timezone.utc))

        # Normalize empty strings to None
        for name in (
            "originator_account_id",
            "originator_post_id",
            "target_account_id",
            "target_post_id",
            "originator_community_id",
            "target_community_id",
        ):
            val = getattr(self, name)
            if isinstance(val, str) and val.strip() == "":
                object.__setattr__(self, name, None)

    # -------- DB helpers --------
    @staticmethod
    def insert_columns(include_id: bool = False) -> Tuple[str, ...]:
        """
        Ordered column list for INSERT statements.
        """
        cols = (
            "originator_account_id",
            "originator_post_id",
            "target_account_id",
            "target_post_id",
            "originator_community_id",
            "target_community_id",
            "action_type",
            "created_at",
            "retrieved_at",
        )
        return ("id",) + cols if include_id else cols

    def insert_values(self, include_id: bool = False) -> Tuple[Any, ...]:
        """
        Values tuple aligned with insert_columns().
        """
        at_str = (
            self.action_type.value
            if isinstance(self.action_type, ActionType)
            else str(self.action_type)
        )
        vals = (
            self.originator_account_id,
            self.originator_post_id,
            self.target_account_id,
            self.target_post_id,
            self.originator_community_id,
            self.target_community_id,
            at_str,
            self.created_at,
            self.retrieved_at,
        )
        return ((self.id,) + vals) if include_id else vals

    @classmethod
    def from_db_row(cls, row: Mapping[str, Any]) -> "Actions":
        """
        Hydrate from a dict-like DB row.
        """
        return cls(
            created_at=row["created_at"],  # NOT NULL
            action_type=row["action_type"],  # NOT NULL
            id=row.get("id"),
            originator_account_id=row.get("originator_account_id"),
            originator_post_id=row.get("originator_post_id"),
            target_account_id=row.get("target_account_id"),
            target_post_id=row.get("target_post_id"),
            originator_community_id=row.get("originator_community_id"),
            target_community_id=row.get("target_community_id"),
            retrieved_at=row.get("retrieved_at"),
        )

    @staticmethod
    def make_bulk_params(
        items: List["Actions"], include_id: bool = False
    ) -> List[Tuple[Any, ...]]:
        """
        Convert a list of Actions to INSERT parameter tuples.
        """
        return [a.insert_values(include_id=include_id) for a in items]
