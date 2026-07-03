from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Mapping, Any, Tuple, List, Union

try:
    from enum import StrEnum  # Python 3.11+
except ImportError:
    from enum import Enum

    class StrEnum(str, Enum):
        pass


class CommunityType(StrEnum):
    """Community types (``CHANNEL`` or ``GROUP``)."""

    CHANNEL = "CHANNEL"
    GROUP = "GROUP"


@dataclass(frozen=True, eq=True)
class Communities:
    """Python model for the ``communities`` table.

    Attributes:
        created_at: Community creation timestamp (tz-aware; naive datetimes coerced to UTC).
        community_type: Type — one of ``"CHANNEL"``, ``"GROUP"``.
        id: Internal database primary key.
        community_id: Platform-specific community identifier.
        community_username: Handle/username of the community.
        community_name: Display name of the community.
        bio: Community description/biography.
        is_public: Whether the community is publicly accessible.
        member_count: Number of members (must be >= 0).
        post_count: Number of posts in the community (must be >= 0).
        owner_account_id: Account ID of the community owner.
        profile_image_url: URL of the community's profile image.
        platform: Canonical source platform (e.g. "twitter", "weibo").
        retrieved_at: Timestamp when the record was retrieved.
    """

    # NOT NULL first (dataclasses require non-defaults before defaults)
    created_at: datetime = field()
    community_type: Union[str, CommunityType] = field()

    # Optionals
    id: Optional[int] = None
    community_id: Optional[str] = None
    community_username: Optional[str] = None
    community_name: Optional[str] = None
    bio: Optional[str] = None
    is_public: Optional[bool] = None
    member_count: Optional[int] = None
    post_count: Optional[int] = None
    owner_account_id: Optional[str] = None
    profile_image_url: Optional[str] = None
    platform: Optional[str] = None
    retrieved_at: Optional[datetime] = None

    # Metadata for StandardDB fallback
    __table_name__ = "communities"
    __jsonb_fields__ = set()

    # -------- Validation / normalization --------
    def __post_init__(self):
        """
        Validates the dataclass fields.
        Standardizes timestamps and string values.
        """
        # Ensure created_at is timezone-aware (UTC if naive)
        ca = self.created_at
        if ca.tzinfo is None or ca.tzinfo.utcoffset(ca) is None:
            object.__setattr__(self, "created_at", ca.replace(tzinfo=timezone.utc))

        # Non-negative checks mirroring SQL CHECKs
        for name in ("member_count", "post_count"):
            val = getattr(self, name)
            if val is not None and val < 0:
                raise ValueError(f"{name} must be >= 0 (got {val})")

        # Trim strings; empty → None
        for name in (
            "community_id",
            "community_type",
            "community_username",
            "community_name",
            "bio",
            "profile_image_url",
            "platform",
        ):
            val = getattr(self, name)
            if isinstance(val, str):
                s = val.strip()
                object.__setattr__(self, name, s or None)

    # -------- DB helpers --------
    @staticmethod
    def insert_columns(include_id: bool = False) -> Tuple[str, ...]:
        """
        Return column names (in order) for INSERT.
        """
        cols = (
            "community_id",
            "community_type",
            "community_username",
            "community_name",
            "bio",
            "is_public",
            "member_count",
            "post_count",
            "owner_account_id",
            "profile_image_url",
            "platform",
            "created_at",
            "retrieved_at",
        )
        return ("id",) + cols if include_id else cols

    def insert_values(self, include_id: bool = False) -> Tuple[Any, ...]:
        """
        Return values tuple aligned with insert_columns().
        """
        ct_str = (
            self.community_type.value
            if isinstance(self.community_type, CommunityType)
            else str(self.community_type)
        )

        vals = (
            self.community_id,
            ct_str,
            self.community_username,
            self.community_name,
            self.bio,
            self.is_public,
            self.member_count,
            self.post_count,
            self.owner_account_id,
            self.profile_image_url,
            self.platform,
            self.created_at,
            self.retrieved_at,
        )
        return ((self.id,) + vals) if include_id else vals

    @classmethod
    def from_db_row(cls, row: Mapping[str, Any]) -> "Communities":
        """
        Hydrate from a dict-like DB row (e.g., psycopg row with name access).
        """
        return cls(
            created_at=row["created_at"],  # NOT NULL
            id=row.get("id"),
            community_id=row.get("community_id"),
            community_type=row.get("community_type"),
            community_username=row.get("community_username"),
            community_name=row.get("community_name"),
            bio=row.get("bio"),
            is_public=row.get("is_public"),
            member_count=row.get("member_count"),
            post_count=row.get("post_count"),
            owner_account_id=row.get("owner_account_id"),
            profile_image_url=row.get("profile_image_url"),
            platform=row.get("platform"),
            retrieved_at=row.get("retrieved_at"),
        )

    @staticmethod
    def make_bulk_params(
        communities: List["Communities"], include_id: bool = False
    ) -> List[Tuple[Any, ...]]:
        """
        Convert a list of Communities to INSERT parameter tuples.
        """
        return [c.insert_values(include_id=include_id) for c in communities]
