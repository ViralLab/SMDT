from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Mapping, Any, Tuple, List, Union

from smdt.store.utils.geo import normalize_point as _normalize_point


@dataclass(frozen=True, eq=True)
class Accounts:
    """Python model for the ``accounts`` table.

    Attributes:
        created_at: Account creation timestamp (tz-aware; naive datetimes coerced to UTC).
        id: Internal database primary key.
        account_id: Platform-specific account identifier.
        username: Handle/username.
        profile_name: Display name.
        bio: Profile biography text.
        location: Geographic location for the PostGIS ``geometry(Point, 4326)``
            column, as EWKT text (``"SRID=4326;POINT(lon lat)"``) or a
            ``(lon, lat)`` tuple/list (converted to EWKT automatically).
        post_count: Number of posts authored (must be >= 0).
        friend_count: Number of accounts followed (must be >= 0).
        follower_count: Number of followers (must be >= 0).
        is_verified: Whether the account has a platform verification badge.
        profile_image_url: URL of the account's profile image.
        platform: Canonical source platform (e.g. "twitter", "weibo").
        retrieved_at: Timestamp when the record was retrieved.
    """

    # NOT NULL first (dataclasses require non-defaults before defaults)
    created_at: datetime = field()

    # Optionals
    id: Optional[int] = None
    account_id: Optional[str] = None
    username: Optional[str] = None
    profile_name: Optional[str] = None
    bio: Optional[str] = None
    location: Optional[Union[str, Tuple[float, float], List[float]]] = None
    post_count: Optional[int] = None
    friend_count: Optional[int] = None
    follower_count: Optional[int] = None
    is_verified: Optional[bool] = None
    profile_image_url: Optional[str] = None
    platform: Optional[str] = None
    retrieved_at: Optional[datetime] = None

    # Metadata for StandardDB fallback
    __table_name__ = "accounts"
    __jsonb_fields__ = set()

    # -------- Validation / normalization --------
    def __post_init__(self):
        """
        Validates and standardizes the dataclass fields.
        Ensures strings are trimmed and numerical counts are non-negative.
        """
        # Ensure created_at is timezone-aware (UTC if naive)
        ca = self.created_at
        if ca.tzinfo is None or ca.tzinfo.utcoffset(ca) is None:
            object.__setattr__(self, "created_at", ca.replace(tzinfo=timezone.utc))

        # Non-negative checks mirroring SQL CHECKs
        for name in ("post_count", "friend_count", "follower_count"):
            val = getattr(self, name)
            if val is not None and val < 0:
                raise ValueError(f"{name} must be >= 0 (got {val})")

        # Trim strings; empty → None
        for name in (
            "account_id",
            "username",
            "profile_name",
            "bio",
            "profile_image_url",
            "platform",
        ):
            val = getattr(self, name)
            if isinstance(val, str):
                s = val.strip()
                object.__setattr__(self, name, s or None)

        # normalize location to PostGIS EWKT text
        object.__setattr__(self, "location", _normalize_point(self.location))

    # -------- DB helpers --------
    @staticmethod
    def insert_columns(include_id: bool = False) -> Tuple[str, ...]:
        """
        Return column names (in order) for INSERT.
        """
        cols = (
            "account_id",
            "username",
            "profile_name",
            "bio",
            "location",
            "post_count",
            "friend_count",
            "follower_count",
            "is_verified",
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
        vals = (
            self.account_id,
            self.username,
            self.profile_name,
            self.bio,
            self.location,  # EWKT text or None
            self.post_count,
            self.friend_count,
            self.follower_count,
            self.is_verified,
            self.profile_image_url,
            self.platform,
            self.created_at,
            self.retrieved_at,
        )
        return ((self.id,) + vals) if include_id else vals

    @classmethod
    def from_db_row(cls, row: Mapping[str, Any]) -> "Accounts":
        """
        Hydrate from a dict-like DB row (e.g., psycopg row with name access).
        """
        return cls(
            created_at=row["created_at"],  # NOT NULL
            id=row.get("id"),
            account_id=row.get("account_id"),
            username=row.get("username"),
            profile_name=row.get("profile_name"),
            bio=row.get("bio"),
            location=row.get("location"),
            post_count=row.get("post_count"),
            friend_count=row.get("friend_count"),
            follower_count=row.get("follower_count"),
            is_verified=row.get("is_verified"),
            profile_image_url=row.get("profile_image_url"),
            platform=row.get("platform"),
            retrieved_at=row.get("retrieved_at"),
        )

    @staticmethod
    def make_bulk_params(
        accounts: List["Accounts"], include_id: bool = False
    ) -> List[Tuple[Any, ...]]:
        """
        Convert a list of Accounts to INSERT parameter tuples.
        """
        return [a.insert_values(include_id=include_id) for a in accounts]
