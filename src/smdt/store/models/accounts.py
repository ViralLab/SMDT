from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Mapping, Any, Tuple, List


@dataclass(frozen=True, eq=True)
class Accounts:
    """
    Python model for `accounts` table.

    Notes:
      - `location` maps to Postgres `point`. Pass as string "(lon,lat)" (lon=x, lat=y).
      - `username` is the handle/username; `profile_name` is the display name.
      - `created_at` is required (TIMESTAMPTZ NOT NULL); if naive, coerced to UTC.
    """

    # NOT NULL first (dataclasses require non-defaults before defaults)
    created_at: datetime = field()

    # Optionals
    id: Optional[int] = None
    account_id: Optional[str] = None
    username: Optional[str] = None  # ← matches schema
    profile_name: Optional[str] = None
    bio: Optional[str] = None
    location: Optional[str] = None  # Postgres point → "(lon,lat)" string or None
    post_count: Optional[int] = None
    friend_count: Optional[int] = None
    follower_count: Optional[int] = None
    is_verified: Optional[bool] = None
    profile_image_url: Optional[str] = None
    retrieved_at: Optional[datetime] = None

    # Metadata for StandardDB fallback
    __table_name__ = "accounts"
    __jsonb_fields__ = set()  # no JSONB columns here

    # -------- Validation / normalization --------
    def __post_init__(self):
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
            "location",
            "profile_image_url",
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
            self.location,  # "(lon,lat)" or None for Postgres point
            self.post_count,
            self.friend_count,
            self.follower_count,
            self.is_verified,
            self.profile_image_url,
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
