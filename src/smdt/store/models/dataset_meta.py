from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Mapping, Any, Tuple, List


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class DatasetMeta:
    """
    Python model for the `dataset_meta` table.

    One row per database, describing the dataset ingested into it.
    `dataset_description` accumulates a timestamped log entry per ingestion run
    (see ingest.pipeline for the upsert-with-append logic).
    """

    platform: str = field()
    standardizer_name: str = field()
    created_at: datetime = field()
    updated_at: datetime = field()

    id: Optional[int] = None
    dataset_description: Optional[str] = None

    __table_name__ = "dataset_meta"
    __jsonb_fields__ = set()

    # -------- Validation / normalization --------
    def __post_init__(self):
        """
        Validates the dataclass fields.

        Raises:
            ValueError: If platform, standardizer_name, created_at or updated_at are missing.
        """
        if not self.platform or not self.platform.strip():
            raise ValueError("platform is required and cannot be empty")
        object.__setattr__(self, "platform", self.platform.strip())

        if not self.standardizer_name or not self.standardizer_name.strip():
            raise ValueError("standardizer_name is required and cannot be empty")
        object.__setattr__(self, "standardizer_name", self.standardizer_name.strip())

        for attr in ("created_at", "updated_at"):
            val = getattr(self, attr)
            if val is None:
                raise ValueError(f"{attr} is required and cannot be None")
            if val.tzinfo is None or val.tzinfo.utcoffset(val) is None:
                object.__setattr__(self, attr, val.replace(tzinfo=timezone.utc))

        if (
            isinstance(self.dataset_description, str)
            and self.dataset_description.strip() == ""
        ):
            object.__setattr__(self, "dataset_description", None)

    # -------- DB helpers --------
    @staticmethod
    def insert_columns(include_id: bool = False) -> Tuple[str, ...]:
        """
        Ordered column names for INSERT statements.
        """
        cols = (
            "platform",
            "standardizer_name",
            "dataset_description",
            "created_at",
            "updated_at",
        )
        return ("id",) + cols if include_id else cols

    def insert_values(self, include_id: bool = False) -> Tuple[Any, ...]:
        """
        Values tuple aligned with insert_columns().
        """
        vals = (
            self.platform,
            self.standardizer_name,
            self.dataset_description,
            self.created_at,
            self.updated_at,
        )
        return ((self.id,) + vals) if include_id else vals

    @classmethod
    def from_db_row(cls, row: Mapping[str, Any]) -> "DatasetMeta":
        """
        Hydrate a DatasetMeta instance from a dict-like DB row.
        """
        return cls(
            platform=row["platform"],
            standardizer_name=row["standardizer_name"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            id=row.get("id"),
            dataset_description=row.get("dataset_description"),
        )

    @staticmethod
    def make_bulk_params(
        items: List["DatasetMeta"], include_id: bool = False
    ) -> List[Tuple[Any, ...]]:
        """
        Convert a list of DatasetMeta into a list of INSERT parameter tuples.
        """
        return [d.insert_values(include_id=include_id) for d in items]
