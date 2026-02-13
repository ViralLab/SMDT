"""Anonymization policy configuration."""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Set, Mapping


@dataclass(frozen=True)
class AnonPolicy:
    """Column-level actions per table: HASH, REDACT, DROP, KEEP, BLANK.

    - `hash_cols`: full pseudonymization columns (IDs, usernames, etc.)
    - `redact_cols`: free-text columns to redact in-place (becomes *_redacted)
    - `drop_cols`: columns removed entirely in anon DB
    - `blank_cols`: columns kept in schema but set to NULL on export
    - Everything else is copied as-is (KEEP)
    """

    hash_cols: Mapping[str, Set[str]] = field(default_factory=dict)
    redact_cols: Mapping[str, Set[str]] = field(default_factory=dict)
    drop_cols: Mapping[str, Set[str]] = field(default_factory=dict)
    blank_cols: Mapping[str, Set[str]] = field(default_factory=dict)

    def is_hash(self, table: str, col: str) -> bool:
        """Check if a column should be hashed.

        Args:
            table: Name of the table.
            col: Name of the column.

        Returns:
            True if the column should be hashed, False otherwise.
        """
        return col in self.hash_cols.get(table, set())

    def is_redact(self, table: str, col: str) -> bool:
        """Check if a column should be redacted.

        Args:
            table: Name of the table.
            col: Name of the column.

        Returns:
            True if the column should be redacted, False otherwise.
        """
        return col in self.redact_cols.get(table, set())

    def is_drop(self, table: str, col: str) -> bool:
        """Check if a column should be dropped.

        Args:
            table: Name of the table.
            col: Name of the column.

        Returns:
            True if the column should be dropped, False otherwise.
        """
        return col in self.drop_cols.get(table, set())

    def is_blank(self, table: str, col: str) -> bool:
        """Check if a column should be blanked (set to NULL).

        Args:
            table: Name of the table.
            col: Name of the column.

        Returns:
            True if the column should be blanked, False otherwise.
        """
        return col in self.blank_cols.get(table, set())


DEFAULT_POLICY = AnonPolicy(
    hash_cols={
        "communities": {"community_id", "community_username", "community_name", "bio"},
        "accounts": {"account_id", "username", "profile_name"},
        "posts": {"post_id", "account_id", "conversation_id", "community_id"},
        "actions": {
            "originator_account_id",
            "originator_post_id",
            "originator_community_id",
            "target_account_id",
            "target_post_id",
            "target_community_id",
        },
        "entities": {"post_id", "account_id", "community_id"},
        "account_enrichments": {"account_id"},
        "post_enrichments": {"post_id"},
    },
    redact_cols={
        "communities": {"bio"},
        "accounts": {"bio"},
        "posts": {"body"},
    },
    drop_cols={
        # keep schema compatibility; do not drop these now
    },
    blank_cols={"accounts": {"profile_image_url"}},
)
