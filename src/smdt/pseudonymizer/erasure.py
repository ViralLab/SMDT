"""GDPR-style erasure ("right to be forgotten") for pseudonymized/raw databases.

Identity resolution is forward-only (see Pseudonymizer/Hasher): given a known
real identity, we either match it literally (plaintext DB) or recompute its
pepper-keyed hash (pseudonymized DB). There is deliberately no reverse-mapping
table -- if you only have a pseudonym and not the real identity, this isn't
the tool for that, by design (see project discussion: a persisted reverse
index would be a new, highly sensitive de-anonymization asset with no
corresponding requirement here).

Table-by-table erasure rules:
  - accounts, communities: every personal column can be set to NULL directly
    (no NOT NULL constraints block it in the schema).
  - posts: same, except account_id is NOT NULL. When scrubbing a plaintext
    (non-pseudonymized) DB, it's replaced with a fresh random placeholder
    (unique per erased identity, consistent across that identity's own rows)
    rather than the real value. Left untouched when the DB is already
    pseudonymized, since the existing hash is already opaque.
  - actions: rows where the person is the *originator* are hard-deleted
    (their own behavior; nothing in the schema references an actions row, so
    this can never orphan anything). Rows where they're only the *target*
    keep the row (it's someone else's behavioral record) but have
    target_account_id nulled.
  - entities, account_enrichments, post_enrichments: hard-deleted (leaf
    nodes -- nothing references them, so deleting is always safe).
  - dataset_meta: untouched (operator-authored dataset metadata, not
    personal data).
"""

from __future__ import annotations
import logging
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

from ..store.standard_db import StandardDB
from .pseudonyms import Algorithm, Hasher

log = logging.getLogger(__name__)


class ErasureMode(str, Enum):
    """What to do with accounts/posts/communities rows for the erased person.

    Only affects accounts/posts (and the account's own row); actions/entities/
    enrichments always follow the fixed rules in the module docstring
    regardless of mode.
    """

    DELETE = "delete"
    SCRUB = "scrub"


@dataclass(frozen=True)
class ErasureTarget:
    """One database to erase a person's data from.

    Attributes:
        db_name: Database to connect to.
        mode: DELETE (hard-remove) or SCRUB (null personal columns, keep the
            row) for accounts/posts.
        is_pseudonymized: True if this DB already stores hashed identifiers
            (e.g. a Pseudonymizer destination DB) -- matching recomputes the
            hash, and posts.account_id is left as-is when scrubbing since
            it's already opaque. False for a plaintext (source) DB --
            matching is literal, and scrubbing posts.account_id needs a
            placeholder since it can't be NULL there.
    """

    db_name: str
    mode: ErasureMode
    is_pseudonymized: bool


class Eraser:
    """Erases all data belonging to one real identity across configured targets."""

    def __init__(
        self,
        targets: List[ErasureTarget],
        pepper: Optional[bytes] = None,
        algorithm: Algorithm = Algorithm.SHA256,
    ):
        """Initialize the Eraser.

        Args:
            targets: Databases to erase from, each with its own mode.
            pepper: Secret pepper -- required if any target is pseudonymized,
                to recompute the same hash used when that DB was populated.
            algorithm: Hashing algorithm -- must match what pseudonymized the
                target DB(s).

        Raises:
            ValueError: If a pseudonymized target is configured without a pepper.
        """
        self.targets = targets
        self.hasher = (
            Hasher(
                algo=algorithm, pepper=pepper, normalizer=lambda s: s.strip().lower()
            )
            if pepper
            else None
        )
        if self.hasher is None and any(t.is_pseudonymized for t in targets):
            raise ValueError(
                "pepper is required to erase from a pseudonymized target"
            )

    def erase(
        self, identity: str, identity_column: str = "account_id"
    ) -> Dict[str, Dict[str, Any]]:
        """Erase all data belonging to `identity` across every configured target.

        Args:
            identity: The account's real identity as it appears in the
                plaintext source system (e.g. the platform account_id or
                username). Always the *real* value, even for pseudonymized
                targets -- it's hashed internally for those.
            identity_column: Which accounts column identity matches --
                "account_id" or "username".

        Returns:
            Per-target report keyed by db_name, e.g.:
            {"matched_account_ids": [...], "posts_scrubbed": N,
            "posts_deleted": N, "accounts_scrubbed"/"accounts_deleted": N,
            "actions_deleted": N, "actions_target_cleared": N,
            "entities_deleted": N, "account_enrichments_deleted": N,
            "post_enrichments_deleted": N, "community_owner_refs_cleared": N}.
        """
        report: Dict[str, Dict[str, Any]] = {}
        for target in self.targets:
            report[target.db_name] = self._erase_from_target(
                identity, identity_column, target
            )
        return report

    def _erase_from_target(
        self, identity: str, identity_column: str, target: ErasureTarget
    ) -> Dict[str, Any]:
        """Resolve `identity` to account_id(s) in `target` and erase each."""
        if identity_column not in ("account_id", "username"):
            raise ValueError(
                f"identity_column must be 'account_id' or 'username', got {identity_column!r}"
            )
        match_value = (
            self.hasher.make(identity) if target.is_pseudonymized else identity
        )

        db = StandardDB(target.db_name)
        conn = db.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT DISTINCT account_id FROM accounts WHERE {identity_column} = %s",
                    (match_value,),
                )
                account_ids = [r[0] for r in cur.fetchall() if r[0] is not None]

                result: Dict[str, Any] = {"matched_account_ids": account_ids}
                for account_id in account_ids:
                    self._erase_account(cur, account_id, target, result)
            conn.commit()
            return result
        finally:
            conn.close()

    def _erase_account(
        self,
        cur,
        account_id: str,
        target: ErasureTarget,
        result: Dict[str, Any],
    ) -> None:
        """Apply every table's erasure rule for one resolved account_id."""

        def _bump(key: str, n: int) -> None:
            result[key] = result.get(key, 0) + n

        # Leaf tables: always hard-deleted regardless of mode -- nothing
        # references these rows, so deleting can never orphan anything.
        cur.execute("DELETE FROM entities WHERE account_id = %s", (account_id,))
        _bump("entities_deleted", cur.rowcount)

        cur.execute(
            "DELETE FROM account_enrichments WHERE account_id = %s", (account_id,)
        )
        _bump("account_enrichments_deleted", cur.rowcount)

        cur.execute(
            "DELETE FROM post_enrichments WHERE post_id IN "
            "(SELECT post_id FROM posts WHERE account_id = %s)",
            (account_id,),
        )
        _bump("post_enrichments_deleted", cur.rowcount)

        # actions: originator rows are this person's own behavior (delete);
        # target-only rows belong to whoever originated them (keep the row,
        # clear just the reference to this person).
        cur.execute(
            "DELETE FROM actions WHERE originator_account_id = %s", (account_id,)
        )
        _bump("actions_deleted", cur.rowcount)

        cur.execute(
            "UPDATE actions SET target_account_id = NULL WHERE target_account_id = %s",
            (account_id,),
        )
        _bump("actions_target_cleared", cur.rowcount)

        # communities: only the owner reference names this person; the
        # community itself isn't being erased.
        cur.execute(
            "UPDATE communities SET owner_account_id = NULL WHERE owner_account_id = %s",
            (account_id,),
        )
        _bump("community_owner_refs_cleared", cur.rowcount)

        # A fresh placeholder per erased identity (not shared globally, not
        # derived from the real value) -- keeps posts/account rows for this
        # person joined to each other without being derivable back to them.
        placeholder = f"erased_{uuid.uuid4().hex}"

        # posts
        if target.mode == ErasureMode.DELETE:
            cur.execute("DELETE FROM posts WHERE account_id = %s", (account_id,))
            _bump("posts_deleted", cur.rowcount)
        else:
            new_account_id = account_id if target.is_pseudonymized else placeholder
            cur.execute(
                """
                UPDATE posts SET
                    body = NULL, conversation_id = NULL, community_id = NULL,
                    location = NULL, like_count = NULL, dislike_count = NULL,
                    view_count = NULL, share_count = NULL, comment_count = NULL,
                    quote_count = NULL, bookmark_count = NULL,
                    account_id = %s
                WHERE account_id = %s
                """,
                (new_account_id, account_id),
            )
            _bump("posts_scrubbed", cur.rowcount)

        # accounts
        if target.mode == ErasureMode.DELETE:
            cur.execute("DELETE FROM accounts WHERE account_id = %s", (account_id,))
            _bump("accounts_deleted", cur.rowcount)
        else:
            new_account_id = account_id if target.is_pseudonymized else placeholder
            cur.execute(
                """
                UPDATE accounts SET
                    username = NULL, profile_name = NULL, bio = NULL,
                    location = NULL, profile_image_url = NULL,
                    post_count = NULL, friend_count = NULL, follower_count = NULL,
                    is_verified = NULL, account_id = %s
                WHERE account_id = %s
                """,
                (new_account_id, account_id),
            )
            _bump("accounts_scrubbed", cur.rowcount)
