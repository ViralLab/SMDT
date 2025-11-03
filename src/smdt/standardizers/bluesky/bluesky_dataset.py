from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, List, Mapping, Optional, Sequence

from smdt.standardizers.base import Standardizer, SourceInfo
from smdt.standardizers.utils import (
    extract_hashtags,
    extract_mentions,
    extract_urls,
    extract_emails,
)
from smdt.store.models.accounts import Accounts
from smdt.store.models.posts import Posts
from smdt.store.models.entities import Entities, EntityType
from smdt.store.models.actions import Actions, ActionType


# ---------------- helpers ----------------


def _to_str(x: Any) -> str:
    if isinstance(x, str):
        return x
    if x is None:
        return ""
    try:
        if x != x:  # NaN/NA
            return ""
    except Exception:
        pass
    return str(x)


def _as_int_str(x: Any) -> Optional[str]:
    """Return a stringified integer-like id or None."""
    if x is None:
        return None
    try:
        s = str(int(x))
        return s
    except Exception:
        try:
            s = str(x).strip()
            return s if s else None
        except Exception:
            return None


def _dt(ts: Any) -> Optional[datetime]:
    """Parse common Bluesky dump timestamp variants."""
    if ts is None:
        return None
    s = _to_str(ts).strip()
    if not s:
        return None
    # try compact yyyymmddHHMM first (e.g., 202401120915)
    try:
        if len(s) in (12, 10):  # yyyymmddHHMM or yyyymmddHH
            fmt = "%Y%m%d%H%M" if len(s) == 12 else "%Y%m%d%H"
            d = datetime.strptime(s, fmt)
            return d.replace(tzinfo=timezone.utc)
    except Exception:
        pass
    # try date + time with/without micros
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            d = datetime.strptime(s, fmt)
            return d.replace(tzinfo=timezone.utc)
        except Exception:
            pass
    # try ISO8601, with Z handling
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        d = datetime.fromisoformat(s)
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d
    except Exception:
        return None


def _retrieved_at(_: Mapping[str, Any]) -> datetime:
    # No embedded __twarc like Twitter; use "now".
    return datetime.now(timezone.utc)


def _sum_metrics(m: Mapping[str, Any] | None, keys: Sequence[str]) -> Optional[int]:
    if not m:
        return None
    total = 0
    ok = False
    for k in keys:
        try:
            v = int(m.get(k, 0) or 0)
            total += v
            ok = True or ok
        except Exception:
            pass
    return total if ok else None


def _member_kind(src: SourceInfo) -> str:
    """
    Classify by archive member path (or filename) to choose parsing logic.
    Expected Bluesky drops have files like:
      - posts/*.json or feed_posts/*.json  (lines of JSON objects)
      - interactions.csv / interactions.tsv
      - feed_posts_likes.csv / .tsv
    """
    name = (src.member or src.path or "").lower()
    if "interactions" in name:
        return "interactions"
    if "likes" in name or "feed_posts_likes" in name:
        return "likes"
    if "feed_posts" in name or "posts" in name:
        return "posts"
    return "unknown"


# ---------------- main standardizer ----------------


@dataclass
class BlueSkyDatasetStandardizer(Standardizer):
    name: str = "bluesky_dataset_standardizer"

    def standardize(self, input_record) -> List[Any]:
        """
        Emits: Accounts, Posts, Entities, Actions for Bluesky dumps.

        We rely on SourceInfo.member (or path) to decide which schema a row belongs to:
          - posts/feed_posts: JSON records with fields {post_id, user_id, text, date, like_count, reply_count, repost_count}
          - interactions: CSV/TSV rows:
              [user_id, replied_author, thread_root_author, reposted_author, quoted_author, date]
          - likes: CSV/TSV rows:
              [liker_id, post_author_id, post_id, timestamp]
        """
        record, src = input_record
        outputs = []

        kind = _member_kind(src)
        ra = _retrieved_at(record)

        # -------- POSTS / FEED_POSTS (JSON) ----------
        if kind == "posts":
            # Robust field extraction
            post_id = _as_int_str(record.get("post_id") or record.get("id"))
            user_id = _as_int_str(
                record.get("user_id")
                or record.get("author")
                or record.get("account_id")
            )
            created_at = _dt(record.get("date") or record.get("created_at"))

            # Account (minimal; Bluesky drops rarely include user profile rows inline)
            if user_id:
                # We don't know the true account creation time; set created_at = row time (or RA) to keep row valid.
                acct_created = created_at or ra
                outputs.append(
                    Accounts(
                        created_at=acct_created,
                        retrieved_at=ra,
                        account_id=user_id,
                        # optional fields left None (username, profile_name, etc.)
                    )
                )

            # Post
            if post_id and user_id:
                # engagement summary from common keys
                pm = {
                    "like_count": record.get("like_count"),
                    "reply_count": record.get("reply_count"),
                    "repost_count": record.get("repost_count"),
                }
                engagement = _sum_metrics(
                    pm, ("like_count", "reply_count", "repost_count")
                )
                body = _to_str(record.get("text"))

                outputs.append(
                    Posts(
                        created_at=created_at or ra,
                        retrieved_at=ra,
                        post_id=post_id,
                        account_id=user_id,
                        conversation_id=None,
                        body=body,
                        engagement_count=engagement,
                        location=None,
                    )
                )

                # Entities from text
                for tag in extract_hashtags(body):
                    outputs.append(
                        Entities(
                            created_at=created_at or ra,
                            retrieved_at=ra,
                            entity_type=EntityType.HASHTAG,
                            account_id=user_id,
                            post_id=post_id,
                            body=tag.replace("#", ""),
                        )
                    )
                for m in extract_mentions(body):
                    outputs.append(
                        Entities(
                            created_at=created_at or ra,
                            retrieved_at=ra,
                            entity_type=EntityType.USER_TAG,
                            account_id=user_id,
                            post_id=post_id,
                            body=m.replace("@", ""),
                        )
                    )
                for u in extract_urls(body):
                    outputs.append(
                        Entities(
                            created_at=created_at or ra,
                            retrieved_at=ra,
                            entity_type=EntityType.LINK,
                            account_id=user_id,
                            post_id=post_id,
                            body=str(u),
                        )
                    )
                for e in extract_emails(body):
                    outputs.append(
                        Entities(
                            created_at=created_at or ra,
                            retrieved_at=ra,
                            entity_type=EntityType.EMAIL,
                            account_id=user_id,
                            post_id=post_id,
                            body=e,
                        )
                    )
            return outputs

        # -------- INTERACTIONS (CSV/TSV row) ----------
        if kind == "interactions":
            # schema: [user_id, replied_author, thread_root_author, reposted_author, quoted_author, date]
            # Accept both list-like and mapping-like rows
            row = record

            def _geti(i: int) -> Any:
                if isinstance(row, Mapping):
                    # tolerate header names if present
                    keys = (
                        "user_id",
                        "replied_author",
                        "thread_root_author",
                        "reposted_author",
                        "quoted_author",
                        "date",
                    )
                    key = keys[i] if i < len(keys) else str(i)
                    # try numeric index first for pandas chunks converted to dict with integer keys
                    return row.get(i, row.get(key))
                else:
                    try:
                        return row[i]
                    except Exception:
                        return None

            originator = _as_int_str(_geti(0))
            replied = _as_int_str(_geti(1))
            root_auth = _as_int_str(_geti(2))
            reposted = _as_int_str(_geti(3))
            quoted = _as_int_str(_geti(4))
            ts = _dt(_geti(5)) or ra

            # Emit bare accounts so FK constraints are happy later
            for uid in (originator, replied, root_auth, reposted, quoted):
                if uid:
                    outputs.append(
                        Accounts(created_at=ts, retrieved_at=ra, account_id=uid)
                    )

            # Build one action row if we can classify
            atype = None
            target_acc = None
            if replied and replied != "None":
                atype = ActionType.COMMENT
                target_acc = replied
            elif reposted and reposted != "None":
                atype = ActionType.SHARE
                target_acc = reposted
            elif quoted and quoted != "None":
                atype = ActionType.QUOTE
                target_acc = quoted
            elif root_auth and root_auth != "None":
                # Thread context only; if you prefer to drop, just return here.
                target_acc = root_auth

            if originator and target_acc:
                outputs.append(
                    Actions(
                        created_at=ts,
                        retrieved_at=ra,
                        action_type=atype or ActionType.UNKNOWN,
                        originator_account_id=originator,
                        originator_post_id=None,
                        target_account_id=target_acc,
                        target_post_id=None,
                    )
                )
            return outputs

        # -------- LIKES (CSV/TSV row) ----------
        if kind == "likes":
            # schema: [liker_id, post_author_id, post_id, timestamp]
            row = record

            def _geti(i: int, name: str) -> Any:
                if isinstance(row, Mapping):
                    return row.get(i, row.get(name))
                else:
                    try:
                        return row[i]
                    except Exception:
                        return None

            liker = _as_int_str(_geti(0, "liker_id"))
            author = _as_int_str(_geti(1, "post_author_id"))
            post_id = _as_int_str(_geti(2, "post_id"))
            ts = _dt(_geti(3, "timestamp")) or ra

            # minimal accounts (again, Bluesky dumps often lack user profiles)
            for uid in (liker, author):
                if uid:
                    outputs.append(
                        Accounts(created_at=ts, retrieved_at=ra, account_id=uid)
                    )

            if liker and (author or post_id):
                outputs.append(
                    Actions(
                        created_at=ts,
                        retrieved_at=ra,
                        action_type=ActionType.UPVOTE,
                        originator_account_id=liker,
                        originator_post_id=None,
                        target_account_id=author,
                        target_post_id=post_id,
                    )
                )
            return outputs

        # -------- unknown kind: try best-effort post shape ----------
        # If a user hands us a single JSON file outside expected names, try to parse post-ish rows.
        post_id = _as_int_str(record.get("post_id") or record.get("id"))
        user_id = _as_int_str(record.get("user_id") or record.get("author"))
        if post_id and user_id:
            created_at = _dt(record.get("date") or record.get("created_at")) or ra
            body = _to_str(record.get("text"))
            outputs.append(
                Accounts(created_at=created_at, retrieved_at=ra, account_id=user_id)
            )
            outputs.append(
                Posts(
                    created_at=created_at,
                    retrieved_at=ra,
                    post_id=post_id,
                    account_id=user_id,
                    conversation_id=None,
                    body=body,
                    engagement_count=None,
                    location=None,
                )
            )
            for tag in extract_hashtags(body):
                outputs.append(
                    Entities(
                        created_at=created_at,
                        retrieved_at=ra,
                        entity_type=EntityType.HASHTAG,
                        account_id=user_id,
                        post_id=post_id,
                        body=tag.replace("#", ""),
                    )
                )
            for m in extract_mentions(body):
                outputs.append(
                    Entities(
                        created_at=created_at,
                        retrieved_at=ra,
                        entity_type=EntityType.USER_TAG,
                        account_id=user_id,
                        post_id=post_id,
                        body=m.replace("@", ""),
                    )
                )
            for u in extract_urls(body):
                outputs.append(
                    Entities(
                        created_at=created_at,
                        retrieved_at=ra,
                        entity_type=EntityType.LINK,
                        account_id=user_id,
                        post_id=post_id,
                        body=str(u),
                    )
                )
            for e in extract_emails(body):
                outputs.append(
                    Entities(
                        created_at=created_at,
                        retrieved_at=ra,
                        entity_type=EntityType.EMAIL,
                        account_id=user_id,
                        post_id=post_id,
                        body=e,
                    )
                )
        return outputs
