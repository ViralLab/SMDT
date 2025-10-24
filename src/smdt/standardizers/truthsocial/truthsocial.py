from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Optional, List, Tuple, DefaultDict
from collections import defaultdict

from smdt.standardizers.base import Standardizer, SourceInfo
from smdt.store.models.accounts import Accounts
from smdt.store.models.posts import Posts
from smdt.store.models.entities import Entities, EntityType
from smdt.store.models.actions import Actions, ActionType

from smdt.standardizers.utils import (
    extract_emails,
    extract_mentions,
    extract_hashtags,
    extract_urls,
)


# --------- tiny parsing helpers ---------


def _dt(ts: Optional[str]) -> Optional[datetime]:

    if not ts or ts == -1:
        return None
    ts = ts.strip()
    # Try common TruthSocial dump formats
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            d = datetime.strptime(ts, fmt)
            return d.replace(tzinfo=timezone.utc)
        except Exception:
            pass
    return None


def _int(x: Any) -> Optional[int]:
    try:
        if x is None or x == "" or str(x).lower() == "nan":
            return None
        v = int(x)
        return v
    except Exception:
        return None


def _bool_t(x: Any) -> bool:
    # Truth dumps used 't' / 'f'
    return str(x).lower() in ("t", "true", "1", "yes")


def _nz(x: Optional[int]) -> int:
    return 0 if x is None else x


def _sum3(a: Optional[int], b: Optional[int], c: Optional[int]) -> int:
    return _nz(a) + _nz(b) + _nz(c)


@dataclass
class TruthSocialStandardizer(Standardizer):
    """
    Streaming standardizer that uses small in-memory maps to replace pandas joins.
    Relies on member_order so caches are available before we hit dependent files.
    """

    name: str = "truth_social_stream"

    replied_user_by_replying_user: dict[str, str] = field(
        default_factory=dict
    )  # replying_user -> replied_user
    truthid_by_external: dict[str, str] = field(
        default_factory=dict
    )  # external_id -> truth_id
    post2account: dict[str, str] = field(default_factory=dict)  # post_id -> account_id

    # target_post_id -> list of deferred retruth actions to emit once we learn target's author
    _pending_retruths: DefaultDict[str, List[Tuple[datetime, datetime, str, str]]] = (
        field(default_factory=lambda: defaultdict(list))
    )

    truth2created_at: DefaultDict[str, str] = field(
        default_factory=lambda: defaultdict(str)
    )

    def standardize(self, row: Mapping[str, Any], src: SourceInfo) -> Iterable[Any]:
        """
        Dispatch by member (file) name. 'row' is a dict produced by your csv reader.
        Yields: Accounts, Posts, Entities, Actions (append-only).
        """
        member = (src.member or "").lower()
        # ---------- USERS (emit Accounts; no join needed) ----------
        if member.endswith("users.tsv"):
            uid = str(row.get("id") or "").strip()
            if not uid:
                return
            created_at = _dt(row.get("timestamp"))
            retrieved_at = _dt(row.get("time_scraped"))

            if created_at:
                yield Accounts(
                    created_at=created_at,
                    retrieved_at=retrieved_at,
                    account_id=uid,
                    username=(row.get("username") or None),
                    profile_name=None,
                    bio=None,
                    location=None,
                    post_count=_int(row.get("tweet_count"))
                    or None,  # dataset may not have it
                    friend_count=(lambda x: None if _int(x) == -1 else _int(x))(
                        row.get("following_count")
                    ),
                    follower_count=(lambda x: None if _int(x) == -1 else _int(x))(
                        row.get("follower_count")
                    ),
                    is_verified=None,
                    profile_image_url=None,
                )
            return

        # ---------- REPLIES (build replying_user -> replied_user map) ----------
        if member.endswith("replies.tsv"):
            ru = str(row.get("replying_user") or "").strip()
            tu = str(row.get("replied_user") or "").strip()
            if ru and tu:
                self.replied_user_by_replying_user[ru] = tu
            return

        # ---------- TRUTHS (emit Posts; also fill external_id -> truth_id) ----------
        if member.endswith("truths.tsv"):
            tid = str(row.get("id") or "").strip()
            if not tid:
                return
            author = str(row.get("author") or "").strip()
            if tid and author:
                self.post2account[tid] = author

            created_at = _dt(row.get("timestamp"))
            if created_at:
                self.truth2created_at[tid] = created_at

            retrieved_at = _dt(row.get("time_scraped"))
            text = (row.get("text") or "") or ""

            # map external_id -> truth_id (your existing code)
            ext = str(row.get("external_id") or "").strip()
            if ext:
                self.truthid_by_external[ext] = tid

            # --- NEW: handle retruths (shares) ---
            # TruthSocial dumps: "is_retruth"=='t' and "truth_retruthed" holds the original post id
            is_retruth = _bool_t(row.get("is_retruth"))
            target_post_id = (
                str(row.get("truth_retruthed") or "").strip() if is_retruth else ""
            )

            if is_retruth and author and target_post_id:
                # try to resolve target account now
                target_author = self.post2account.get(target_post_id)
                if target_author and created_at:
                    # we know original author -> emit immediately
                    yield Actions(
                        created_at=created_at,
                        retrieved_at=retrieved_at,
                        action_type=ActionType.SHARE,
                        originator_account_id=author,
                        originator_post_id=tid,
                        target_account_id=target_author,
                        target_post_id=target_post_id,
                    )
                else:
                    # defer until we see the original post and learn its author
                    self._pending_retruths[target_post_id].append(
                        (created_at, retrieved_at, tid, author)
                    )
            # --- END NEW ---

            # Post row (your existing emit)
            like = _int(row.get("like_count"))
            rt = _int(row.get("retruth_count"))
            rep = _int(row.get("reply_count"))
            if created_at:
                yield Posts(
                    created_at=created_at,
                    retrieved_at=retrieved_at,
                    post_id=tid,
                    account_id=author or "",
                    conversation_id=None,
                    body=text,
                    engagement_count=_sum3(like, rt, rep),
                    location=None,
                )

            # emit mentions/emails (your existing code) ...
            # emit reply->comment (your existing code) ...

            # --- NEW: flush any retruths that were waiting on THIS post's author ---
            pending = self._pending_retruths.pop(tid, [])
            if pending:
                for (
                    created_at,
                    retrieved_at,
                    origin_post_id,
                    origin_account_id,
                ) in pending:
                    if created_at:
                        yield Actions(
                            created_at=created_at,
                            retrieved_at=retrieved_at,
                            action_type=ActionType.SHARE,  # <- rename if needed
                            originator_account_id=origin_account_id,
                            originator_post_id=origin_post_id,
                            target_account_id=author or None,  # now known
                            target_post_id=tid,  # this truth
                        )
            # --- END NEW ---
            if created_at:
                emails = extract_emails(text)
                for email in emails:
                    yield Entities(
                        created_at=created_at,
                        retrieved_at=retrieved_at,
                        entity_type=EntityType.EMAIL,
                        account_id=author or None,
                        post_id=tid,
                        body=email,
                    )

                mentions = extract_mentions(text)
                for mention in mentions:
                    yield Entities(
                        created_at=created_at,
                        retrieved_at=retrieved_at,
                        entity_type=EntityType.USER_TAG,
                        account_id=author or None,
                        post_id=tid,
                        body=mention.replace("@", ""),
                    )

                urls = extract_urls(text)
                for url in urls:
                    yield Entities(
                        created_at=created_at,
                        retrieved_at=retrieved_at,
                        entity_type=EntityType.LINK,
                        account_id=author or None,
                        post_id=tid,
                        body=url,
                    )
                hashtags = extract_hashtags(text)
                for tag in hashtags:
                    yield Entities(
                        created_at=created_at,
                        retrieved_at=retrieved_at,
                        entity_type=EntityType.HASHTAG,
                        account_id=author or None,
                        post_id=tid,
                        body=tag.replace("#", ""),
                    )

                # If this truth is a reply, create COMMENT action using the reply map
                if _bool_t(row.get("is_reply")) and author:
                    target_user = self.replied_user_by_replying_user.get(author)
                    if target_user and created_at:
                        yield Actions(
                            created_at=created_at,
                            retrieved_at=retrieved_at,
                            action_type=ActionType.COMMENT,
                            originator_account_id=author,
                            originator_post_id=tid,
                            target_account_id=target_user,
                            target_post_id=None,
                        )

            return

        # ---------- QUOTES (needs truths external_id -> id map) ----------
        if member.endswith("quotes.tsv"):
            origin_post = str(row.get("quoting_truth") or "").strip()
            origin_user = str(row.get("quoting_user") or "").strip()
            target_user = str(row.get("quoted_user") or "").strip()
            ext_id = str(row.get("quoted_truth_external_id") or "").strip()

            if not (origin_post and origin_user and ext_id):
                return

            target_post = self.truthid_by_external.get(
                ext_id
            )  # present after truths.tsv

            created_at = self.truth2created_at.get(origin_post)
            retrieved_at = _dt(row.get("time_scraped"))
            if created_at:
                yield Actions(
                    created_at=created_at,
                    retrieved_at=retrieved_at,
                    action_type=ActionType.QUOTE,
                    originator_account_id=origin_user or None,
                    originator_post_id=origin_post or None,
                    target_account_id=target_user or None,
                    target_post_id=target_post or None,
                )
            return

        # ---------- FOLLOWS (pure edge; no post ids) ----------
        if member.endswith("follows.tsv"):
            follower = str(row.get("follower") or "").strip()
            followed = str(row.get("followed") or "").strip()
            retrieved_at = _dt(row.get("time_scraped"))
            created_at = _dt(row.get("time_scraped"))
            if follower and followed:
                if created_at:
                    yield Actions(
                        created_at=created_at,
                        retrieved_at=retrieved_at,
                        action_type=ActionType.FOLLOW,
                        originator_account_id=follower,
                        originator_post_id=None,
                        target_account_id=followed,
                        target_post_id=None,
                    )
            return
