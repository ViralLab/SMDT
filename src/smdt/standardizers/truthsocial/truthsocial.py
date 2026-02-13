from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, List, Optional, Tuple, DefaultDict
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


def _dt(ts: Optional[str]) -> Optional[datetime]:
    """
    Parses a timestamp string from Truth Social dumps into a timezone-aware datetime object.
    Supports formats: "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d".
    """
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
    """
    Safely converts a value to an integer. returns None for 'nan' or empty strings.
    """
    try:
        if x is None or x == "" or str(x).lower() == "nan":
            return None
        v = int(x)
        return v
    except Exception:
        return None


def _bool_t(x: Any) -> bool:
    """
    Parses boolean values from Truth Social dumps ('t'/'f', 'true', '1', 'yes').
    """
    # Truth dumps used 't' / 'f'
    return str(x).lower() in ("t", "true", "1", "yes")


def _nz(x: Optional[int]) -> int:
    """
    Returns 0 if the input integer is None, otherwise returns the integer.
    """

    return 0 if x is None else x


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

    def standardize(self, input_record: Tuple[dict, SourceInfo]) -> List[Any]:
        """
        Standardizes a single input record into a list of schema models.
        Dispatch by member (file) name. 'row' is a dict produced by your csv reader.

        Args:
           input_record (Tuple[dict, SourceInfo]): A tuple containing the raw record and source information.

        Returns:
           List[Any]: A list of standardized models (Accounts, Posts, Entities, Actions) derived from the input record.
        """
        record, src = input_record
        outputs = []

        member = (src.member or "").lower()
        # ---------- USERS (emit Accounts; no join needed) ----------
        if member.endswith("users.tsv"):
            uid = str(record.get("id") or "").strip()
            if not uid:
                return outputs
            created_at = _dt(record.get("timestamp"))
            retrieved_at = _dt(record.get("time_scraped"))

            if created_at:
                outputs.append(
                    Accounts(
                        created_at=created_at,
                        retrieved_at=retrieved_at,
                        account_id=uid,
                        username=(record.get("username") or None),
                        profile_name=None,
                        bio=None,
                        location=None,
                        post_count=_int(record.get("tweet_count"))
                        or None,  # dataset may not have it
                        friend_count=(lambda x: None if _int(x) == -1 else _int(x))(
                            record.get("following_count")
                        ),
                        follower_count=(lambda x: None if _int(x) == -1 else _int(x))(
                            record.get("follower_count")
                        ),
                        is_verified=None,
                        profile_image_url=None,
                    )
                )
            return outputs

        # ---------- REPLIES (build replying_user -> replied_user map) ----------
        if member.endswith("replies.tsv"):
            ru = str(record.get("replying_user") or "").strip()
            tu = str(record.get("replied_user") or "").strip()
            if ru and tu:
                self.replied_user_by_replying_user[ru] = tu
            return outputs

        # ---------- TRUTHS (emit Posts; also fill external_id -> truth_id) ----------
        if member.endswith("truths.tsv"):
            tid = str(record.get("id") or "").strip()
            if not tid:
                return outputs
            author = str(record.get("author") or "").strip()
            if tid and author:
                self.post2account[tid] = author

            created_at = _dt(record.get("timestamp"))
            if created_at:
                self.truth2created_at[tid] = created_at

            retrieved_at = _dt(record.get("time_scraped"))
            text = (record.get("text") or "") or ""

            # map external_id -> truth_id (your existing code)
            ext = str(record.get("external_id") or "").strip()
            if ext:
                self.truthid_by_external[ext] = tid

            # handle retruths (shares)
            # TruthSocial dumps: "is_retruth"=='t' and "truth_retruthed" holds the original post id
            is_retruth = _bool_t(record.get("is_retruth"))
            target_post_id = (
                str(record.get("truth_retruthed") or "").strip() if is_retruth else ""
            )

            if is_retruth and author and target_post_id:
                # try to resolve target account now
                target_author = self.post2account.get(target_post_id)
                if target_author and created_at:
                    # we know original author -> emit immediately
                    outputs.append(
                        Actions(
                            created_at=created_at,
                            retrieved_at=retrieved_at,
                            action_type=ActionType.SHARE,
                            originator_account_id=author,
                            originator_post_id=tid,
                            target_account_id=target_author,
                            target_post_id=target_post_id,
                        )
                    )
                else:
                    # defer until we see the original post and learn its author
                    self._pending_retruths[target_post_id].append(
                        (created_at, retrieved_at, tid, author)
                    )

            # Post row (your existing emit)
            like = _int(record.get("like_count"))
            rt = _int(record.get("retruth_count"))
            rep = _int(record.get("reply_count"))
            if created_at:
                outputs.append(
                    Posts(
                        created_at=created_at,
                        retrieved_at=retrieved_at,
                        post_id=tid,
                        account_id=author or "",
                        conversation_id=None,
                        body=text,
                        like_count=_nz(like),
                        view_count=None,
                        share_count=_nz(rt),
                        comment_count=_nz(rep),
                        quote_count=None,
                        bookmark_count=None,
                        location=None,
                    )
                )

            pending = self._pending_retruths.pop(tid, [])
            if pending:
                for (
                    created_at,
                    retrieved_at,
                    origin_post_id,
                    origin_account_id,
                ) in pending:
                    if created_at:
                        outputs.append(
                            Actions(
                                created_at=created_at,
                                retrieved_at=retrieved_at,
                                action_type=ActionType.SHARE,
                                originator_account_id=origin_account_id,
                                originator_post_id=origin_post_id,
                                target_account_id=author or None,
                                target_post_id=tid,
                            )
                        )
            if created_at:
                emails = extract_emails(text)
                for email in emails:
                    outputs.append(
                        Entities(
                            created_at=created_at,
                            retrieved_at=retrieved_at,
                            entity_type=EntityType.EMAIL,
                            account_id=author or None,
                            post_id=tid,
                            body=email,
                        )
                    )

                mentions = extract_mentions(text)
                for mention in mentions:
                    outputs.append(
                        Entities(
                            created_at=created_at,
                            retrieved_at=retrieved_at,
                            entity_type=EntityType.USER_TAG,
                            account_id=author or None,
                            post_id=tid,
                            body=mention.replace("@", ""),
                        )
                    )

                urls = extract_urls(text)
                for url in urls:
                    outputs.append(
                        Entities(
                            created_at=created_at,
                            retrieved_at=retrieved_at,
                            entity_type=EntityType.LINK,
                            account_id=author or None,
                            post_id=tid,
                            body=url,
                        )
                    )
                hashtags = extract_hashtags(text)
                for tag in hashtags:
                    outputs.append(
                        Entities(
                            created_at=created_at,
                            retrieved_at=retrieved_at,
                            entity_type=EntityType.HASHTAG,
                            account_id=author or None,
                            post_id=tid,
                            body=tag.replace("#", ""),
                        )
                    )

                if _bool_t(record.get("is_reply")) and author:
                    target_user = self.replied_user_by_replying_user.get(author)
                    if target_user and created_at:
                        outputs.append(
                            Actions(
                                created_at=created_at,
                                retrieved_at=retrieved_at,
                                action_type=ActionType.COMMENT,
                                originator_account_id=author,
                                originator_post_id=tid,
                                target_account_id=target_user,
                                target_post_id=None,
                            )
                        )

            return outputs

        # ---------- QUOTES (needs truths external_id -> id map) ----------
        if member.endswith("quotes.tsv"):
            origin_post = str(record.get("quoting_truth") or "").strip()
            origin_user = str(record.get("quoting_user") or "").strip()
            target_user = str(record.get("quoted_user") or "").strip()
            ext_id = str(record.get("quoted_truth_external_id") or "").strip()

            if not (origin_post and origin_user and ext_id):
                return outputs

            external_map = getattr(self, "truthid_by_external", None) or getattr(
                self, "_external_id_map", None
            )
            if external_map is None:
                external_map = {}

            target_post = external_map.get(
                ext_id
            )  # present after truths.tsv or prepopulated

            created_at = (
                self.truth2created_at.get(origin_post)
                if hasattr(self, "truth2created_at")
                else None
            )
            retrieved_at = _dt(record.get("time_scraped"))
            if not created_at:
                created_at = retrieved_at

            if created_at:
                outputs.append(
                    Actions(
                        created_at=created_at,
                        retrieved_at=retrieved_at,
                        action_type=ActionType.QUOTE,
                        originator_account_id=origin_user or None,
                        originator_post_id=origin_post or None,
                        target_account_id=target_user or None,
                        target_post_id=target_post or None,
                    )
                )
            return outputs

        # ---------- FOLLOWS (pure edge; no post ids) ----------
        if member.endswith("follows.tsv"):
            follower = str(record.get("follower") or "").strip()
            followed = str(record.get("followed") or "").strip()
            retrieved_at = _dt(record.get("time_scraped"))
            created_at = _dt(record.get("time_scraped"))
            if follower and followed:
                if created_at:
                    outputs.append(
                        Actions(
                            created_at=created_at,
                            retrieved_at=retrieved_at,
                            action_type=ActionType.FOLLOW,
                            originator_account_id=follower,
                            originator_post_id=None,
                            target_account_id=followed,
                            target_post_id=None,
                        )
                    )
            return outputs
        return outputs
