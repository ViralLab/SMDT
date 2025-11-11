from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Optional, List, Tuple

from smdt.standardizers.base import Standardizer, SourceInfo
from smdt.store.models.accounts import Accounts
from smdt.store.models.posts import Posts
from smdt.store.models.entities import Entities, EntityType
from smdt.store.models.actions import Actions, ActionType

from smdt.standardizers.utils import (
    extract_emails,
    extract_hashtags,
    extract_urls,
)


def nan_to_none(val: Any) -> Any:
    """Normalize various NaN-like values to None/leave normal scalars as-is."""
    if val is None:
        return None
    # Fast path: ints / bools / normal strings
    if isinstance(val, (int, bool)):
        return val
    # Handle float NaN
    if isinstance(val, float):
        return None if math.isnan(val) else val
    # Handle numpy.nan without importing numpy (duck-typing on repr)
    if repr(val) == "nan":
        return None
    # Handle string "nan"
    if isinstance(val, str) and val.lower() == "nan":
        return None
    return val


def map2int(value: Any) -> Optional[int]:
    """Convert value to int if possible, returning None on failure or NaN-like."""
    value = nan_to_none(value)
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _dt(s: Optional[str]) -> Optional[datetime]:
    """Parse ISO8601-ish timestamp into timezone-aware UTC datetime."""
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


@dataclass
class TwitterUSCStandardizer(Standardizer):
    name: str = "twitter_usc"

    def standardize(
        self, input_record: Tuple[Mapping[str, Any], SourceInfo]
    ) -> Iterable[Any]:
        """
        Standardize a single Twitter USC row into Accounts, Posts, Entities, Actions.
        """
        outputs: List[Any] = []
        record, _src = input_record

        # ---------------- timestamps ----------------
        epoch = nan_to_none(record.get("epoch"))
        created_at: Optional[datetime] = None
        if epoch is not None:
            # ensure float in case it's string-ish
            created_at = datetime.fromtimestamp(float(epoch), tz=timezone.utc)

        # ---------------- basic post fields ----------------
        post_id = record.get("id_str") or record.get("id")
        conversation_id_raw = record.get("conversationIdStr", "") or record.get(
            "conversationId", ""
        )
        conversation_id = str(conversation_id_raw) if conversation_id_raw else None

        text = record.get("text") or ""
        raw_content = record.get("rawContent") or ""
        body = text or raw_content or None  # prefer text, then rawContent

        url = nan_to_none(record.get("url"))

        # username from url if present
        username: Optional[str] = None
        if url:
            url_splits = str(url).split("/status/")
            if len(url_splits) == 2:
                parts = url_splits[0].split("/")
                if parts:
                    username = parts[-1]

        # ---------------- user ----------------
        account_id = nan_to_none(record.get("user__id_str")) or nan_to_none(
            record.get("user__id")
        )
        if isinstance(account_id, (float, int)):
            # Make sure it's a clean string without ".0"
            account_id = str(int(account_id))  # type: ignore[assignment]

        user_created = record.get("user__created")
        if isinstance(user_created, str):
            user_created = _dt(user_created)

        # If created_at is missing, fall back to user_created if available
        if created_at is None and isinstance(user_created, datetime):
            created_at = user_created

        # ---------------- Accounts ----------------
        if user_created:
            outputs.append(
                Accounts(
                    created_at=user_created,
                    account_id=account_id,
                    username=username,
                    profile_name=record.get("user__username"),
                    bio=record.get("user__rawDescription"),
                    location=None,
                    post_count=nan_to_none(record.get("user__statusesCount")),
                    friend_count=nan_to_none(record.get("user__friendsCount")),
                    follower_count=nan_to_none(record.get("user__followersCount")),
                    is_verified=record.get("user__verified"),
                    profile_image_url=record.get("user__profileImageUrl"),
                    retrieved_at=created_at,
                )
            )

        # ---------------- Posts ----------------
        view_count = map2int(record.get("viewCount__count"))

        outputs.append(
            Posts(
                created_at=created_at,
                account_id=account_id,
                post_id=post_id,
                conversation_id=conversation_id,
                body=body,
                like_count=map2int(record.get("likeCount")),
                view_count=view_count,
                share_count=map2int(record.get("retruthCount")),
                comment_count=map2int(record.get("replyCount")),
                quote_count=map2int(record.get("quoteCount")),
                bookmark_count=None,
                location=None,
                retrieved_at=created_at,
            )
        )

        # ---------------- Entities (mentions / hashtags / URLs / emails) ----------------
        msnames_raw = record.get("mentionedUsers__screen_names", "")
        msnames_raw = nan_to_none(msnames_raw)
        if isinstance(msnames_raw, str) and msnames_raw:
            msnames = [m for m in msnames_raw.split(",") if m]
        else:
            msnames = []

        for mention in msnames:
            outputs.append(
                Entities(
                    created_at=created_at,
                    entity_type=EntityType.USER_TAG,
                    account_id=account_id,
                    post_id=post_id,
                    body=str(mention).replace("@", ""),
                    retrieved_at=created_at,
                )
            )

        # For text-based entities, avoid running regex twice: use combined text
        combined_text = " ".join(p for p in (text, raw_content) if p)

        if combined_text:
            hashtags = set(extract_hashtags(combined_text))
            for hashtag in hashtags:
                outputs.append(
                    Entities(
                        created_at=created_at,
                        entity_type=EntityType.HASHTAG,
                        account_id=account_id,
                        post_id=post_id,
                        body=hashtag.replace("#", ""),
                        retrieved_at=created_at,
                    )
                )

            urls = set(extract_urls(combined_text))
            for u in urls:
                outputs.append(
                    Entities(
                        created_at=created_at,
                        entity_type=EntityType.LINK,
                        account_id=account_id,
                        post_id=post_id,
                        body=u,
                        retrieved_at=created_at,
                    )
                )

            emails = set(extract_emails(combined_text))
            for email in emails:
                outputs.append(
                    Entities(
                        created_at=created_at,
                        entity_type=EntityType.EMAIL,
                        account_id=account_id,
                        post_id=post_id,
                        body=email,
                        retrieved_at=created_at,
                    )
                )

        # ---------------- Actions ----------------
        originator_account_id = account_id
        originator_post_id = post_id

        in_reply_to_status = nan_to_none(
            record.get("in_reply_to_status_id_str")
        ) or nan_to_none(record.get("in_reply_to_status_id"))

        if in_reply_to_status and (originator_account_id or originator_post_id):
            outputs.append(
                Actions(
                    created_at=created_at,
                    action_type=ActionType.COMMENT,
                    originator_account_id=originator_account_id,
                    originator_post_id=originator_post_id,
                    target_account_id=nan_to_none(
                        record.get("in_reply_to_user_id_str")
                    ),
                    target_post_id=in_reply_to_status,
                    retrieved_at=created_at,
                )
            )

        retweeted_user_id = nan_to_none(record.get("retweetedUserID"))
        retweeted_status_id = nan_to_none(record.get("retweetedStatusID"))

        # (retweeted_user_id or retweeted_status_id) must hold AND we must have an originator
        if (retweeted_user_id or retweeted_status_id) and (
            originator_account_id or originator_post_id
        ):
            outputs.append(
                Actions(
                    created_at=created_at,
                    action_type=ActionType.SHARE,
                    originator_account_id=originator_account_id,
                    originator_post_id=originator_post_id,
                    target_account_id=retweeted_user_id,
                    target_post_id=retweeted_status_id,
                    retrieved_at=created_at,
                )
            )

        # quotedTweet handling left unchanged
        # if record.get("quotedTweet", False) is True:
        #     pass

        return outputs
