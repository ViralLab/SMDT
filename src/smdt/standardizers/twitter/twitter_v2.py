from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Tuple, List, Mapping, Optional

from smdt.standardizers.base import Standardizer, SourceInfo
from smdt.standardizers.utils import (
    extract_hashtags,
    extract_mentions,
    extract_urls,
    extract_emails,
)
from smdt.store.models import (
    Accounts,
    Posts,
    Entities,
    EntityType,
    Actions,
    ActionType,
)


def _dt(s: Optional[str]) -> Optional[datetime]:
    """
    Parses ISO 8601 strings (e.g., from Twitter v2) into timezone-aware UTC datetimes.
    """
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        d = datetime.fromisoformat(s)
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d
    except Exception:
        return None


def _retrieved_at(root: Mapping[str, Any]) -> datetime:
    """
    Extracts retrieval time from internal metadata (e.g. __twarc) or defaults to now-UTC.
    """
    ra = _dt(((root.get("__twarc") or {}).get("retrieved_at")))
    return ra or datetime.now(timezone.utc)


def _point_ewkt(place: Optional[Mapping[str, Any]]) -> Optional[str]:
    """
    Converts Twitter 'geo' objects to POINT EWKT strings (SRID=4326).
    Checks 'coordinates' first, then 'centroid'.
    """
    if not place:
        return None
    coords = (place.get("coordinates") or {}).get("coordinates")
    if isinstance(coords, (list, tuple)) and len(coords) == 2:
        try:
            lon, lat = float(coords[0]), float(coords[1])
            return f"SRID=4326;POINT({lon} {lat})"
        except Exception:
            return None
    geo = place.get("geo") or {}
    centroid = geo.get("centroid")
    if isinstance(centroid, (list, tuple)) and len(centroid) == 2:
        try:
            lon, lat = float(centroid[0]), float(centroid[1])
            return f"SRID=4326;POINT({lon} {lat})"
        except Exception:
            return None
    """
    Safely converts a value to int; returns None on failure.
    """
    return None


def map2int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _find_user_includes(
    rec: Mapping[str, Any], uid: Optional[str]
) -> Optional[Mapping[str, Any]]:
    """
    Looks up a user object by ID within the 'includes.users' expansion.
    """
    if not uid:
        return None
    for u in rec.get("includes", {}).get("users", []) or []:
        if u.get("id") == uid:
            return u
    return None


def _find_tweet_includes(
    rec: Mapping[str, Any], tid: Optional[str]
) -> Optional[Mapping[str, Any]]:
    """
    Looks up a tweet object by ID within the 'includes.tweets' expansion.
    """
    if not tid:
        return None
    for t in rec.get("includes", {}).get("tweets", []) or []:
        if t.get("id") == tid:
            return t
    return None


def _account_from_user(
    user: Mapping[str, Any], retrieved_at: datetime
) -> Optional[Accounts]:
    """
    Build Accounts only if we can parse the *account creation* time from the user object.
    Otherwise return None (skip).
    """
    created_account = _dt(user.get("created_at"))
    if not created_account:
        return None  # strict: don't emit partial account rows

    pm = user.get("public_metrics") or {}
    return Accounts(
        created_at=created_account,  # <-- true account creation time
        retrieved_at=retrieved_at,
        account_id=user.get("id"),
        username=user.get("username"),
        profile_name=user.get("name"),
        bio=user.get("description"),
        location=None,  # can be enriched later if you want
        post_count=pm.get("tweet_count"),
        friend_count=pm.get("following_count"),
        follower_count=pm.get("followers_count"),
        is_verified=(
            bool(user.get("verified")) if user.get("verified") is not None else None
        ),
        profile_image_url=user.get("profile_image_url"),
    )


@dataclass
class TwitterV2Standardizer(Standardizer):
    """
    Standardizer for Twitter API v2 data.

    This class processes records from Twitter API v2 exports (including expansions),
    normalizing them into the standard schema models (Accounts, Posts, Entities, Actions).
    """

    name: str = "twitter_v2"

    def standardize(
        self, input_record: Tuple[Mapping[str, Any], SourceInfo]
    ) -> List[Any]:
        """
        Standardizes a single input record into a list of schema models.

        Args:
           input_record (Tuple[Mapping[str, Any], SourceInfo]): A tuple containing the raw record and source information.

        Returns:
           List[Any]: A list of standardized models (Accounts, Posts, Actions, etc.) derived from the input record.
        """
        record, src = input_record

        outputs = []

        ra = _retrieved_at(record)
        tweet = record.get("data") or record

        # --- MAIN TWEET ---
        main_id = tweet.get("id")
        author_id = tweet.get("author_id")
        t_created = _dt(tweet.get("created_at")) or ra
        text = tweet.get("text") or ""

        # Try to resolve author user object with created_at
        author_user = _find_user_includes(record, author_id)
        acct_row = _account_from_user(author_user, ra) if author_user else None
        if acct_row:
            outputs.append(acct_row)
        # else: skip author account (strict), but we can still emit the post

        # Emit main post if we have an account_id for it (tweet always has author_id)
        if author_id:
            metrics = tweet.get("public_metrics") or {}
            loc = _point_ewkt(tweet.get("geo") or {})
            outputs.append(
                Posts(
                    created_at=t_created,
                    retrieved_at=ra,
                    post_id=main_id,
                    account_id=author_id,
                    conversation_id=tweet.get("conversation_id"),
                    body=text,
                    like_count=map2int(metrics.get("like_count")),
                    view_count=map2int(metrics.get("impression_count")),
                    share_count=map2int(metrics.get("retweet_count")),
                    comment_count=map2int(metrics.get("reply_count")),
                    quote_count=map2int(metrics.get("quote_count")),
                    bookmark_count=None,
                    location=loc,
                )
            )

        # Entities from text
        for tag in extract_hashtags(text):
            outputs.append(
                Entities(
                    created_at=t_created,
                    retrieved_at=ra,
                    entity_type=EntityType.HASHTAG,
                    account_id=author_id,
                    post_id=main_id,
                    body=tag.replace("#", ""),
                )
            )
        for m in extract_mentions(text):
            outputs.append(
                Entities(
                    created_at=t_created,
                    retrieved_at=ra,
                    entity_type=EntityType.USER_TAG,
                    account_id=author_id,
                    post_id=main_id,
                    body=m.replace("@", ""),
                )
            )
        for u in extract_urls(text):
            outputs.append(
                Entities(
                    created_at=t_created,
                    retrieved_at=ra,
                    entity_type=EntityType.LINK,
                    account_id=author_id,
                    post_id=main_id,
                    body=str(u),
                )
            )
        for e in extract_emails(text):
            outputs.append(
                Entities(
                    created_at=t_created,
                    retrieved_at=ra,
                    entity_type=EntityType.EMAIL,
                    account_id=author_id,
                    post_id=main_id,
                    body=e,
                )
            )

        # --- REFERENCED TWEETS (reply/quote/retweet) ---
        for ref in tweet.get("referenced_tweets") or []:
            r_type = ref.get("type")
            r_tweet = ref.get("tweet") or ref
            if (not r_tweet.get("author_id")) or (not r_tweet.get("created_at")):
                full = _find_tweet_includes(record, r_tweet.get("id"))
                if full:
                    r_tweet = full

            r_id = r_tweet.get("id")
            r_auth = r_tweet.get("author_id")
            if not r_id:
                continue

            # referenced account: only if we can get user.created_at
            r_user = r_tweet.get("author") or _find_user_includes(record, r_auth)
            r_acct = _account_from_user(r_user, ra) if r_user else None
            if r_acct:
                outputs.append(r_acct)

            # referenced post
            r_created = _dt(r_tweet.get("created_at")) or ra
            if r_auth:
                r_text = r_tweet.get("text") or ""
                r_loc = _point_ewkt(r_tweet.get("geo") or {})
                metrics = r_tweet.get("public_metrics") or {}
                outputs.append(
                    Posts(
                        created_at=r_created,
                        retrieved_at=ra,
                        post_id=r_id,
                        account_id=r_auth,
                        conversation_id=r_tweet.get("conversation_id"),
                        body=r_text,
                        like_count=map2int(metrics.get("like_count")),
                        view_count=map2int(metrics.get("impression_count")),
                        share_count=map2int(metrics.get("retweet_count")),
                        comment_count=map2int(metrics.get("reply_count")),
                        quote_count=map2int(metrics.get("quote_count")),
                        bookmark_count=None,
                        location=r_loc,
                    )
                )

            # action edge main -> referenced
            atype = (
                ActionType.COMMENT
                if r_type == "replied_to"
                else (
                    ActionType.QUOTE
                    if r_type == "quoted"
                    else ActionType.SHARE if r_type == "retweeted" else None
                )
            )
            if atype:
                outputs.append(
                    Actions(
                        created_at=t_created,
                        retrieved_at=ra,
                        action_type=atype,
                        originator_account_id=author_id,
                        originator_post_id=main_id,
                        target_account_id=r_auth,
                        target_post_id=r_id,
                    )
                )

        return outputs
