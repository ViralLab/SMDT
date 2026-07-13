from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from typing import Any, Iterable, List, Mapping, Optional, Dict, Tuple

from smdt.standardizers.base import Standardizer, SourceInfo
from smdt.store.models.accounts import Accounts
from smdt.store.models.posts import Posts
from smdt.store.models.entities import Entities, EntityType
from smdt.store.models.actions import Actions, ActionType

from smdt.standardizers.utils import (
    extract_emails,
    extract_hashtags,
    extract_mentions,
)

from atproto_core.cid import CID
import ast


def parse_ts(ts: Optional[str]) -> Optional[datetime]:
    """Parse an ISO timestamp into a UTC-aware datetime, or return None."""
    if not ts:
        return None
    # Accept both "...Z" and "+00:00"
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def reconstruct_key(
    entries: Iterable[Mapping[str, Any]], current_entry: Mapping[str, Any]
) -> str:
    """Reconstruct the full key using prefix length."""
    prefix_len = current_entry.get("p", 0)
    raw_key = current_entry.get("k", b"")

    if prefix_len == 0:
        return raw_key.decode("utf-8")

    entries_list = list(entries)
    entry_idx = entries_list.index(current_entry)
    if entry_idx == 0:
        raise ValueError("First entry cannot have non-zero prefix length")

    prev_entry = entries_list[entry_idx - 1]
    prev_key = prev_entry["k"].decode("utf-8")
    curr_suffix = raw_key.decode("utf-8")

    return prev_key[:prefix_len] + curr_suffix


def extract_text_urls(record: Mapping[str, Any]) -> List[str]:
    urls: List[str] = []
    for facet in record.get("facets") or []:
        for feat in facet.get("features") or []:
            if feat.get("$type") == "app.bsky.richtext.facet#link":
                uri = feat.get("uri")
                if isinstance(uri, str):
                    urls.append(uri)
    return urls


def find_created_at(obj: Any) -> Optional[str]:
    """Recursively search for a 'createdAt' key in nested dict/list structures."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "createdAt" and v is not None:
                return v
            result = find_created_at(v)
            if result is not None:
                return result
    elif isinstance(obj, list):
        for item in obj:
            result = find_created_at(item)
            if result is not None:
                return result
    return None


def get_record_created_at(record: Mapping[str, Any]) -> Optional[str]:
    """Find createdAt in record or nested subkeys."""
    created_at = record.get("value", {}).get("createdAt")
    if created_at is not None:
        return created_at

    created_at = find_created_at(record)
    if created_at is not None:
        print(f"Found createdAt in subkeys: {created_at}")
    return created_at


def safe_parse_created_at(record: Mapping[str, Any]) -> Optional[datetime]:
    """Get and parse createdAt from a record, or return None if missing/invalid."""
    created_at_str = get_record_created_at(record)
    if not created_at_str:
        return None
    return parse_ts(created_at_str)


def extract_target_from_uri(uri: Any) -> Tuple[str, str]:
    """
    Given a Bluesky URI like 'at://did:plc:xyz/app.bsky.feed.post/abc',
    return (account_id, post_id). Returns ('', '') on failure.
    """
    if not isinstance(uri, str) or "at://" not in uri:
        return "", ""
    tail = uri.split("at://", 1)[-1]
    parts = tail.split("/")
    if len(parts) < 2:
        return "", ""
    return parts[0], parts[-1]


@dataclass
class BlueSkyEFEStandardizer(Standardizer):
    name: str = "bluesky_efe"
    platform: str = "bluesky"

    last_cid2key: Optional[Dict[Any, str]] = None
    last_did: Optional[str] = None

    def standardize(self, input_record: Any) -> List[Any]:
        """
        Standardize a single input record into Accounts / Posts / Entities / Actions.

        Always returns a list (possibly empty).
        """
        record, src = input_record
        outputs: List[Any] = []
        retrieved_at: Optional[datetime] = None

        path = getattr(src, "path", "")
        # ===================== Account records: all_users =====================
        if "profile" in path:
            created_at_str = record.get("createdAt")
            created_at_dt = parse_ts(created_at_str)
            if created_at_dt is None:
                return outputs  # no createdAt; return empty list

            outputs.append(
                Accounts(
                    created_at=created_at_dt,
                    account_id=record.get("did"),
                    username=record.get("handle"),
                    profile_name=record.get("displayName"),
                    bio=record.get("description"),
                    location=None,
                    post_count=record.get("postsCount"),
                    friend_count=record.get("followsCount"),
                    follower_count=record.get("followersCount"),
                    is_verified=record.get("verified"),
                    profile_image_url=record.get("avatar"),
                    retrieved_at=retrieved_at,
                )
            )

        else:
            account_id_raw = None
            if "follow" in path:
                account_id_raw = path.split("/")[-1].replace("_follow.jsonl", "")
            elif "block" in path:
                account_id_raw = path.split("/")[-1].replace("_block.jsonl", "")
            elif "like" in path:
                account_id_raw = path.split("/")[-1].replace("_like.jsonl", "")
            elif "repost" in path:
                account_id_raw = path.split("/")[-1].replace("_repost.jsonl", "")
            elif "post" in path:
                account_id_raw = path.split("/")[-1].replace("_post.jsonl", "")
            if account_id_raw is None:
                print("BUG")
                input(path)
                return outputs
            account_id = account_id_raw.replace("did_plc_", "did:plc:")

            if (
                isinstance(record, dict)
                and record.get("value", {}).get("$type") is not None
            ):
                rtype = record.get("value", {}).get("$type")

                # -------------------------- Feed post --------------------------
                if rtype == "app.bsky.feed.post":
                    uri = record.get("uri", "")
                    post_id = uri.split("/")[-1]

                    created_at_dt = safe_parse_created_at(record)
                    if created_at_dt is None:
                        return outputs  # skip, but still return list

                    text = record.get("value", {}).get("text", "")
                    post_data = dict(
                        created_at=created_at_dt,
                        account_id=account_id,
                        post_id=post_id,
                        conversation_id=None,
                        body=text,
                        location=None,
                        retrieved_at=retrieved_at,
                    )

                    # ------------- Entities from text (emails / mentions / hashtags / links) -------------
                    emails = extract_emails(text)
                    mentions = extract_mentions(text)
                    hashtags = extract_hashtags(text)

                    for email in emails:
                        outputs.append(
                            Entities(
                                entity_type=EntityType.EMAIL,
                                body=email,
                                account_id=account_id,
                                post_id=post_id,
                                retrieved_at=retrieved_at,
                                created_at=created_at_dt,
                            )
                        )

                    for url in extract_text_urls(record.get("value", {})):
                        outputs.append(
                            Entities(
                                entity_type=EntityType.LINK,
                                body=url,
                                account_id=account_id,
                                post_id=post_id,
                                created_at=created_at_dt,
                                retrieved_at=retrieved_at,
                            )
                        )

                    for mention in mentions:
                        outputs.append(
                            Entities(
                                entity_type=EntityType.USER_TAG,
                                body=mention,
                                account_id=account_id,
                                post_id=post_id,
                                created_at=created_at_dt,
                                retrieved_at=retrieved_at,
                            )
                        )

                    for hashtag in hashtags:
                        outputs.append(
                            Entities(
                                entity_type=EntityType.HASHTAG,
                                body=hashtag,
                                account_id=account_id,
                                post_id=post_id,
                                created_at=created_at_dt,
                                retrieved_at=retrieved_at,
                            )
                        )

                    # ------------------------- Reply / quote / media types -------------------------
                    embed = record.get("value", {}).get("embed")
                    # Reply
                    if record.get("value", {}).get("reply"):
                        parent_uri = (
                            record.get("value", {})
                            .get("reply", {})
                            .get("parent", {})
                            .get("uri", "")
                        )
                        root_uri = (
                            record.get("value", {})
                            .get("reply", {})
                            .get("root", {})
                            .get("uri", "")
                        )

                        target_account_id, target_post_id = extract_target_from_uri(
                            parent_uri
                        )
                        conversation_id = extract_target_from_uri(root_uri)[1] or None

                        post_data["conversation_id"] = conversation_id
                        outputs.append(Posts(**post_data))

                        if target_account_id and target_post_id:
                            outputs.append(
                                Actions(
                                    originator_account_id=account_id,
                                    originator_post_id=post_id,
                                    target_account_id=target_account_id,
                                    target_post_id=target_post_id,
                                    action_type=ActionType.COMMENT,
                                    created_at=created_at_dt,
                                    retrieved_at=retrieved_at,
                                )
                            )

                    # Quote
                    elif embed and embed.get("$type") == "app.bsky.embed.record":
                        outputs.append(Posts(**post_data))
                        uri = embed.get("record", {}).get("uri", "")
                        target_account_id, target_post_id = extract_target_from_uri(uri)
                        if target_account_id and target_post_id:
                            outputs.append(
                                Actions(
                                    originator_account_id=account_id,
                                    originator_post_id=post_id,
                                    target_account_id=target_account_id,
                                    target_post_id=target_post_id,
                                    action_type=ActionType.QUOTE,
                                    created_at=created_at_dt,
                                    retrieved_at=retrieved_at,
                                )
                            )
                    # Post with images
                    elif embed and embed.get("$type") == "app.bsky.embed.images":
                        outputs.append(Posts(**post_data))

                        for image in embed.get("images", []):
                            outputs.append(
                                Entities(
                                    created_at=created_at_dt,
                                    entity_type=EntityType.IMAGE,
                                    account_id=account_id,
                                    post_id=post_id,
                                    body=json.dumps(image),
                                    retrieved_at=retrieved_at,
                                )
                            )

                        uri = embed.get("record", {}).get("uri", "")
                        target_account_id, target_post_id = extract_target_from_uri(uri)
                        if target_account_id and target_post_id:
                            outputs.append(
                                Actions(
                                    originator_account_id=account_id,
                                    originator_post_id=post_id,
                                    target_account_id=target_account_id,
                                    target_post_id=target_post_id,
                                    action_type=ActionType.QUOTE,
                                    created_at=created_at_dt,
                                    retrieved_at=retrieved_at,
                                )
                            )

                    # External (URL) quote
                    elif embed and embed.get("$type") == "app.bsky.embed.external":
                        outputs.append(Posts(**post_data))
                        uri = embed.get("record", {}).get("uri", "")
                        target_account_id, target_post_id = extract_target_from_uri(uri)
                        if target_account_id and target_post_id:
                            outputs.append(
                                Actions(
                                    originator_account_id=account_id,
                                    originator_post_id=post_id,
                                    target_account_id=target_account_id,
                                    target_post_id=target_post_id,
                                    action_type=ActionType.QUOTE,
                                    created_at=created_at_dt,
                                    retrieved_at=retrieved_at,
                                )
                            )

                    # Record with media (images / video + quote)
                    elif (
                        embed and embed.get("$type") == "app.bsky.embed.recordWithMedia"
                    ):
                        outputs.append(Posts(**post_data))

                        record_part = embed.get("record", {}).get("record", {}) or {}
                        uri = record_part.get("uri", "")
                        target_account_id, target_post_id = extract_target_from_uri(uri)
                        if target_account_id and target_post_id:
                            outputs.append(
                                Actions(
                                    originator_account_id=account_id,
                                    originator_post_id=post_id,
                                    target_account_id=target_account_id,
                                    target_post_id=target_post_id,
                                    action_type=ActionType.QUOTE,
                                    created_at=created_at_dt,
                                    retrieved_at=retrieved_at,
                                )
                            )

                        images = embed.get("media", {}).get("images", []) or []
                        for image in images:
                            outputs.append(
                                Entities(
                                    created_at=created_at_dt,
                                    entity_type=EntityType.IMAGE,
                                    account_id=account_id,
                                    post_id=post_id,
                                    body=json.dumps(image),
                                    retrieved_at=retrieved_at,
                                )
                            )

                        video = embed.get("media", {}).get("video")
                        if video:
                            outputs.append(
                                Entities(
                                    created_at=created_at_dt,
                                    entity_type=EntityType.VIDEO,
                                    account_id=account_id,
                                    post_id=post_id,
                                    body=json.dumps(video),
                                    retrieved_at=retrieved_at,
                                )
                            )

                    # Video embed
                    elif embed and embed.get("$type") == "app.bsky.embed.video":
                        outputs.append(Posts(**post_data))

                        video = embed.get("video")
                        if video is not None:
                            outputs.append(
                                Entities(
                                    created_at=created_at_dt,
                                    entity_type=EntityType.VIDEO,
                                    account_id=account_id,
                                    post_id=post_id,
                                    body=json.dumps(video),
                                    retrieved_at=retrieved_at,
                                )
                            )

                        uri = embed.get("record", {}).get("uri", "")
                        target_account_id, target_post_id = extract_target_from_uri(uri)
                        if target_account_id and target_post_id:
                            outputs.append(
                                Actions(
                                    originator_account_id=account_id,
                                    originator_post_id=post_id,
                                    target_account_id=target_account_id,
                                    target_post_id=target_post_id,
                                    action_type=ActionType.QUOTE,
                                    created_at=created_at_dt,
                                    retrieved_at=retrieved_at,
                                )
                            )

                    # Simple post (no reply / embed)
                    else:
                        outputs.append(Posts(**post_data))

                # -------------------------- Follow --------------------------
                elif rtype == "app.bsky.graph.follow":
                    created_at_dt = safe_parse_created_at(record)
                    if created_at_dt is None:
                        return outputs

                    target_account_id = record.get("value", {}).get("subject", "")
                    if target_account_id:
                        outputs.append(
                            Actions(
                                originator_account_id=account_id,
                                originator_post_id=None,
                                target_account_id=target_account_id,
                                target_post_id=None,
                                action_type=ActionType.FOLLOW,
                                created_at=created_at_dt,
                                retrieved_at=retrieved_at,
                            )
                        )
                # -------------------------- Block --------------------------
                elif rtype == "app.bsky.graph.block":
                    created_at_dt = safe_parse_created_at(record)
                    if created_at_dt is None:
                        return outputs

                    target_account_id = record.get("value", {}).get("subject", "")
                    if target_account_id:
                        outputs.append(
                            Actions(
                                originator_account_id=account_id,
                                originator_post_id=None,
                                target_account_id=target_account_id,
                                target_post_id=None,
                                action_type=ActionType.BLOCK,
                                created_at=created_at_dt,
                                retrieved_at=retrieved_at,
                            )
                        )
                # -------------------------- Like --------------------------
                elif rtype == "app.bsky.feed.like":
                    created_at_dt = safe_parse_created_at(record)
                    if created_at_dt is None:
                        return outputs

                    subject_uri = (
                        record.get("value", {}).get("subject", {}).get("uri", "")
                    )
                    target_account_id, target_post_id = extract_target_from_uri(
                        subject_uri
                    )
                    if target_account_id and target_post_id:
                        outputs.append(
                            Actions(
                                originator_account_id=account_id,
                                originator_post_id=None,
                                target_account_id=target_account_id,
                                target_post_id=target_post_id,
                                action_type=ActionType.UPVOTE,
                                created_at=created_at_dt,
                                retrieved_at=retrieved_at,
                            )
                        )

                # -------------------------- Repost --------------------------
                elif rtype == "app.bsky.feed.repost":
                    created_at_dt = safe_parse_created_at(record)
                    if created_at_dt is None:
                        return outputs

                    post_id = record.get("uri", "").split("/")[-1]

                    subject_uri = (
                        record.get("value", {}).get("subject", {}).get("uri", "")
                    )
                    target_account_id, target_post_id = extract_target_from_uri(
                        subject_uri
                    )
                    if target_account_id and target_post_id:
                        outputs.append(
                            Actions(
                                originator_account_id=account_id,
                                originator_post_id=post_id,
                                target_account_id=target_account_id,
                                target_post_id=target_post_id,
                                action_type=ActionType.SHARE,
                                created_at=created_at_dt,
                                retrieved_at=retrieved_at,
                            )
                        )

                # -------------------------- Other types we ignore --------------------------
                else:
                    # Not a type we care about; return empty or accumulated outputs
                    pass

        return outputs
