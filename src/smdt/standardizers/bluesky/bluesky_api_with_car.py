from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from typing import Any, Iterable, List, Mapping, Optional

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

from atproto import CAR
from atproto_core.cid import CID
import ast


def parse_ts(ts: Optional[str]) -> datetime:
    if not ts:
        return None
    # Accept both "...Z" and "+00:00"
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def reconstruct_key(entries, current_entry):
    """Reconstruct the full key using prefix length"""
    if current_entry["p"] == 0:
        return current_entry["k"].decode("utf-8")

    # Find the previous entry in the array
    entry_idx = entries.index(current_entry)
    if entry_idx == 0:
        raise ValueError("First entry cannot have non-zero prefix length")

    prev_entry = entries[entry_idx - 1]
    prev_key = prev_entry["k"].decode("utf-8")
    curr_suffix = current_entry["k"].decode("utf-8")

    # Take prefix_len characters from previous key and append current suffix
    return prev_key[: current_entry["p"]] + curr_suffix


def get_mapper(CAR_PATH: str):
    records = {}
    car = {}
    # First pass: process MST nodes to build key -> CID mapping
    key_to_cid = {}
    with open(CAR_PATH, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue

            split_len = len(
                '{"enriched_cid": "bafyreifbpqliftfmnvh462gcm7yv7w24jbxvhidrgi2tg3wonv66mhppzq",'
            )

            left = line[:split_len].strip()
            rhs = line[split_len + 1 :].strip()
            rhs = "{" + rhs

            rhs = rhs.strip()
            try:
                block = ast.literal_eval(rhs)
                car[left] = block
            except Exception:
                continue

            if not (
                isinstance(block, dict)
                and "e" in block
                and isinstance(block["e"], list)
            ):
                continue

            for entry in block["e"]:
                try:
                    # Reconstruct the full key
                    full_key = reconstruct_key(block["e"], entry)

                    # Decode the CID from the 'v' field
                    value_cid = CID.decode(entry["v"])

                    key_to_cid[full_key] = value_cid
                except Exception as e:
                    print(f"Error processing entry: {e}")
                    continue

    # Second pass: match keys with their record content
    for record_key, record_cid in key_to_cid.items():
        try:
            record_content = car.get(record_cid)
            if record_content:
                records[record_key] = record_content
        except Exception as e:
            print(f"Error fetching record content: {e}")
            continue

    cid2keys = {v: k for k, v in key_to_cid.items()}
    return cid2keys


def extract_text_urls(record):
    urls = []
    for facet in record.get("facets", []) or []:
        for feat in facet.get("features", []) or []:
            if feat.get("$type") == "app.bsky.richtext.facet#link":
                uri = feat.get("uri")
                if isinstance(uri, str):
                    urls.append(uri)
    return urls


def find_created_at(obj):
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


def get_record_created_at(record):
    if record.get("createdAt") is None:
        created_at = find_created_at(record)
        if created_at is not None:
            print(f"Found createdAt in subkeys: {created_at}")
            return created_at
    else:
        return record.get("createdAt")


@dataclass
class BlueSkyAPICARStandardizer(Standardizer):
    name: str = "bluesky_api_with_CAR"

    last_cid2key = None
    last_did = None

    def standardize(self, input_record) -> List[Any]:
        retrieved_at = None
        record, src = input_record
        outputs = []
        if "all_users" in src.path:
            created_at = get_record_created_at(record)
            if created_at is None:
                return outputs
            created_at = parse_ts(created_at)
            if created_at is None:
                return outputs  # skip this record
            outputs.append(
                Accounts(
                    created_at=created_at,
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

        elif "cars" in src.path:
            account_id = src.path.split("/")[-1].replace(".jsonl", "")
            if self.last_did != account_id:
                self.last_cid2key = get_mapper(src.path)
                self.last_did = account_id

            account_id = account_id.replace("did-plc-", "did:plc:")
            if isinstance(record, dict) and "$type" in record:

                if record.get("$type") == "app.bsky.feed.post":
                    cid: Any | None = record.get("enriched_cid")  # CID
                    post_id = self.last_cid2key.get(cid)
                    if post_id:
                        post_id = post_id.split("/")[-1]
                    else:
                        return  # skip this record
                    created_at = get_record_created_at(record)

                    if created_at is None:
                        return  # skip this record

                    created_at = parse_ts(created_at)
                    if created_at is None:
                        return  # skip this record
                    post_data = dict(
                        created_at=created_at,
                        account_id=account_id,
                        post_id=post_id,
                        conversation_id=None,
                        body=record.get("text"),
                        location=None,
                        retrieved_at=retrieved_at,
                    )

                    emails = extract_emails(record.get("text", ""))
                    # urls = extract_urls(record.get("text", ""))
                    mentions = extract_mentions(record.get("text", ""))
                    hashtags = extract_hashtags(record.get("text", ""))
                    for email in emails:
                        outputs.append(
                            Entities(
                                entity_type=EntityType.EMAIL,
                                body=email,
                                account_id=account_id,
                                post_id=post_id,
                                retrieved_at=retrieved_at,
                                created_at=created_at,
                            )
                        )

                    for url in extract_text_urls(record):
                        outputs.append(
                            Entities(
                                entity_type=EntityType.LINK,
                                body=url,
                                account_id=account_id,
                                post_id=post_id,
                                created_at=created_at,
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
                                created_at=created_at,
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
                                created_at=created_at,
                                retrieved_at=retrieved_at,
                            )
                        )

                    if record.get("reply"):
                        uri = record.get("reply", {}).get("parent", {}).get("uri", "")
                        target_account_id = uri.split("at://")[-1].split("/")[0]
                        target_post_id = uri.split("/")[-1]
                        conversation_id = (
                            record.get("reply", {}).get("root", {}).get("uri", None)
                        )
                        if conversation_id:
                            conversation_id = conversation_id.split("/")[-1]

                        post_data["conversation_id"] = conversation_id
                        outputs.append(Posts(**post_data))

                        if target_account_id != "" and target_post_id != "":
                            outputs.append(
                                Actions(
                                    originator_account_id=account_id,
                                    originator_post_id=post_id,
                                    target_account_id=target_account_id,
                                    target_post_id=target_post_id,
                                    action_type=ActionType.COMMENT,
                                    created_at=created_at,
                                    retrieved_at=retrieved_at,
                                )
                            )

                    elif (
                        "embed" in record
                        and record["embed"].get("$type") == "app.bsky.embed.record"
                    ):
                        # Quote
                        outputs.append(Posts(**post_data))
                        target_account_id = (
                            record["embed"]
                            .get("record", {})
                            .get("uri", "")
                            .split("at://")[-1]
                            .split("/")[0]
                        )
                        target_post_id = (
                            record["embed"]
                            .get("record", {})
                            .get("uri", "")
                            .split("/")[-1]
                        )
                        if target_account_id != "" and target_post_id != "":
                            outputs.append(
                                Actions(
                                    originator_account_id=account_id,
                                    originator_post_id=post_id,
                                    target_account_id=target_account_id,
                                    target_post_id=target_post_id,
                                    action_type=ActionType.QUOTE,
                                    created_at=created_at,
                                    retrieved_at=retrieved_at,
                                )
                            )

                    elif (
                        "embed" in record
                        and record["embed"].get("$type") == "app.bsky.embed.images"
                    ):
                        # Post with images
                        outputs.append(Posts(**post_data))

                        for image in record["embed"].get("images", []):
                            outputs.append(
                                Entities(
                                    created_at=created_at,
                                    entity_type=EntityType.IMAGE,
                                    account_id=post_data.get("account_id"),
                                    post_id=post_data.get("post_id"),
                                    body=json.dumps(image),
                                    retrieved_at=retrieved_at,
                                )
                            )

                        target_account_id = (
                            record["embed"]
                            .get("record", {})
                            .get("uri", "")
                            .split("at://")[-1]
                            .split("/")[0]
                        )
                        target_post_id = (
                            record["embed"]
                            .get("record", {})
                            .get("uri", "")
                            .split("/")[-1]
                        )
                        if target_account_id != "" and target_post_id != "":
                            outputs.append(
                                Actions(
                                    originator_account_id=account_id,
                                    originator_post_id=post_id,
                                    target_account_id=target_account_id,
                                    target_post_id=target_post_id,
                                    action_type=ActionType.QUOTE,
                                    created_at=created_at,
                                    retrieved_at=retrieved_at,
                                )
                            )

                    elif (
                        "embed" in record
                        and record["embed"].get("$type") == "app.bsky.embed.external"
                    ):
                        outputs.append(Posts(**post_data))
                        target_account_id = (
                            record["embed"]
                            .get("record", {})
                            .get("uri", "")
                            .split("at://")[-1]
                            .split("/")[0]
                        )
                        target_post_id = (
                            record["embed"]
                            .get("record", {})
                            .get("uri", "")
                            .split("/")[-1]
                        )
                        if target_account_id != "" and target_post_id != "":
                            outputs.append(
                                Actions(
                                    originator_account_id=account_id,
                                    originator_post_id=post_id,
                                    target_account_id=target_account_id,
                                    target_post_id=target_post_id,
                                    action_type=ActionType.QUOTE,
                                    created_at=created_at,
                                    retrieved_at=retrieved_at,
                                )
                            )
                    elif (
                        "embed" in record
                        and record["embed"].get("$type")
                        == "app.bsky.embed.recordWithMedia"
                    ):
                        outputs.append(Posts(**post_data))

                        target_account_id = (
                            record["embed"]
                            .get("record", {})
                            .get("record", {})
                            .get("uri", "")
                            .split("at://")[-1]
                            .split("/")[0]
                        )
                        target_post_id = (
                            record["embed"]
                            .get("record", {})
                            .get("record", {})
                            .get("uri", "")
                            .split("/")[-1]
                        )
                        if target_account_id != "" and target_post_id != "":
                            outputs.append(
                                Actions(
                                    originator_account_id=account_id,
                                    originator_post_id=post_id,
                                    target_account_id=target_account_id,
                                    target_post_id=target_post_id,
                                    action_type=ActionType.QUOTE,
                                    created_at=created_at,
                                    retrieved_at=retrieved_at,
                                )
                            )

                        images = record["embed"].get("media", {}).get("images", [])
                        for image in images:
                            outputs.append(
                                Entities(
                                    created_at=created_at,
                                    entity_type=EntityType.IMAGE,
                                    account_id=post_data.get("account_id"),
                                    post_id=post_data.get("post_id"),
                                    body=json.dumps(image),
                                    retrieved_at=retrieved_at,
                                )
                            )

                        video = record["embed"].get("media", {}).get("video")
                        if video:
                            outputs.append(
                                Entities(
                                    created_at=created_at,
                                    entity_type=EntityType.VIDEO,
                                    account_id=post_data.get("account_id"),
                                    post_id=post_data.get("post_id"),
                                    body=json.dumps(video),
                                    retrieved_at=retrieved_at,
                                )
                            )

                    elif (
                        "embed" in record
                        and record["embed"].get("$type") == "app.bsky.embed.video"
                    ):
                        outputs.append(Posts(**post_data))
                        video = record["embed"].get("video")
                        outputs.append(
                            Entities(
                                created_at=created_at,
                                entity_type=EntityType.VIDEO,
                                account_id=post_data.get("account_id"),
                                post_id=post_data.get("post_id"),
                                body=json.dumps(video),
                                retrieved_at=retrieved_at,
                            )
                        )

                        target_account_id = (
                            record["embed"]
                            .get("record", {})
                            .get("uri", "")
                            .split("at://")[-1]
                            .split("/")[0]
                        )
                        target_post_id = (
                            record["embed"]
                            .get("record", {})
                            .get("uri", "")
                            .split("/")[-1]
                        )
                        if target_account_id != "" and target_post_id != "":
                            outputs.append(
                                Actions(
                                    originator_account_id=account_id,
                                    originator_post_id=post_id,
                                    target_account_id=target_account_id,
                                    target_post_id=target_post_id,
                                    action_type=ActionType.QUOTE,
                                    created_at=created_at,
                                    retrieved_at=retrieved_at,
                                )
                            )
                    else:
                        # Simple post
                        outputs.append(Posts(**post_data))
                elif record.get("$type") == "app.bsky.graph.follow":
                    created_at = get_record_created_at(record)
                    if created_at is None:
                        return outputs  # skip this record
                    created_at = parse_ts(created_at)
                    if created_at is None:
                        return outputs  # skip this record
                    target_account_id = (
                        record.get("subject", "").split("at://")[-1].split("/")[0]
                    )
                    if target_account_id != "":
                        outputs.append(
                            Actions(
                                originator_account_id=account_id,
                                originator_post_id=None,
                                target_account_id=target_account_id,
                                target_post_id=None,
                                action_type=ActionType.FOLLOW,
                                created_at=created_at,
                                retrieved_at=retrieved_at,
                            )
                        )
                elif record.get("$type") == "app.bsky.graph.block":
                    created_at = get_record_created_at(record)
                    if created_at is None:
                        return outputs  # skip this record
                    created_at = parse_ts(created_at)
                    if created_at is None:
                        return outputs  # skip this record
                    target_account_id = (
                        record.get("subject", "").split("at://")[-1].split("/")[0]
                    )
                    if target_account_id != "":
                        outputs.append(
                            Actions(
                                originator_account_id=account_id,
                                originator_post_id=None,
                                target_account_id=target_account_id,
                                target_post_id=None,
                                action_type=ActionType.BLOCK,
                                created_at=created_at,
                                retrieved_at=retrieved_at,
                            )
                        )

                elif record.get("$type") == "app.bsky.feed.like":
                    created_at = get_record_created_at(record)
                    if created_at is None:
                        return outputs  # skip this record
                    created_at = parse_ts(created_at)
                    if created_at is None:
                        return outputs  # skip this record
                    target_post_id = (
                        record.get("subject", {}).get("uri", "").split("/")[-1]
                    )
                    target_account_id = (
                        record.get("subject", {})
                        .get("uri", "")
                        .split("at://")[-1]
                        .split("/")[0]
                    )
                    if target_account_id != "" and target_post_id != "":
                        outputs.append(
                            Actions(
                                originator_account_id=account_id,
                                originator_post_id=None,
                                target_account_id=target_account_id,
                                target_post_id=target_post_id,
                                action_type=ActionType.UPVOTE,
                                created_at=created_at,
                                retrieved_at=retrieved_at,
                            )
                        )
                elif record.get("$type") == "app.bsky.feed.repost":
                    created_at = get_record_created_at(record)
                    if created_at is None:
                        return outputs  # skip this record
                    created_at = parse_ts(created_at)
                    if created_at is None:
                        return outputs  # skip this record
                    cid: Any | None = record.get("enriched_cid")  # CID
                    post_id = self.last_cid2key.get(cid)
                    if post_id:
                        post_id = post_id.split("/")[-1]
                    else:
                        return outputs  # skip this record
                    target_post_id = (
                        record.get("subject", {}).get("uri", "").split("/")[-1]
                    )
                    target_account_id = (
                        record.get("subject", {})
                        .get("uri", "")
                        .split("at://")[-1]
                        .split("/")[0]
                    )
                    if target_account_id != "" and target_post_id != "":
                        outputs.append(
                            Actions(
                                originator_account_id=account_id,
                                originator_post_id=post_id,
                                target_account_id=target_account_id,
                                target_post_id=target_post_id,
                                action_type=ActionType.SHARE,
                                created_at=created_at,
                                retrieved_at=retrieved_at,
                            )
                        )
                else:
                    # We don't care anymore
                    pass
            else:
                # We do not care anymore
                pass

        return outputs
