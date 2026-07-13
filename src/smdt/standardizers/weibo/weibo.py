from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, List, Mapping, Optional, List, Tuple, DefaultDict
from collections import defaultdict

from smdt.standardizers.base import Standardizer, SourceInfo
from smdt.store.models.accounts import Accounts
from smdt.store.models.posts import Posts
from smdt.store.models.entities import Entities, EntityType
from smdt.store.models.actions import Actions, ActionType
import re

from smdt.standardizers.utils import (
    extract_emails,
)


def extract_hashtags(text):
    HASHTAG_RE = re.compile(r"#([^#\s]+)#")
    return HASHTAG_RE.findall(text)


def extract_mentions(text):
    WEIBO_MENTION_RE = re.compile(r"@([^\s@#：:，,。.!?()（）【】\[\]<>\"“”‘’]+)")
    return WEIBO_MENTION_RE.findall(text)


@dataclass
class WeiboStandardizer(Standardizer):
    name: str = "weibo_stream"
    platform: str = "weibo"

    def standardize(
        self, input_record: Tuple[Mapping[str, Any], SourceInfo]
    ) -> List[Any]:
        """
        Dispatch by member (file) name. 'row' is a dict produced by your csv reader.
        Yields: Accounts, Posts, Entities, Actions (append-only).
        """
        record, src = input_record
        outputs = []

        # set created_at as the begging of the time
        start_datetime = datetime(1970, 1, 1, tzinfo=timezone.utc)

        mblog_id = src.path.split("/")[
            -2
        ]  # assuming src ends with /<mblog_id>/new.json or comments.json
        file_basename = src.path.split("/")[-1]

        if file_basename == "new.json":
            user = record.get("user", {})
            user_created_at = user.get("account_created_at")
            if user_created_at:
                user_created_at = datetime.strptime(
                    user_created_at, "%a %b %d %H:%M:%S %z %Y"
                )
            outputs.append(
                Accounts(
                    created_at=user_created_at or start_datetime,
                    account_id=user.get("idstr"),
                    username=None,
                    profile_name=user.get("screen_name"),
                    bio=user.get("description"),
                    location=None,
                    post_count=user.get("statuses_count"),
                    friend_count=user.get("friends_count"),
                    follower_count=user.get("followers_count"),
                    is_verified=user.get("verified"),
                    profile_image_url=user.get("profile_image_url"),
                    retrieved_at=start_datetime,
                )
            )

            created_at = record.get("created_at")
            if created_at:
                created_at = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")

                outputs.append(
                    Posts(
                        created_at=created_at,
                        account_id=user.get("idstr"),
                        post_id=mblog_id,
                        conversation_id=mblog_id,
                        body=record.get("text_raw"),
                        like_count=record.get("attitudes_count"),
                        view_count=None,
                        share_count=record.get("reposts_count"),
                        comment_count=record.get("comments_count"),
                        quote_count=None,
                        bookmark_count=None,
                        location=None,
                        retrieved_at=start_datetime,
                    )
                )

                hashtags = extract_hashtags(record.get("text_raw", ""))
                for tag in hashtags:
                    outputs.append(
                        Entities(
                            account_id=user.get("idstr"),
                            post_id=record.get("idstr"),
                            body=tag.replace("#", ""),
                            entity_type=EntityType.HASHTAG,
                            created_at=created_at,
                            retrieved_at=start_datetime,
                        )
                    )

                emails = extract_emails(record.get("text_raw", ""))
                for email in emails:
                    outputs.append(
                        Entities(
                            account_id=user.get("idstr"),
                            post_id=record.get("idstr"),
                            body=email,
                            entity_type=EntityType.EMAIL,
                            created_at=created_at,
                            retrieved_at=start_datetime,
                        )
                    )

                for uo in record.get("url_objects") or []:
                    # url_objects vary; try common keys
                    url = (
                        uo.get("url")
                        or uo.get("short_url")
                        or uo.get("long_url")
                        or uo.get("ori_url")
                    )
                    if url:
                        outputs.append(
                            Entities(
                                account_id=user.get("idstr"),
                                post_id=record.get("idstr"),
                                body=url,
                                entity_type=EntityType.LINK,
                                created_at=created_at,
                                retrieved_at=start_datetime,
                            )
                        )
                mentions = extract_mentions(record.get("text_raw", ""))
                for mention in mentions:
                    outputs.append(
                        Entities(
                            account_id=user.get("idstr"),
                            post_id=record.get("idstr"),
                            body=mention.replace("@", ""),
                            entity_type=EntityType.USER_TAG,
                            created_at=created_at,
                            retrieved_at=start_datetime,
                        )
                    )

        elif file_basename == "comments.json":
            user = record.get("user", {})
            user_created_at = user.get("account_created_at")
            if user_created_at:
                user_created_at = datetime.strptime(
                    user_created_at, "%a %b %d %H:%M:%S %z %Y"
                )
            outputs.append(
                Accounts(
                    created_at=user_created_at or start_datetime,
                    account_id=user.get("idstr"),
                    username=None,
                    profile_name=user.get("screen_name"),
                    bio=user.get("description"),
                    location=None,
                    post_count=user.get("statuses_count"),
                    friend_count=user.get("friends_count"),
                    follower_count=user.get("followers_count"),
                    is_verified=user.get("verified"),
                    profile_image_url=user.get("profile_image_url"),
                    retrieved_at=start_datetime,
                )
            )
            if record.get("created_at"):
                created_at = datetime.strptime(
                    record.get("created_at"), "%a %b %d %H:%M:%S %z %Y"
                )

            outputs.append(
                Posts(
                    created_at=created_at,
                    account_id=user.get("idstr"),
                    post_id=record.get("idstr"),
                    conversation_id=mblog_id,
                    body=record.get("text_raw"),
                    like_count=record.get("attitudes_count"),
                    view_count=None,
                    share_count=record.get("reposts_count"),
                    comment_count=record.get("comments_count"),
                    quote_count=None,
                    bookmark_count=None,
                    location=None,
                    retrieved_at=start_datetime,
                )
            )

            outputs.append(
                Actions(
                    created_at=created_at,
                    action_type=ActionType.COMMENT,
                    originator_account_id=user.get("idstr"),
                    originator_post_id=record.get("idstr"),
                    target_account_id=None,
                    target_post_id=mblog_id,
                    retrieved_at=start_datetime,
                )
            )

            outputs.append(
                Actions(
                    created_at=created_at,
                    action_type=ActionType.COMMENT,
                    originator_account_id=user.get("idstr"),
                    originator_post_id=record.get("idstr"),
                    target_account_id=None,
                    target_post_id=mblog_id,
                    retrieved_at=start_datetime,
                )
            )

            hashtags = extract_hashtags(record.get("text_raw", ""))
            for tag in hashtags:
                outputs.append(
                    Entities(
                        account_id=user.get("idstr"),
                        post_id=record.get("idstr"),
                        body=tag.replace("#", ""),
                        entity_type=EntityType.HASHTAG,
                        created_at=created_at,
                        retrieved_at=start_datetime,
                    )
                )

            emails = extract_emails(record.get("text_raw", ""))
            for email in emails:
                outputs.append(
                    Entities(
                        account_id=user.get("idstr"),
                        post_id=record.get("idstr"),
                        body=email,
                        entity_type=EntityType.EMAIL,
                        created_at=created_at,
                        retrieved_at=start_datetime,
                    )
                )

            for uo in record.get("url_objects") or []:
                # url_objects vary; try common keys
                url = (
                    uo.get("url")
                    or uo.get("short_url")
                    or uo.get("long_url")
                    or uo.get("ori_url")
                )
                if url:
                    outputs.append(
                        Entities(
                            account_id=user.get("idstr"),
                            post_id=record.get("idstr"),
                            body=url,
                            entity_type=EntityType.LINK,
                            created_at=created_at,
                            retrieved_at=start_datetime,
                        )
                    )

            mentions = extract_mentions(record.get("text_raw", ""))
            for mention in mentions:
                outputs.append(
                    Entities(
                        account_id=user.get("idstr"),
                        post_id=record.get("idstr"),
                        body=mention.replace("@", ""),
                        entity_type=EntityType.USER_TAG,
                        created_at=created_at,
                        retrieved_at=start_datetime,
                    )
                )
        else:
            print(f"Unknown file type: {file_basename}")

        return outputs
