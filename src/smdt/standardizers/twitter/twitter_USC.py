from __future__ import annotations
from dataclasses import dataclass, field
import datetime
import json
from typing import Any, Iterable, Mapping, Optional, List, Tuple, DefaultDict
from collections import defaultdict

from httpx import post

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


from ast import literal_eval
from rich import print
import math
import numpy as np


def nan_to_none(val):
    # Handle actual NaN (float or numpy.nan)
    if isinstance(val, float) and math.isnan(val):
        return None
    # Handle string "nan"
    if isinstance(val, str) and val.lower() == "nan":
        return None
    return val


def map2int(value: Any) -> Optional[int]:
    value = nan_to_none(value)
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def sum_engagements(record: Mapping[str, Any]) -> Optional[int]:
    total = 0
    for field in ("likeCount", "replyCount", "quoteCount", "retweetCount"):
        try:
            val = int(record.get(field))
        except Exception:
            val = None
        if isinstance(val, int):
            total += val
    return total


def get_embedded_user(record: Mapping[str, Any]) -> Optional[Mapping[str, Any]]:
    user_str = record.get("user", "")
    try:
        user = eval(user_str, {"__builtins__": {}}, {"datetime": datetime})
        if isinstance(user, dict):
            return user
    except Exception:
        return None
    return None


@dataclass
class TwitterUSCStandardizer(Standardizer):
    name: str = "twitter_usc"

    def standardize(self, input_record) -> Iterable[Any]:
        """
        id or id_str => post_id
        text or rawContent => body


        hashtags = '[]' => entities.HASHTAG

        # mentionedUsers

        # engagement_count
        likeCount
        retweetCount
        quoteCount
        replyCount
        viewCount {}

        in_reply_to_status_id_str => target_post_id
        in_reply_to_user_id_str => target_account_id
        if reply:
            - user
                id or id_str => account_id



        location => location only name of the place, no geocoords
        """

        outputs = []
        record, src = input_record
        epoch = record.get("epoch")
        epoch = nan_to_none(epoch)
        if epoch:
            created_at = datetime.datetime.fromtimestamp(
                epoch, tz=datetime.timezone.utc
            )

        post_id = record.get("id_str") or record.get("id")
        conversation_id = str(record.get("conversationIdStr", "")) or str(
            record.get("conversationId", "")
        )
        body = record.get("text") or record.get("rawContent")
        # location = nan_to_none(record.get("location", None))

        url = nan_to_none(record.get("url", None))

        url_splits = url.split("/status/") if url else []
        if len(url_splits) == 2:
            username = url_splits[0].split("/")
            if len(username) > 0:
                username = username[-1]
            else:
                username = None
        else:
            username = None

        user = get_embedded_user(record)

        if user and isinstance(user, dict):

            account_id = user.get("id_str") or user.get("id")
            if user.get("created"):
                outputs.append(
                    Accounts(
                        created_at=user.get("created"),
                        account_id=account_id,
                        username=username,
                        profile_name=user.get("username", None),
                        bio=user.get("rawDescription", None),
                        location=None,
                        post_count=user.get("statusesCount", None),
                        friend_count=user.get("friendsCount", None),
                        follower_count=user.get("followersCount", None),
                        is_verified=user.get("verified", None),
                        profile_image_url=user.get("profileImageUrl", None),
                        retrieved_at=created_at,
                    )
                )

                view_count_obj = literal_eval(
                    nan_to_none(record.get("viewCount", "None"))
                )
                if isinstance(view_count_obj, dict):
                    view_count = map2int(view_count_obj.get("count"))
                outputs.append(
                    Posts(
                        created_at=created_at,
                        account_id=account_id,
                        post_id=post_id,
                        conversation_id=(
                            conversation_id if conversation_id != "" else None
                        ),
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

                mentions = literal_eval(record.get("mentionedUsers", "[]"))
                for mention in mentions:
                    mention = nan_to_none(mention)
                    if mention and nan_to_none(mention.get("screen_name")):
                        outputs.append(
                            Entities(
                                created_at=created_at,
                                entity_type=EntityType.USER_TAG,
                                account_id=account_id,
                                post_id=post_id,
                                body=mention.get("screen_name").replace("@", ""),
                                retrieved_at=created_at,
                            )
                        )
                hashtags = set(extract_hashtags(record.get("text"))) | set(
                    extract_hashtags(record.get("rawContent"))
                )
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

                urls = set(extract_urls(record.get("text"))) | set(
                    extract_urls(record.get("rawContent"))
                )
                for url in urls:
                    outputs.append(
                        Entities(
                            created_at=created_at,
                            entity_type=EntityType.LINK,
                            account_id=account_id,
                            post_id=post_id,
                            body=url,
                            retrieved_at=created_at,
                        )
                    )
                emails = set(extract_emails(record.get("text"))) | set(
                    extract_emails(record.get("rawContent"))
                )
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
            if record.get("in_reply_to_status_id_str") or record.get(
                "in_reply_to_status_id"
            ):
                outputs.append(
                    Actions(
                        created_at=created_at,
                        action_type=ActionType.COMMENT,
                        originator_account_id=account_id,
                        originator_post_id=post_id,
                        target_account_id=record.get("in_reply_to_user_id_str"),
                        target_post_id=record.get("in_reply_to_status_id_str"),
                        retrieved_at=created_at,
                    )
                )

            if record.get("retweetedUserID") or record.get("retweetedStatusID"):
                outputs.append(
                    Actions(
                        created_at=created_at,
                        action_type=ActionType.SHARE,
                        originator_account_id=account_id,
                        originator_post_id=post_id,
                        target_account_id=record.get("retweetedUserID"),
                        target_post_id=record.get("retweetedStatusID"),
                        retrieved_at=created_at,
                    )
                )

            if record.get("quotedTweet", False) == True:
                # This is just self declaration, actions cannot be made becuase there is no target info
                pass

        return outputs
