from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Tuple

from smdt.standardizers.base import Standardizer, SourceInfo
from smdt.store.models.accounts import Accounts
from smdt.store.models.posts import Posts
from smdt.store.models.entities import Entities, EntityType
from smdt.store.models.actions import Actions, ActionType
from smdt.store.models.communities import Communities, CommunityType

from smdt.standardizers.utils import (
    extract_emails,
    extract_mentions,
    extract_urls,
    extract_hashtags,
)


@dataclass
class PushShiftTelegramStandardizer(Standardizer):
    name: str = "telegram_standardizer"

    def standardize(self, input_record: Tuple[dict, SourceInfo]) -> List[Any]:
        record, src = input_record

        outputs = []

        if "channels" in src.path:
            retrieved_at = (
                datetime.fromtimestamp(record.get("retrieved_utc"))
                if record.get("retrieved_utc")
                else None
            )

            community_linked_group_ids = []
            community_id = None
            for chat in record.get("chats", []):
                created_at = chat.get("date")
                if created_at:
                    created_at = datetime.fromisoformat(created_at)
                    if chat.get("broadcast"):
                        community_id = (
                            str(chat.get("id")) if chat.get("id") is not None else None
                        )
                        community = Communities(
                            created_at=created_at,
                            community_type=chat.get("broadcast")
                            and CommunityType.CHANNEL
                            or CommunityType.GROUP,
                            community_username=chat.get("username"),
                            community_name=chat.get("title"),
                            community_id=community_id,
                            bio=record.get("full_chat", {}).get("about"),
                            member_count=record.get("full_chat", {}).get(
                                "participants_count"
                            ),
                            is_public=chat.get("username") is not None,
                            post_count=record.get("full_chat", {}).get("pinned_msg_id"),
                            owner_account_id=None,
                            profile_image_url=None,
                            retrieved_at=retrieved_at,
                        )
                        outputs.append(community)
                    else:
                        # group
                        community = Communities(
                            created_at=created_at,
                            community_type=CommunityType.GROUP,
                            community_username=chat.get("username"),
                            community_name=chat.get("title"),
                            community_id=(
                                str(chat.get("id"))
                                if chat.get("id") is not None
                                else None
                            ),
                            is_public=chat.get("username") is not None,
                            owner_account_id=None,
                            profile_image_url=None,
                            retrieved_at=retrieved_at,
                        )
                        community_linked_group_ids.append(community.community_id)
                        outputs.append(community)

            for cid in community_linked_group_ids:
                if community_id and cid:
                    action = Actions(
                        originator_community_id=community_id,
                        target_community_id=cid,
                        action_type=ActionType.LINK,
                        retrieved_at=retrieved_at,
                        created_at=retrieved_at,
                    )
                    outputs.append(action)

        if "accounts" in src.path:
            if record.get("retrieved_utc"):
                retrieved_at = int(record["retrieved_utc"])
                retrieved_at = datetime.fromtimestamp(retrieved_at)
                display_name = (
                    (record.get("first_name") or "")
                    + " "
                    + (record.get("last_name") or "")
                )
                account = Accounts(
                    account_id=(
                        str(record.get("id")) if record.get("id") is not None else None
                    ),
                    username=record.get("username"),
                    profile_name=display_name,
                    is_verified=record.get("verified"),
                    retrieved_at=retrieved_at,
                    created_at=retrieved_at,
                )

                outputs.append(account)

        if "messages" in src.path:
            # print(record)
            # input("WAIT")
            community_id = record.get("to_id", {}).get("channel_id")
            account_id = (
                str(record.get("from_id"))
                if record.get("from_id") is not None
                else None
            )
            if account_id:
                post = Posts(
                    post_id=(
                        str(record.get("id")) if record.get("id") is not None else None
                    ),
                    account_id=account_id,
                    body=record.get("message"),
                    created_at=(
                        datetime.fromisoformat(record.get("date"))
                        if record.get("date")
                        else None
                    ),
                    retrieved_at=(
                        datetime.fromtimestamp(record.get("retrieved_utc"))
                        if record.get("retrieved_utc")
                        else None
                    ),
                    community_id=(
                        str(community_id) if community_id is not None else None
                    ),
                )
                outputs.append(post)

                if record.get("reply_to_msg_id"):
                    print(record)
                    input("WAIT")
                    action = Actions(
                        created_at=(
                            datetime.fromisoformat(record.get("date"))
                            if record.get("date")
                            else None
                        ),
                        action_type=ActionType.COMMENT,
                        originator_account_id=account_id,
                        originator_post_id=(
                            str(record.get("id"))
                            if record.get("id") is not None
                            else None
                        ),
                        target_account_id=None,
                        target_post_id=(
                            str(record.get("reply_to_msg_id"))
                            if record.get("reply_to_msg_id") is not None
                            else None
                        ),
                        retrieved_at=(
                            datetime.fromtimestamp(record.get("retrieved_utc"))
                            if record.get("retrieved_utc")
                            else None
                        ),
                    )
                    outputs.append(action)

                if record.get("fwd_from"):
                    target_account_id = record.get("fwd_from", {}).get("from_id")
                    target_community_id = record.get("to_id", {}).get()
                    if target_account_id:
                        action = Actions(
                            created_at=(
                                datetime.fromisoformat(record.get("date"))
                                if record.get("date")
                                else None
                            ),
                            action_type=ActionType.SHARE,
                            originator_account_id=account_id,
                            originator_post_id=(
                                str(record.get("id"))
                                if record.get("id") is not None
                                else None
                            ),
                            target_account_id=(
                                str(target_account_id)
                                if target_account_id is not None
                                else None
                            ),
                            target_post_id=None,
                            originator_community_id=None,
                            target_community_id=(
                                str(target_community_id)
                                if target_community_id is not None
                                else None
                            ),
                            retrieved_at=(
                                datetime.fromtimestamp(record.get("retrieved_utc"))
                                if record.get("retrieved_utc")
                                else None
                            ),
                        )
                        outputs.append(action)

            urls = extract_urls(record.get("message", "") or "")
            for url in urls:
                entity = Entities(
                    entity_type=EntityType.LINK,
                    body=url,
                    created_at=(
                        datetime.fromisoformat(record.get("date"))
                        if record.get("date")
                        else None
                    ),
                    retrieved_at=(
                        datetime.fromtimestamp(record.get("retrieved_utc"))
                        if record.get("retrieved_utc")
                        else None
                    ),
                    account_id=account_id,
                    post_id=(
                        str(record.get("id")) if record.get("id") is not None else None
                    ),
                )
                outputs.append(entity)

            hashtags = extract_hashtags(record.get("message", "") or "")
            for hashtag in hashtags:
                entity = Entities(
                    entity_type=EntityType.HASHTAG,
                    body=hashtag.lstrip("#"),
                    created_at=(
                        datetime.fromisoformat(record.get("date"))
                        if record.get("date")
                        else None
                    ),
                    retrieved_at=(
                        datetime.fromtimestamp(record.get("retrieved_utc"))
                        if record.get("retrieved_utc")
                        else None
                    ),
                    account_id=account_id,
                    post_id=(
                        str(record.get("id")) if record.get("id") is not None else None
                    ),
                )
                outputs.append(entity)

            mentions = extract_mentions(record.get("message", "") or "")
            for mention in mentions:
                entity = Entities(
                    entity_type=EntityType.USER_TAG,
                    body=mention.lstrip("@"),
                    created_at=(
                        datetime.fromisoformat(record.get("date"))
                        if record.get("date")
                        else None
                    ),
                    retrieved_at=(
                        datetime.fromtimestamp(record.get("retrieved_utc"))
                        if record.get("retrieved_utc")
                        else None
                    ),
                    account_id=account_id,
                    post_id=(
                        str(record.get("id")) if record.get("id") is not None else None
                    ),
                )
                outputs.append(entity)

            emails = extract_emails(record.get("message", "") or "")
            for email in emails:
                entity = Entities(
                    entity_type=EntityType.EMAIL,
                    body=email,
                    created_at=(
                        datetime.fromisoformat(record.get("date"))
                        if record.get("date")
                        else None
                    ),
                    retrieved_at=(
                        datetime.fromtimestamp(record.get("retrieved_utc"))
                        if record.get("retrieved_utc")
                        else None
                    ),
                    account_id=account_id,
                    post_id=(
                        str(record.get("id")) if record.get("id") is not None else None
                    ),
                )
                outputs.append(entity)
        return outputs
