from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Tuple

from smdt.standardizers.base import Standardizer, SourceInfo
from smdt.store.models.communities import Communities, CommunityType
from smdt.store.models.accounts import Accounts
from smdt.store.models.posts import Posts
from smdt.store.models.entities import Entities, EntityType
from smdt.store.models.actions import Actions, ActionType

from smdt.standardizers.utils import (
    extract_emails,
    extract_mentions,
    extract_urls,
    extract_hashtags,
)

import ast


@dataclass
class VoatCoStandardizer(Standardizer):
    """
    Standardizer for Voat.co data.

    Dataset Paper Reference: https://arxiv.org/pdf/2201.05933
    
    Dataset Link: https://zenodo.org/records/5841668

    This class processes records from Voat.co exports, normalizing them into the standard
    schema models (Communities, Accounts, Posts, Entities, Actions).
    """

    name: str = "voatco_standardizer"
    subverse2ownerid = dict()

    def _get_z_count(self, value):
        """
        Helper to return null for negative counts, or the value itself if valid.
        """
        if value and value >= 0:
            return value
        return None

    def standardize(self, input_record: Tuple[dict, SourceInfo]) -> List[Any]:
        """
        Standardizes a single input record into a list of schema models.

        Args:
           input_record (Tuple[dict, SourceInfo]): A tuple containing the raw record and source information.

        Returns:
           List[Any]: A list of standardized models (Communities, Accounts, Posts, etc.) derived from the input record.
        """
        record, src = input_record
        outputs = []

        if "user" in src.member:
            created_at = record.get("reg_date")
            if created_at:
                created_at = datetime.fromisoformat(created_at)
                account = Accounts(
                    created_at=created_at,
                    account_id=record.get("user"),
                )
                outputs.append(account)

                subverses = ast.literal_eval(record.get("owns", "[]"))
                for subverse in subverses:
                    self.subverse2ownerid[subverse] = record.get("user")

        if "subverse_profiles" in src.member:
            created_at = record.get("date_created")
            if created_at:
                created_at = datetime.fromisoformat(created_at)
                community = Communities(
                    created_at=created_at,
                    community_type=CommunityType.CHANNEL,
                    community_id=record.get("subverse"),
                    community_username=record.get("subverse"),
                    bio=record.get("about"),
                    member_count=self._get_z_count(record.get("subscriber_count")),
                    owner_account_id=self.subverse2ownerid.get(record.get("subverse")),
                )

                outputs.append(community)

        if "submission" in src.member:
            date_ = record.get("date")
            time_ = record.get("time")
            if date_ and time_:
                created_at = datetime.fromisoformat(f"{date_}T{time_}")

                title = (record.get("title") or "").strip()
                body = (record.get("body") or "").strip()

                full_text = title + "\n\n" + body if body else title

                submission_id = (
                    str(record.get("submission_id"))
                    if record.get("submission_id") is not None
                    else None
                )
                if submission_id:
                    account_id = record.get("user")
                    if account_id == "None":
                        account_id = None

                    community = Communities(
                        community_type=CommunityType.GROUP,
                        community_id=submission_id,
                        created_at=created_at,
                        bio=full_text,
                        owner_account_id=account_id,
                    )
                    outputs.append(community)

                    action = Actions(
                        originator_account_id=account_id,
                        originator_community_id=submission_id,
                        target_community_id=record.get("subverse"),
                        created_at=created_at,
                        action_type=ActionType.LINK,
                    )
                    outputs.append(action)

                    urls = extract_urls(full_text)
                    for url in urls:
                        entity = Entities(
                            entity_type=EntityType.LINK,
                            body=url,
                            created_at=created_at,
                            account_id=account_id,
                            community_id=submission_id,
                        )
                        outputs.append(entity)

                    hashtags = extract_hashtags(full_text)
                    for hashtag in hashtags:
                        entity = Entities(
                            entity_type=EntityType.HASHTAG,
                            body=hashtag.lstrip("#"),
                            created_at=created_at,
                            account_id=account_id,
                            community_id=submission_id,
                        )
                        outputs.append(entity)

                    emails = extract_emails(full_text)
                    for email in emails:
                        entity = Entities(
                            entity_type=EntityType.EMAIL,
                            body=email,
                            created_at=created_at,
                            account_id=account_id,
                            community_id=submission_id,
                        )
                        outputs.append(entity)

                    mentions = extract_mentions(full_text)
                    for mention in mentions:
                        entity = Entities(
                            entity_type=EntityType.USER_TAG,
                            body=mention.lstrip("@"),
                            created_at=created_at,
                            account_id=account_id,
                            community_id=submission_id,
                        )
                        outputs.append(entity)

        if "comments" in src.member:
            time_ = record.get("time")
            date_ = record.get("date")
            if time_ and date_:
                created_at = datetime.fromisoformat(f"{date_}T{time_}")
                comment_id = (
                    str(record.get("comment_id"))
                    if record.get("comment_id") is not None
                    else None
                )

                subverse = record.get("subverse")
                root_submission = (
                    str(record.get("root_submission"))
                    if record.get("root_submission") is not None
                    else None
                )
                post = Posts(
                    post_id=comment_id,
                    account_id=record.get("user"),
                    body=record.get("body"),
                    created_at=created_at,
                    community_id=subverse,
                    conversation_id=root_submission,
                )

                outputs.append(post)

                urls = extract_urls(record.get("body") or "")
                for url in urls:
                    entity = Entities(
                        entity_type=EntityType.LINK,
                        body=url,
                        post_id=comment_id,
                        account_id=record.get("user"),
                        community_id=subverse,
                        created_at=created_at,
                    )
                    outputs.append(entity)

                hashtags = extract_hashtags(record.get("body") or "")
                for hashtag in hashtags:
                    entity = Entities(
                        entity_type=EntityType.HASHTAG,
                        body=hashtag.lstrip("#"),
                        post_id=comment_id,
                        account_id=record.get("user"),
                        community_id=subverse,
                        created_at=created_at,
                    )
                    outputs.append(entity)

                emails = extract_emails(record.get("body") or "")
                for email in emails:
                    entity = Entities(
                        entity_type=EntityType.EMAIL,
                        body=email,
                        post_id=comment_id,
                        account_id=record.get("user"),
                        community_id=subverse,
                        created_at=created_at,
                    )
                    outputs.append(entity)

                mentions = extract_mentions(record.get("body") or "")
                for mention in mentions:
                    entity = Entities(
                        entity_type=EntityType.USER_TAG,
                        body=mention.lstrip("@"),
                        post_id=comment_id,
                        account_id=record.get("user"),
                        community_id=subverse,
                        created_at=created_at,
                    )
                    outputs.append(entity)

        return outputs
