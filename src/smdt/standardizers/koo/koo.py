from __future__ import annotations
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
import re
from typing import Any, Iterable, List, Mapping, Optional

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
    PostEnrichments,
)
import glob
from pprint import pprint
import json


@dataclass
class KooStandardizer(Standardizer):
    name: str = "koo"

    def get_post_comment_entities(self, record):
        entities = []

        if record.get("createdAt") is None:
            return entities

        created_at = datetime.fromtimestamp(record.get("createdAt"))

        # extract mentions
        for mention in extract_mentions(record.get("title", "")):
            entity = Entities(
                entity_type=EntityType.USER_TAG,
                body=mention,
                account_id=str(record.get("id")),
                created_at=created_at,
            )
            entities.append(entity)

        # extract hashtags
        for hashtag in extract_hashtags(record.get("title", "")):
            entity = Entities(
                entity_type=EntityType.HASHTAG,
                body=hashtag.lstrip("#"),
                account_id=str(record.get("id")),
                created_at=created_at,
            )
            entities.append(entity)

        for url in extract_urls(record.get("title", "")):
            entity = Entities(
                entity_type=EntityType.LINK,
                body=url,
                account_id=str(record.get("id")),
                created_at=created_at,
            )
            entities.append(entity)

        return entities

    def standardize(self, input_record) -> List[Any]:
        record, src = input_record
        outputs = []

        if "users" in src.path:
            created_at = record.get("createdAt")  # e.g 1683852499
            title = record.get("title", "")
            description = record.get("description", "")

            full_bio = f"{title}\n\n{description}"

            if created_at:
                created_at = datetime.fromtimestamp(created_at)
                account = Accounts(
                    account_id=str(record.get("id")),
                    username=record.get("handle"),
                    bio=full_bio,
                    created_at=created_at,
                )
                outputs.append(account)
        if "posts" in src.path:
            created_at = record.get("createdAt")  # e.g 1683852499
            if created_at:
                created_at = datetime.fromtimestamp(created_at)

                post = Posts(
                    post_id=str(record.get("id")),
                    account_id=str(record.get("creatorId")),
                    body=record.get("title"),
                    created_at=created_at,
                )
                outputs.append(post)

                lang = src.member.split("/")[-1].split("_")[0]

                post_enrichment = PostEnrichments(
                    post_id=str(record.get("id")),
                    model_id="dataset:lang",
                    body={"lang": lang},
                    created_at=created_at,
                )
                outputs.append(post_enrichment)

                ents = self.get_post_comment_entities(record)
                outputs.extend(ents)

        if "comments" in src.path:
            created_at = record.get("createdAt")  # e.g 1683852499
            if created_at:
                created_at = datetime.fromtimestamp(created_at)

                post = Posts(
                    post_id=str(record.get("id")),
                    account_id=str(record.get("commenter_id")),
                    body=record.get("title"),
                    created_at=created_at,
                )
                outputs.append(post)
                lang = src.member.split("/")[-1].split("_")[0]

                post_enrichment = PostEnrichments(
                    post_id=str(record.get("id")),
                    model_id="dataset:lang",
                    body={"lang": lang},
                    created_at=created_at,
                )
                outputs.append(post_enrichment)

                ents = self.get_post_comment_entities(record)
                outputs.extend(ents)

        if "shares" in src.path:
            created_at = record.get("createdAt")  # e.g 1683852499
            if created_at:
                created_at = datetime.fromtimestamp(created_at)

                action = Actions(
                    action_type=ActionType.SHARE,
                    originator_account_id=record.get("sharer_id"),
                    target_account_id=record.get("creatorId"),
                    originator_post_id=record.get("id"),
                    created_at=created_at,
                )
                outputs.append(action)

        if "likes" in src.path:
            created_at = record.get("createdAt")  # e.g 1683852499
            if created_at:
                created_at = datetime.fromtimestamp(created_at)

                action = Actions(
                    action_type=ActionType.UPVOTE,
                    originator_account_id=record.get("sharer_id"),
                    target_account_id=record.get("creatorId"),
                    originator_post_id=record.get("id"),
                    created_at=created_at,
                )
                outputs.append(action)

        return outputs
