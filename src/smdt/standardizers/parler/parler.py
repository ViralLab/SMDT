from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, List, Mapping, Optional, Set, Tuple, Union

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


@dataclass
class ParlerStandardizer(Standardizer):
    name: str = "parler"

    def standardize(self, input_record: Tuple[dict, SourceInfo]) -> List[Any]:
        outputs = []
        record, src = input_record
        if "users" in src.path:
            joined = record.get("joined")  # e.g 20201111172810
            if joined:
                created_at = datetime.strptime(joined, "%Y%m%d%H%M%S").replace(
                    tzinfo=timezone.utc
                )
                if created_at:
                    account = Accounts(
                        account_id=record.get("id"),
                        bio=record.get("bio"),
                        username=record.get("username"),
                        is_verified=record.get("verified"),
                        follower_count=record.get("user_followers"),
                        friend_count=record.get("user_following"),
                        created_at=created_at,
                        post_count=record.get("posts"),
                    )
                    outputs.append(account)

        elif "posts" in src.path:
            if record.get("bodywithurls", "").strip() != "":
                created_at = record.get("createdAt")  # e.g 20200701023759
                if created_at:
                    created_at = datetime.strptime(created_at, "%Y%m%d%H%M%S").replace(
                        tzinfo=timezone.utc
                    )
                    post = Posts(
                        post_id=record.get("id"),
                        body=record.get("bodywithurls", ""),
                        created_at=created_at,
                        share_count=record.get("reposts"),
                        account_id=record.get("creator"),
                        like_count=record.get("upvotes"),
                        view_count=record.get("impressions"),
                    )

                    outputs.append(post)

                    urls = extract_urls(record.get("bodywithurls", ""))
                    for url in urls:
                        entity = Entities(
                            post_id=record.get("id"),
                            account_id=record.get("creator"),
                            body=url,
                            created_at=created_at,
                            entity_type=EntityType.LINK,
                        )
                        outputs.append(entity)

                    hashtags = extract_hashtags(record.get("bodywithurls", ""))
                    for hashtag in hashtags:
                        entity = Entities(
                            post_id=record.get("id"),
                            account_id=record.get("creator"),
                            body=hashtag,
                            created_at=created_at,
                            entity_type=EntityType.HASHTAG,
                        )
                        outputs.append(entity)

                    mentions = extract_mentions(record.get("bodywithurls", ""))
                    for mention in mentions:
                        entity = Entities(
                            post_id=record.get("id"),
                            account_id=record.get("creator"),
                            body=mention,
                            created_at=created_at,
                            entity_type=EntityType.USER_TAG,
                        )
                        outputs.append(entity)

                    emails = extract_emails(record.get("bodywithurls", ""))
                    for email in emails:
                        entity = Entities(
                            post_id=record.get("id"),
                            account_id=record.get("creator"),
                            body=email,
                            created_at=created_at,
                            entity_type=EntityType.EMAIL,
                        )
                        outputs.append(entity)

        return outputs
