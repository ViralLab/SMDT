from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Set, Tuple

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


@dataclass
class ScoredStandardizer(Standardizer):    
    """
    Standardizer for Reddit data from PushShift.

        
    Dataset Paper Reference: https://arxiv.org/abs/2405.10233
    
    Dataset Link: https://zenodo.org/records/10516043

    This class processes records from PushShift Reddit exports, normalizing them into the standard
    schema models (Communities, Accounts, Posts, Entities, Actions).
    """
    name: str = "scored_standardizer"

    def standardize(self, input_record: Tuple[dict, SourceInfo]) -> List[Any]:
        record, src = input_record

        outputs = []

        if "submissions" in src.path:

            if (
                record.get("created") is not None
                and record.get("author") is not None
                and record.get("author", "").strip() != ""
            ):
                created_at = datetime.fromtimestamp(record.get("created") / 1000.0)
                score_up = record.get("score_up", 0)
                score_down = record.get("score_down", 0)
                community = str(record.get("community"))

                title = record.get("title", "")
                link = record.get("link", "")
                full_text = f"{title}\n\n{link}"

                community = Communities(
                    community_id=str(record.get("community")),
                    community_username=str(record.get("community")),
                    community_type=CommunityType.GROUP,
                    owner_account_id=str(record.get("author")),
                    bio=full_text,
                    created_at=created_at,
                )
                outputs.append(community)

                for email in extract_emails(full_text):
                    entity = Entities(
                        entity_type=EntityType.EMAIL,
                        body=email,
                        community_id=community.community_id,
                        account_id=community.owner_account_id,
                        created_at=created_at,
                    )
                    outputs.append(entity)

                for mention in extract_mentions(full_text):
                    entity = Entities(
                        entity_type=EntityType.USER_TAG,
                        body=mention.lstrip("@"),
                        community_id=community.community_id,
                        account_id=community.owner_account_id,
                        created_at=created_at,
                    )
                    outputs.append(entity)

                for url in extract_urls(full_text):
                    entity = Entities(
                        entity_type=EntityType.LINK,
                        body=url,
                        community_id=community.community_id,
                        account_id=community.owner_account_id,
                        created_at=created_at,
                    )
                    outputs.append(entity)

                for hashtag in extract_hashtags(full_text):
                    entity = Entities(
                        entity_type=EntityType.HASHTAG,
                        body=hashtag.lstrip("#"),
                        community_id=community.community_id,
                        account_id=community.owner_account_id,
                        created_at=created_at,
                    )
                    outputs.append(entity)

        if "comments" in src.path:
            if (
                record.get("created") is not None
                and record.get("author") is not None
                and record.get("author", "").strip() != ""
            ):
                created_at = datetime.fromtimestamp(record.get("created") / 1000.0)
                score_up = record.get("score_up", 0)
                score_down = record.get("score_down", 0)

                body = record.get("raw_content", "")
                account_id = str(record.get("author"))
                post_id = str(record.get("uuid"))
                community = record.get("community")

                post = Posts(
                    post_id=post_id,
                    account_id=account_id,
                    body=body,
                    created_at=created_at,
                    like_count=score_up,
                    dislike_count=score_down,
                    community_id=community,
                )
                outputs.append(post)

                for mention in extract_mentions(body):
                    entity = Entities(
                        entity_type=EntityType.USER_TAG,
                        body=mention.lstrip("@"),
                        post_id=post.post_id,
                        account_id=account_id,
                        created_at=created_at,
                    )
                    outputs.append(entity)

                for url in extract_urls(body):
                    entity = Entities(
                        entity_type=EntityType.LINK,
                        body=url,
                        post_id=post.post_id,
                        account_id=account_id,
                        created_at=created_at,
                    )
                    outputs.append(entity)

                for hashtag in extract_hashtags(body):
                    entity = Entities(
                        entity_type=EntityType.HASHTAG,
                        body=hashtag.lstrip("#"),
                        post_id=post.post_id,
                        account_id=account_id,
                        created_at=created_at,
                    )
                    outputs.append(entity)

                action = Actions(
                    originator_account_id=account_id,
                    originator_post_id=post_id,
                    action_type=ActionType.COMMENT,
                    target_community_id=community,
                    created_at=created_at,
                )
                outputs.append(action)

        return outputs
