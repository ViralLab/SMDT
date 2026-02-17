from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Mapping, Optional, Tuple

from smdt.standardizers.base import Standardizer, SourceInfo
from smdt.store.models.accounts import Accounts
from smdt.store.models.posts import Posts
from smdt.store.models.entities import Entities, EntityType
from smdt.store.models.actions import Actions, ActionType

from smdt.standardizers.utils import extract_emails

from ast import literal_eval


def _sum_engagements(record: Mapping[str, Any]) -> Optional[int]:
    """
    Calculates the total engagement count (likes, replies, retruths) for a record.
    """
    total = 0
    for field in ("like_count", "reply_count", "retruth_count"):
        val = record.get(field)
        if isinstance(val, int):
            total += val
    return total


def _map2int(value: Any) -> Optional[int]:
    """
    Safely converts a value to an integer, returning None on failure.
    """
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


@dataclass
class TruthSocialUSCStandardizer(Standardizer):
    """
    Standardizer for Truth Social data (USC dataset).

    Dataset Paper Reference: https://arxiv.org/abs/2411.01330
    
    Dataset Link: https://github.com/kashish-s/TruthSocial_2024ElectionInitiative

    This class processes records from Truth Social exports, normalizing them into the standard
    schema models (Posts, Entities, etc.).
    """

    name: str = "truthsocial_usc"

    def standardize(self, input_record: Tuple[dict, SourceInfo]) -> Iterable[Any]:
        """
        Standardizes a single input record into a list of schema models.

        Args:
           input_record (Tuple[dict, SourceInfo]): A tuple containing the raw record and source information.

        Returns:
           Iterable[Any]: An iterable of standardized models (Posts, Entities, etc.) derived from the input record.

        Mapping Notes:
            associated_tags => Hashtags
            timestamp => created_at
            scraping Date => retrieved_at
            like_count, reply_count, retruth_count => engagement metrics
            tagged_accounts => Entities.USER_TAG
            author_username => Accounts.username
            status => Posts.body
            status_links => Entities.LINK
            media_urls => Entities.VIDEO
        """
        # print(record)
        # input()
        outputs = []
        record, src = input_record
        post_id = record.get("url", "").split("/")[-1]
        username = record.get("author_username", "").lower().replace("@", "")
        if username and post_id:
            account_id = f"TRUTHSOCIAL_USC_{username}"
            outputs.append(
                Posts(
                    account_id=account_id,
                    post_id=post_id,
                    conversation_id=None,
                    body=record["status"],
                    like_count=_map2int(record.get("like_count")),
                    view_count=None,
                    share_count=_map2int(record.get("retruth_count")),
                    comment_count=_map2int(record.get("reply_count")),
                    quote_count=None,
                    bookmark_count=None,
                    location=None,
                    created_at=datetime.fromisoformat(record["timestamp"]),
                    retrieved_at=datetime.fromisoformat(record["timestamp"]),
                )
            )
            hashtags = literal_eval(record.get("associated_tags", "[]"))
            for hashtag in hashtags:
                outputs.append(
                    Entities(
                        created_at=datetime.fromisoformat(record["timestamp"]),
                        entity_type=EntityType.HASHTAG,
                        account_id=account_id,
                        post_id=post_id,
                        body=hashtag.replace("#", "").lower(),
                        retrieved_at=datetime.fromisoformat(record["timestamp"]),
                    )
                )

            for email in extract_emails(record.get("status", "")):
                outputs.append(
                    Entities(
                        created_at=datetime.fromisoformat(record["timestamp"]),
                        entity_type=EntityType.EMAIL,
                        account_id=account_id,
                        post_id=post_id,
                        body=email,
                        retrieved_at=datetime.fromisoformat(record["timestamp"]),
                    )
                )

            mentions = literal_eval(record.get("tagged_accounts", "[]"))
            for mention in mentions:
                outputs.append(
                    Entities(
                        created_at=datetime.fromisoformat(record["timestamp"]),
                        entity_type=EntityType.USER_TAG,
                        account_id=account_id,
                        post_id=post_id,
                        body=mention.lower().replace("@", ""),
                        retrieved_at=datetime.fromisoformat(record["timestamp"]),
                    )
                )

            urls = literal_eval(record.get("status_links", "[]"))
            for url in urls:
                outputs.append(
                    Entities(
                        created_at=datetime.fromisoformat(record["timestamp"]),
                        entity_type=EntityType.LINK,
                        account_id=account_id,
                        post_id=post_id,
                        body=url,
                        retrieved_at=datetime.fromisoformat(record["timestamp"]),
                    )
                )

            media_urls = literal_eval(record.get("media_urls", "[]"))
            for media_url in media_urls:
                outputs.append(
                    Entities(
                        created_at=datetime.fromisoformat(record["timestamp"]),
                        entity_type=EntityType.VIDEO,
                        account_id=account_id,
                        post_id=post_id,
                        body=media_url,
                        retrieved_at=datetime.fromisoformat(record["timestamp"]),
                    )
                )

            outputs.append(
                Accounts(
                    account_id=account_id,
                    username=username,
                    created_at=datetime.fromisoformat(
                        record["timestamp"]
                    ),  # JUST TO BE ABLE TO ADD TO THE DB
                    retrieved_at=datetime.fromisoformat(record["timestamp"]),
                )
            )

            if record.get("is_reply", False):
                target_usernames = literal_eval(record.get("replying_to", "[]"))
                for target_username in target_usernames:
                    outputs.append(
                        Actions(
                            created_at=datetime.fromisoformat(record["timestamp"]),
                            action_type=ActionType.COMMENT,
                            originator_account_id=account_id,
                            originator_post_id=post_id,
                            target_account_id=f"TRUTHSOCIAL_USC_{target_username.lower().replace('@','')}",
                            target_post_id=None,
                            retrieved_at=datetime.fromisoformat(record["timestamp"]),
                        )
                    )
        return outputs
