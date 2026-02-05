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
    extract_urls,
    extract_hashtags,
)


def extract_reddit_mentions(text: str) -> Set[str]:
    mentions = set()
    words = text.split()
    for word in words:
        if word.startswith("u/") or word.startswith("/u/"):
            mention = word.split("/u/")[-1].strip(".,!?:;\"'()[]{}<>")
            mentions.add(mention)
    return mentions


@dataclass
class PushShiftRedditStandardizer(Standardizer):
    name: str = "reddit_standardizer"

    def standardize(self, input_record: Tuple[dict, SourceInfo]) -> List[Any]:
        record, src = input_record
        outputs = []

        if "reddit_subreddits" in src.path:
            created_utc = record.get("created_utc")
            if created_utc is not None:
                created_at = datetime.fromtimestamp(created_utc)
                retrieved_utc = record.get("retrieved_utc")
                if retrieved_utc is not None:
                    retrieved_at = datetime.fromtimestamp(retrieved_utc)

                member_count = None
                if record.get("subscribers") is not None:
                    if isinstance(record.get("subscribers"), int):
                        if record.get("subscribers") >= 0:
                            member_count = record.get("subscribers")
                community = Communities(
                    created_at=created_at,
                    community_type=CommunityType.CHANNEL,
                    community_id=(
                        str(record.get("name"))
                        if record.get("name") is not None
                        else None
                    ),  # e.g., t5_2qh33
                    community_username=record.get("url"),  # e.g., /r/AskReddit/
                    community_name=record.get("display_name"),
                    bio=record.get("public_description"),
                    is_public=record.get("subreddit_type") == "public",
                    member_count=member_count,
                    post_count=None,
                    owner_account_id=None,
                    profile_image_url=record.get("header_img"),
                    retrieved_at=retrieved_at,
                )

                outputs.append(community)

        if "authors" in src.path:
            created_utc = record.get("created_utc")
            if created_utc is not None:
                created_at = datetime.fromtimestamp(created_utc)
                account = Accounts(
                    created_at=created_at,
                    account_id=(
                        str(record.get("id")) if record.get("id") is not None else None
                    ),
                    username=record.get("author"),
                )
                outputs.append(account)

        if "submissions" in src.path:
            if created_utc is not None:
                created_at = datetime.fromtimestamp(created_utc)
                retrieved_utc = record.get("retrieved_utc")
                if retrieved_utc is not None:
                    retrieved_at = datetime.fromtimestamp(retrieved_utc)

                title = record.get("title", "")
                selftext = record.get("selftext", "")
                url = record.get("url", "")
                full_text = f"{title}\n\n{selftext}\n\n{url}"

                community = Communities(
                    community_type=CommunityType.GROUP,
                    community_id=(
                        "t3_" + str(record.get("id"))
                        if record.get("id") is not None
                        else None
                    ),  # e.g., t3_abcdef
                    community_username=record.get("subreddit"),
                    community_name=record.get("subreddit_name_prefixed"),
                    bio=full_text,
                    post_count=record.get("num_comments"),
                    owner_account_id=record.get("author"),
                    created_at=created_at,
                    retrieved_at=retrieved_at,
                )
                outputs.append(community)

                if (
                    record.get("id") is not None
                    and record.get("subreddit_id") is not None
                ):
                    action = Actions(
                        action_type=ActionType.LINK,
                        originator_community_id="t3_" + str(record.get("id")),
                        target_community_id=record.get("subreddit_id"),
                        created_at=created_at,
                        retrieved_at=retrieved_at,
                    )
                    outputs.append(action)

                urls = extract_urls(full_text)
                for url in urls:
                    entity = Entities(
                        entity_type=EntityType.LINK,
                        body=url,
                        community_id="t3_" + str(record.get("id")),
                        account_id=record.get("author"),
                        created_at=created_at,
                        retrieved_at=retrieved_at,
                    )
                    outputs.append(entity)

                for hashtag in extract_hashtags(full_text):
                    entity = Entities(
                        entity_type=EntityType.HASHTAG,
                        body=hashtag.lstrip("#"),
                        post_id="t3_" + str(record.get("id")),
                        account_id=record.get("author"),
                        created_at=created_at,
                        retrieved_at=retrieved_at,
                    )
                    outputs.append(entity)

        if "comments" in src.path:
            if created_utc is not None:
                created_at = datetime.fromtimestamp(created_utc)
                retrieved_utc = record.get("retrieved_on")
                if retrieved_utc is not None:
                    retrieved_at = datetime.fromtimestamp(retrieved_utc)
                post = Posts(
                    post_id=(
                        "t1_" + str(record.get("id"))
                        if record.get("id") is not None
                        else None
                    ),
                    account_id=record.get("author"),
                    body=record.get("body"),
                    created_at=created_at,
                    retrieved_at=retrieved_at,
                    community_id=record.get("subreddit_id"),
                    conversation_id=record.get("link_id"),
                    like_count=record.get("score"),
                )
                outputs.append(post)

                submission_id = record.get("link_id")  # e.g., t3_abcdef
                parent_id = record.get("parent_id")  # e.g., t1_ghijkl


                # target community is the submission
                action = Actions(
                    action_type=ActionType.COMMENT,
                    originator_post_id=post.post_id,
                    target_post_id=parent_id,
                    target_community_id=submission_id,
                    created_at=created_at,
                    retrieved_at=retrieved_at,
                )
                outputs.append(action)

                # target community is the subreddit
                action = Actions(
                    action_type=ActionType.COMMENT,
                    originator_post_id=post.post_id,
                    target_post_id=parent_id,
                    target_community_id=record.get("subreddit_id"),
                    created_at=created_at,
                    retrieved_at=retrieved_at,
                )
                outputs.append(action)

                emails = extract_emails(record.get("body", ""))
                for email in emails:
                    entity = Entities(
                        entity_type=EntityType.EMAIL,
                        body=email,
                        post_id=post.post_id,
                        created_at=created_at,
                        retrieved_at=retrieved_at,
                        account_id=post.account_id,
                    )
                    outputs.append(entity)

                mentions = extract_reddit_mentions(record.get("body", ""))
                for mention in mentions:
                    entity = Entities(
                        entity_type=EntityType.USER_TAG,
                        body=mention,
                        post_id=post.post_id,
                        account_id=post.account_id,
                        created_at=created_at,
                        retrieved_at=retrieved_at,
                    )
                    outputs.append(entity)

                urls = extract_urls(record.get("body", ""))
                for url in urls:
                    entity = Entities(
                        entity_type=EntityType.LINK,
                        body=url,
                        post_id=post.post_id,
                        account_id=post.account_id,
                        created_at=created_at,
                        retrieved_at=retrieved_at,
                    )
                    outputs.append(entity)

                for hashtag in extract_hashtags(record.get("body", "")):
                    entity = Entities(
                        entity_type=EntityType.HASHTAG,
                        body=hashtag.lstrip("#"),
                        post_id=post.post_id,
                        account_id=post.account_id,
                        created_at=created_at,
                        retrieved_at=retrieved_at,
                    )
                    outputs.append(entity)

        return outputs
