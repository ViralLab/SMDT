from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Set, Tuple

from smdt.standardizers.base import Standardizer, SourceInfo
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
class GabStandardizer(Standardizer):
    """
    Standardizer for Gab data.
    """

    name: str = "gab_standardizer"

    def _get_account_info(self, record: dict) -> Accounts:
        """
        Extract account information from a record.

        Args:
            record: The raw Gab record.

        Returns:
            An Accounts model instance or None.
        """

        if record.get("retrieved_utc"):
            retrieved_at = datetime.fromtimestamp(int(record["retrieved_utc"]))

            account = Accounts(
                account_id=(
                    str(record.get("user", {}).get("id"))
                    if record.get("user", {}).get("id")
                    else None
                ),
                username=record.get("user", {}).get("username"),
                profile_name=record.get("user", {}).get("name"),
                profile_image_url=record.get("user", {}).get("picture_url"),
                is_verified=record.get("user", {}).get("verified"),
                created_at=retrieved_at,
                retrieved_at=retrieved_at,
            )

            return account

    def _get_z_count(self, value):
        """
        Helper to ensure count values are non-negative.

        Args:
            value: The count value to check.

        Returns:
            The value if non-negative, else None.
        """
        if value and value >= 0:
            return value
        return None

    def _get_post_info(self, record: dict) -> Posts:
        """
        Extract post information from a record.

        Args:
            record: The raw Gab record.

        Returns:
            A Posts model instance or None.
        """
        if record.get("created_utc"):
            timestamp = int(record["created_utc"])
            created_at = datetime.fromtimestamp(timestamp)
            retrieved_at = datetime.fromtimestamp(int(record["retrieved_utc"]))

            post = Posts(
                account_id=(
                    str(record.get("user", {}).get("id"))
                    if record.get("user", {}).get("id")
                    else None
                ),
                post_id=str(record.get("id")) if record.get("id") else None,
                body=record.get("body"),
                conversation_id=(
                    str(record.get("conversation_parent_id"))
                    if record.get("conversation_parent_id")
                    else None
                ),
                like_count=self._get_z_count(record.get("like_count")),
                dislike_count=self._get_z_count(record.get("dislike_count")),
                share_count=self._get_z_count(record.get("repost_count")),
                comment_count=self._get_z_count(record.get("reply_count")),
                created_at=created_at,
                retrieved_at=retrieved_at,
            )

        return post

    def _get_action_info(self, record: dict) -> List[Actions]:
        """
        Extract action information (comments, quotes) from a record.

        Args:
            record: The raw Gab record.

        Returns:
            A list of Actions model instances.
        """
        actions_list = []

        originator_post_id = str(pid) if (pid := record.get("id")) is not None else None
        originator_account_id = (
            str(record.get("user", {}).get("id"))
            if record.get("user", {}).get("id")
            else None
        )

        created_at = None
        if record.get("created_utc"):
            created_at = datetime.fromtimestamp(int(record["created_utc"]))

        retrieved_at = datetime.fromtimestamp(int(record["retrieved_utc"]))

        if (
            record.get("is_reply")
            and record.get("parent_id")
            and (originator_account_id or originator_post_id)
        ):
            actions_list.append(
                Actions(
                    action_type=ActionType.COMMENT,
                    originator_post_id=originator_post_id,
                    target_post_id=str(
                        record["parent_id"]
                    ),  # Target is the thread parent
                    originator_account_id=originator_account_id,
                    created_at=created_at,
                    retrieved_at=retrieved_at,
                )
            )

        if record.get("is_quote") and (originator_account_id or originator_post_id):
            quoted_id = record.get("quote_conversation_parent_id")
            partent_id = str(record["parent_id"]) if record.get("parent_id") else None
            if quoted_id:
                actions_list.append(
                    Actions(
                        action_type=ActionType.QUOTE,
                        originator_post_id=originator_post_id,
                        target_post_id=str(quoted_id) if quoted_id else None,
                        originator_account_id=originator_account_id,
                        created_at=created_at,
                        retrieved_at=retrieved_at,
                    )
                )
            elif partent_id:
                actions_list.append(
                    Actions(
                        action_type=ActionType.QUOTE,
                        originator_post_id=originator_post_id,
                        target_post_id=partent_id,
                        originator_account_id=originator_account_id,
                        created_at=created_at,
                        retrieved_at=retrieved_at,
                    )
                )

        return actions_list

    def _get_entity_info(self, record: dict) -> Set[Entities]:
        """
        Extract entities (links, hashtags, mentions, emails) from a record.

        Args:
            record: The raw Gab record.

        Returns:
            A set of Entities model instances.
        """

        if record.get("retrieved_utc"):
            retrieved_at = datetime.fromtimestamp(int(record["retrieved_utc"]))

        entity_set = set()
        post_id = str(record.get("id"))
        account_id = (
            str(record.get("user", {}).get("id"))
            if record.get("user", {}).get("id")
            else None
        )
        if record.get("created_utc"):
            timestamp = int(record["created_utc"])
            timestamp = datetime.fromtimestamp(timestamp)
        else:
            timestamp = None

        if (
            record.get("attachment", {})
            and record["attachment"].get("type")
            and record["attachment"].get("value")
        ):

            if record["attachment"]["type"] == "media":
                for url in record["attachment"]["value"]:
                    entity_dict = {
                        "post_id": post_id,
                        "body": url.get("url_full"),
                        "entity_type": EntityType.LINK,
                        "created_at": timestamp,
                        "account_id": account_id,
                        "retrieved_at": retrieved_at,
                    }

                    entity_set.add(Entities(**entity_dict))

            elif record["attachment"]["type"] == "tv":

                txt = record.get("body").replace("\n", " ")
                txt = txt.replace("\t", " ")
                txt.split(" ")

                entity_dict = {
                    "post_id": post_id,
                    "body": txt[-1] if txt[-1].startswith("https://") else None,
                    "entity_type": EntityType.LINK,
                    "created_at": timestamp,
                    "account_id": account_id,
                    "retrieved_at": retrieved_at,
                }

                entity_set.add(Entities(**entity_dict))

            elif record["attachment"]["type"] == "url":
                entity_dict = {
                    "post_id": post_id,
                    "body": record["attachment"]["value"].get("url"),
                    "entity_type": EntityType.LINK,
                    "created_at": timestamp,
                    "account_id": account_id,
                    "retrieved_at": retrieved_at,
                }

                entity_set.add(Entities(**entity_dict))

            elif record["attachment"]["type"] == "giphy":
                entity_dict = {
                    "post_id": post_id,
                    "body": record["attachment"]["value"],
                    "entity_type": EntityType.LINK,
                    "created_at": timestamp,
                    "account_id": account_id,
                    "retrieved_at": retrieved_at,
                }

                entity_set.add(Entities(**entity_dict))

            elif record["attachment"]["type"] == "youtube":
                entity_dict = {
                    "post_id": post_id,
                    "body": record["attachment"]["value"].get("url"),
                    "entity_type": EntityType.LINK,
                    "created_at": timestamp,
                    "account_id": account_id,
                    "retrieved_at": retrieved_at,
                }

                entity_set.add(Entities(**entity_dict))

        emails = extract_emails(record.get("body", ""))
        for email in emails:
            entity_dict = {
                "post_id": post_id,
                "body": email,
                "entity_type": EntityType.EMAIL,
                "created_at": timestamp,
                "account_id": account_id,
                "retrieved_at": retrieved_at,
            }

            entity_set.add(Entities(**entity_dict))

        urls = extract_urls(record.get("body", ""))
        for url in urls:
            entity_dict = {
                "post_id": post_id,
                "body": url,
                "entity_type": EntityType.LINK,
                "created_at": timestamp,
                "account_id": account_id,
                "retrieved_at": retrieved_at,
            }

            entity_set.add(Entities(**entity_dict))

        hashtags = extract_hashtags(record.get("body", ""))
        for hashtag in hashtags:
            entity_dict = {
                "post_id": post_id,
                "body": hashtag,
                "entity_type": EntityType.HASHTAG,
                "created_at": timestamp,
                "account_id": account_id,
                "retrieved_at": retrieved_at,
            }

            entity_set.add(Entities(**entity_dict))

        mentions = extract_mentions(record.get("body", ""))
        for mention in mentions:
            entity_dict = {
                "post_id": post_id,
                "body": mention,
                "entity_type": EntityType.USER_TAG,
                "created_at": timestamp,
                "account_id": account_id,
                "retrieved_at": retrieved_at,
            }

            entity_set.add(Entities(**entity_dict))
        return entity_set

    def standardize(self, input_record: Tuple[dict, SourceInfo]) -> List[Any]:
        """
        Standardize a Gab record into a list of DB model instances.

        Args:
            input_record: A tuple containing the raw record and source info.

        Returns:
            A list of DB model instances (Accounts, Posts, Actions, Entities).
        """
        record, src = input_record

        outputs = []

        account = self._get_account_info(record)
        if account:
            outputs.append(account)

        post = self._get_post_info(record)
        if post:
            outputs.append(post)

        entities = self._get_entity_info(record)
        outputs.extend(list(entities))

        actions = self._get_action_info(record)
        outputs.extend(actions)

        return outputs
