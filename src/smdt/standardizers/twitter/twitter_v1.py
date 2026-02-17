from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, List, Mapping, Optional, Set, Tuple

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


def _point_ewkt(geo: Optional[Mapping[str, Any]]) -> Optional[str]:
    """
    Parses a geospatial dictionary into a POINT EWKT string (SRID 4326).
    """
    if not geo:
        return None
    coords = geo.get("coordinates") or {}
    if isinstance(coords, (list, tuple)) and len(coords) == 2:
        try:
            lon, lat = float(coords[0]), float(coords[1])
            return f"SRID=4326;POINT({lon} {lat})"
        except Exception:
            return None


def _map2int(value: Any) -> Optional[int]:
    """
    Safely converts a value to an integer, returning None on failure.
    """
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


@dataclass
class TwitterV1Standardizer(Standardizer):
    """
    Standardizer for Twitter API v1.1 data.

    This class processes records from Twitter API v1 exports, normalizing them into the standard
    schema models (Accounts, Posts, Entities, Actions). It handles recursive structures like
    retweeted_status and quoted_status.
    """

    name: str = "twitter_v1"

    _DATE_FMT = "%a %b %d %H:%M:%S %z %Y"

    @staticmethod
    def _parse_dt(s: Optional[str]) -> Optional[datetime]:
        """
        Parses a Twitter timestamp string into a timezone-agnostic UTC datetime.
        """
        if not s:
            return None
        try:
            dt = datetime.strptime(s, TwitterV1Standardizer._DATE_FMT)
            return dt.astimezone(timezone.utc).replace(tzinfo=None)
        except Exception:
            return None

    def _extract_accounts(
        self, record: Mapping[str, Any], accounts: Set[Accounts], retrieved_at: datetime
    ) -> None:
        """
        Recursively extracts user accounts from the record and its nested status objects.
        """
        user = record.get("user")
        if user:
            created_at = self._parse_dt(user.get("created_at"))
            accounts.add(
                Accounts(
                    account_id=user.get("id_str"),
                    username=user.get("screen_name"),
                    profile_name=user.get("name"),
                    bio=user.get("description"),
                    location=None,
                    post_count=user.get("statuses_count"),
                    friend_count=user.get("friends_count"),
                    follower_count=user.get("followers_count"),
                    is_verified=user.get("verified"),
                    profile_image_url=user.get("profile_image_url"),
                    created_at=created_at,
                    retrieved_at=retrieved_at,
                )
            )
        if "retweeted_status" in record:
            self._extract_accounts(record["retweeted_status"], accounts, retrieved_at)

        if "quoted_status" in record:
            self._extract_accounts(record["quoted_status"], accounts, retrieved_at)

    def _extract_posts_and_text_entities(
        self,
        record: Mapping[str, Any],
        posts_and_entities: Set[Posts],
        retrieved_at: datetime,
    ) -> None:
        """
        Recursively extracts posts and entities found in the text (emails, hashtags, etc.).
        """
        body = None
        if record.get("truncated"):
            if record["truncated"]:
                body = record.get("extended_tweet", {}).get("full_text")
            else:
                body = record.get("text")
        else:
            body = record.get("text")

        if body:
            posts_and_entities.add(
                Posts(
                    account_id=record.get("user", {}).get("id_str"),
                    post_id=record.get("id_str"),
                    conversation_id=record.get("in_reply_to_status_id_str"),
                    body=body,
                    like_count=_map2int(record.get("favorite_count")),
                    view_count=None,
                    share_count=_map2int(record.get("retweet_count")),
                    comment_count=_map2int(record.get("reply_count")),
                    quote_count=_map2int(record.get("quote_count")),
                    bookmark_count=None,
                    location=_point_ewkt(record.get("geo")),
                    created_at=self._parse_dt(record.get("created_at")),
                    retrieved_at=retrieved_at,
                )
            )

            emails = extract_emails(body)
            for email in emails:
                posts_and_entities.add(
                    Entities(
                        created_at=self._parse_dt(record.get("created_at")),
                        retrieved_at=retrieved_at,
                        post_id=record.get("id_str"),
                        body=email,
                        entity_type=EntityType.EMAIL,
                        account_id=record.get("user", {}).get("id_str"),
                    )
                )

            hashtags = extract_hashtags(body)
            for hashtag in hashtags:
                posts_and_entities.add(
                    Entities(
                        created_at=self._parse_dt(record.get("created_at")),
                        retrieved_at=retrieved_at,
                        post_id=record.get("id_str"),
                        body=hashtag.replace("#", ""),
                        entity_type=EntityType.HASHTAG,
                        account_id=record.get("user", {}).get("id_str"),
                    )
                )

            mentions = extract_mentions(body)
            for mention in mentions:
                posts_and_entities.add(
                    Entities(
                        created_at=self._parse_dt(record.get("created_at")),
                        retrieved_at=retrieved_at,
                        post_id=record.get("id_str"),
                        body=mention.replace("@", ""),
                        entity_type=EntityType.USER_TAG,
                        account_id=record.get("user", {}).get("id_str"),
                    )
                )

            urls = extract_urls(body)
            for url in urls:
                posts_and_entities.add(
                    Entities(
                        created_at=self._parse_dt(record.get("created_at")),
                        retrieved_at=retrieved_at,
                        post_id=record.get("id_str"),
                        body=str(url),
                        entity_type=EntityType.LINK,
                        account_id=record.get("user", {}).get("id_str"),
                    )
                )

        if "retweeted_status" in record:
            self._extract_posts_and_text_entities(
                record["retweeted_status"], posts_and_entities, retrieved_at
            )

        if "quoted_status" in record:
            self._extract_posts_and_text_entities(
                record["quoted_status"], posts_and_entities, retrieved_at
            )

    def _extract_actions(
        self, record: Mapping[str, Any], actions: Set[Actions], retrieved_at: datetime
    ) -> None:
        """
        Recursively extracts actions (shares/retweets, comments/replies) from the record.
        """
        if record.get("in_reply_to_status_id_str"):
            if record.get("created_at"):
                actions.add(
                    Actions(
                        created_at=self._parse_dt(record.get("created_at")),
                        action_type=ActionType.COMMENT,
                        originator_account_id=record.get("user", {}).get("id_str"),
                        originator_post_id=record.get("id_str"),
                        target_account_id=record.get("in_reply_to_user_id_str"),
                        target_post_id=record.get("in_reply_to_status_id_str"),
                        retrieved_at=retrieved_at,
                    )
                )

        if "quoted_status" in record:
            originator_account_id = record.get("user", {}).get("id_str")
            originator_post_id = record.get("id_str")
            target_account_id = (
                record.get("quoted_status").get("user", {}).get("id_str")
            )
            target_post_id = record.get("quoted_status_id_str")
            if target_post_id is None and isinstance(record["quoted_status"], dict):
                target_post_id = record.get("quoted_status_id_str")

            has_originator = originator_post_id or originator_account_id
            has_target = target_post_id or target_account_id
            if has_originator and has_target:
                actions.add(
                    Actions(
                        created_at=self._parse_dt(record.get("created_at")),
                        action_type=ActionType.QUOTE,
                        originator_account_id=originator_account_id,
                        originator_post_id=originator_post_id,
                        target_account_id=target_account_id,
                        target_post_id=target_post_id,
                        retrieved_at=retrieved_at,
                    )
                )

        text = record.get("text", "")
        if len(text) >= 4 and "RT @" in text[:4]:  # if retweet
            created_at = self._parse_dt(record.get("created_at"))
            originator_account_id = record.get("user", {}).get("id_str")
            originator_post_id = record.get("id_str")
            users = record.get("entities", {}).get("user_mentions", [])
            if len(users) > 0:
                target_account_id = users[0].get("id_str")
                # V1 old version did not have this information
                target_post_id = None
            action_type = ActionType.SHARE

            has_originator = originator_post_id or originator_account_id
            has_target = target_post_id or target_account_id

            if has_originator and has_target:
                actions.add(
                    Actions(
                        created_at=created_at,
                        action_type=action_type,
                        originator_account_id=originator_account_id,
                        originator_post_id=originator_post_id,
                        target_account_id=target_account_id,
                        target_post_id=target_post_id,
                        retrieved_at=retrieved_at,
                    )
                )

        if record.get("in_reply_to_status_id_str"):
            created_at = self._parse_dt(record.get("created_at"))
            originator_account_id = record.get("user", {}).get("id_str")
            originator_post_id = record.get("id_str")
            target_post_id = record.get("in_reply_to_status_id_str")
            target_account_id = record.get("in_reply_to_user_id_str")
            action_type = ActionType.COMMENT

            has_originator = originator_post_id or originator_account_id
            has_target = target_post_id or target_account_id

            if has_originator and has_target:
                actions.add(
                    Actions(
                        created_at=created_at,
                        action_type=action_type,
                        originator_account_id=originator_account_id,
                        originator_post_id=originator_post_id,
                        target_account_id=target_account_id,
                        target_post_id=target_post_id,
                        retrieved_at=retrieved_at,
                    )
                )

        if "retweeted_status" in record:
            self._extract_actions(record["retweeted_status"], actions, retrieved_at)

        if "quoted_status" in record:
            self._extract_actions(record["quoted_status"], actions, retrieved_at)

    def _extract_entities(
        self,
        record: Mapping[str, Any],
        output_entities: Set[Entities],
        retrieved_at: datetime,
    ) -> None:
        post_id = record.get("id_str")  # Tweet id
        entities = record["entities"]

        for entity_type in entities:
            for single_entity in entities[entity_type]:
                if entity_type == "user_mentions":
                    entity_dict = {
                        "post_id": post_id,
                        "body": single_entity.get("id_str"),
                        "entity_type": EntityType.USER_TAG,
                    }

                elif entity_type == "hashtags":
                    entity_dict = {
                        "post_id": post_id,
                        "body": single_entity.get("text"),
                        "entity_type": EntityType.HASHTAG,
                    }

                elif entity_type == "urls":
                    entity_dict = {
                        "post_id": post_id,
                        "body": single_entity.get("expanded_url"),
                        "entity_type": EntityType.LINK,
                    }

                elif entity_type == "media":
                    entity_type = None
                    if "photo" in str(single_entity.get("type")).lower():
                        entity_type = EntityType.IMAGE
                    elif "video" in str(single_entity.get("type")).lower():
                        entity_type = EntityType.VIDEO
                    elif "gif" in str(single_entity.get("type")).lower():
                        entity_type = EntityType.VIDEO  # ON purpose
                    if entity_type:
                        entity_dict = {
                            "post_id": post_id,
                            "body": single_entity.get("media_url_https"),
                            "entity_type": entity_type,
                        }

                else:
                    break

                created_at = self._parse_dt(record.get("created_at"))

                output_entities.add(
                    Entities(
                        created_at=created_at,
                        retrieved_at=retrieved_at,
                        post_id=post_id,
                        body=entity_dict["body"],
                        entity_type=entity_dict["entity_type"],
                        account_id=record.get("user", {}).get("id_str"),
                    )
                )

        for media_item in record.get("extended_entities", {}).get("media", []):
            entity_type = None
            if "photo" in str(media_item.get("type")).lower():
                entity_type = EntityType.IMAGE
            elif "video" in str(media_item.get("type")).lower():
                entity_type = EntityType.VIDEO
            elif "gif" in str(media_item.get("type")).lower():
                entity_type = EntityType.VIDEO  # ON purpose
            if entity_type:
                entity_dict = {
                    "post_id": post_id,
                    "body": media_item.get("expanded_url"),
                    "entity_type": entity_type,
                }

            output_entities.add(
                Entities(
                    created_at=self._parse_dt(record.get("created_at")),
                    retrieved_at=retrieved_at,
                    post_id=post_id,
                    body=entity_dict["body"],
                    entity_type=entity_dict["entity_type"],
                    account_id=record.get("user", {}).get("id_str"),
                )
            )

        if "retweeted_status" in record:
            self._extract_entities(
                record["retweeted_status"], output_entities, retrieved_at
            )

        if "quoted_status" in record:
            self._extract_entities(
                record["quoted_status"], output_entities, retrieved_at
            )

    # --------- main API ---------
    def standardize(
        self, input_record: Tuple[Mapping[str, Any], SourceInfo]
    ) -> List[Any]:
        """
        Standardizes a single input record into a list of schema models.
        Accepts a single Twitter v1 tweet object (possibly with nested retweet/quote trees).

        Args:
            input_record (Tuple[Mapping[str, Any], SourceInfo]): A tuple containing the raw record and source information.

        Returns:
            List[Any]: A list of standardized models (Accounts, Posts, Actions, Entities) derived from the input record.
        """
        record, src = input_record
        # Not available in v1 tweets; use current time
        retrieved_at = datetime.now(timezone.utc)

        outputs = set()

        # build per-record sets (dedup within the nested structure)
        self._extract_accounts(record, outputs, retrieved_at)
        self._extract_posts_and_text_entities(record, outputs, retrieved_at)
        self._extract_actions(record, outputs, retrieved_at)
        self._extract_entities(record, outputs, retrieved_at)

        return list(outputs)
