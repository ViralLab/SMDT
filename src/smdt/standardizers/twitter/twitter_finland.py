from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, List, Mapping, Optional, Set

from smdt.standardizers.base import Standardizer, SourceInfo
from smdt.standardizers.utils import (
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


def map2int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        d = datetime.fromisoformat(s)
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d
    except Exception:
        return None


def _retrieved_at(root: Mapping[str, Any]) -> datetime:
    ra = _dt(((root.get("__twarc") or {}).get("retrieved_at")))
    return ra or datetime.now(timezone.utc)


@dataclass
class TwitterFinlandStandardizer(Standardizer):
    name: str = "twitter_finland"
    platform: str = "twitter"

    def _get_account_info(self, obj: dict) -> Set[Accounts]:
        """
        From the given object, creates a set of Accounts objects

        Parameters:
        obj (dict): json object of one post

        Returns:
        Set[Accounts]: A set of Accounts, with attributes filled via obj

        Notes:
        The function returns a set since there are many users' information on a single tweet
        """

        def get_account_dict(user):
            account_dict = {
                "account_id": user.get("id"),
                "username": user.get("username"),
                "profile_name": user.get("name"),
                "bio": user.get("description"),
                "post_count": map2int(
                    user.get("public_metrics", {}).get("tweet_count")
                ),
                "friend_count": map2int(
                    user.get("public_metrics", {}).get("following_count")
                ),
                "follower_count": map2int(
                    user.get("public_metrics", {}).get("followers_count")
                ),
                "is_verified": user.get("verified"),
                "profile_image_url": user.get("profile_image_url"),
                "created_at": _dt(user.get("created_at")),
                "retrieved_at": _retrieved_at(user),
                # "location": user.get("location"),
                "location": None,
            }

            return Accounts(**account_dict)

        def extract_accounts(tweet_data, account_dicts=None):
            """
            Recursively extracts all unique authors from a tweet data structure.
            :param tweet_data: Dictionary containing tweet data.
            :param authors: Set to store unique authors.
            :return: Set of unique authors.
            """
            if account_dicts is None:
                account_dicts = set()

            if "author" in tweet_data:
                account_dicts.add(get_account_dict(tweet_data["author"]))

            if "referenced_tweets" in tweet_data:
                for ref_tweet in tweet_data["referenced_tweets"]:
                    if "tweet" in ref_tweet:
                        extract_accounts(ref_tweet["tweet"], account_dicts)

            return account_dicts

        accounts_set = extract_accounts(obj)

        return accounts_set

    def _get_post_info(self, obj: dict) -> Set[Posts]:
        """
        From the given object, creates a set of Post objects

        Parameters:
        obj (dict): json object of one post

        Returns:
        Set[Posts]: A set of Posts, with attributes filled via obj

        Notes:
        The function returns a set since there are many posts' information on a single tweet
        Engagement count is the sum of all the values on public_metrics
        """

        def get_post_dict(data):
            post_dict = {
                "account_id": data.get("author_id"),
                "post_id": data.get("id"),
                "body": data.get("text"),
                "like_count": map2int(data.get("public_metrics", {}).get("like_count")),
                "view_count": map2int(data.get("public_metrics", {}).get("view_count")),
                "share_count": map2int(
                    data.get("public_metrics", {}).get("retweet_count")
                ),
                "comment_count": map2int(
                    data.get("public_metrics", {}).get("reply_count")
                ),
                "quote_count": map2int(
                    data.get("public_metrics", {}).get("quote_count")
                ),
                "bookmark_count": None,  # Not available in Twitter API
                "conversation_id": data.get("conversation_id"),
                "created_at": _dt(data.get("created_at")),
                "retrieved_at": _retrieved_at(data),
            }

            return Posts(**post_dict)

        def extract_posts(tweet_data, post_dicts=None):
            """
            Recursively extracts all unique posts from a tweet data structure.
            :param tweet_data: Dictionary containing tweet data.
            :param post_dicts: Set to store unique posts.
            :return: Set of unique posts.
            """
            if post_dicts is None:
                post_dicts = set()

            post_dicts.add(get_post_dict(tweet_data))

            if "referenced_tweets" in tweet_data:
                for ref_tweet in tweet_data["referenced_tweets"]:
                    if "tweet" in ref_tweet:
                        extract_posts(ref_tweet["tweet"], post_dicts)

            return post_dicts

        posts_set = extract_posts(obj)

        return posts_set

    def _get_action_info(self, obj: dict) -> Set[Actions]:
        """
        From the given object, creates a set of Action objects

        Parameters:
        obj (dict): json object of one post

        Returns:
        Set[Actions]: A set of Actions, with attributes filled via obj

        Notes:
        The function returns a set since there are many posts' information on a single tweet
        Originator id and target id are tweet ids
        """

        def extract_actions(tweet_data, actions=None):
            """
            Recursively extracts actions (quoted, retweeted, replied) along with their originators and targets from a tweet data structure.
            :param tweet_data: Dictionary containing tweet data.
            :param actions: List to store extracted actions.
            :return: List of extracted actions.
            """
            if actions is None:
                actions = []

            if "referenced_tweets" in tweet_data:
                originator_account_id = tweet_data.get("author", {}).get("id")
                originator_post_id = tweet_data.get("id")
                action_date = created_at = _dt(tweet_data.get("created_at"))

                for ref_tweet in tweet_data["referenced_tweets"]:
                    action_dict = {}
                    action_type = ref_tweet.get("type")
                    target_account_id = ref_tweet.get("tweet", {}).get("author_id")
                    target_post_id = ref_tweet.get("tweet", {}).get("id")
                    if action_type:
                        does_originator_exist = (
                            originator_account_id or originator_post_id
                        )
                        does_target_exist = target_account_id or target_post_id
                        if not (does_originator_exist and does_target_exist):
                            continue
                        is_ref = True
                        action_dict["target_account_id"] = target_account_id
                        if action_type == "replied_to":
                            action_dict["action_type"] = ActionType.COMMENT
                        elif action_type == "quoted":
                            action_dict["action_type"] = ActionType.QUOTE
                        elif action_type == "retweeted":
                            action_dict["action_type"] = ActionType.SHARE
                        else:
                            is_ref = False

                        action_dict["originator_account_id"] = originator_account_id
                        action_dict["created_at"] = action_date
                        action_dict["originator_post_id"] = originator_post_id
                        action_dict["target_post_id"] = target_post_id
                        action_dict["retrieved_at"] = _retrieved_at(tweet_data)

                        if is_ref:
                            actions.append(Actions(**action_dict))
                    if "tweet" in ref_tweet:
                        extract_actions(ref_tweet["tweet"], actions)

            return actions

        actions_set = extract_actions(obj)
        return actions_set

    def _process_entity_field(
        self,
        entities: dict,
        post_id: str,
        account_id: str,
        created_at: Optional[datetime],
        retrieved_at: Optional[datetime] = None,
    ) -> Set[Entities]:
        """
        Processes entity fields (e.g., URLs, hashtags, mentions) in the given JSON data and adds them to the entity set.

        Parameters:
        entities (dict): The JSON object containing entity information.
        post_id (str): The ID of the post associated with these entities.
        account_id (str): The ID of the account associated with these entities.

        Returns:
        Set[Entities]: The updated set of Entity objects.
        """
        entity_set = set()
        if "urls" in entities:
            for url in entities["urls"]:
                entity_dict = {
                    "post_id": post_id,
                    "account_id": account_id,
                    "body": url.get("expanded_url"),
                    "entity_type": EntityType.LINK,
                    "created_at": created_at,
                    "retrieved_at": retrieved_at,
                }

                entity_set.add(Entities(**entity_dict))

        if "hashtags" in entities:
            for hashtag in entities["hashtags"]:
                entity_dict = {
                    "post_id": post_id,
                    "account_id": account_id,
                    "body": hashtag.get("tag").lstrip("#"),
                    "entity_type": EntityType.HASHTAG,
                    "created_at": created_at,
                    "retrieved_at": retrieved_at,
                }

                entity_set.add(Entities(**entity_dict))

        if "url" in entities:
            for url in entities["url"]["urls"]:
                entity_dict = {
                    "post_id": post_id,
                    "account_id": account_id,
                    "body": url.get("expanded_url"),
                    "entity_type": EntityType.LINK,
                    "created_at": created_at,
                    "retrieved_at": retrieved_at,
                }

                entity_set.add(Entities(**entity_dict))

        if "mentions" in entities:
            for tag in entities["mentions"]:
                entity_dict = {
                    "post_id": post_id,
                    "account_id": account_id,
                    "body": tag.get("id").lstrip("@"),
                    "entity_type": EntityType.USER_TAG,
                    "created_at": created_at,
                    "retrieved_at": retrieved_at,
                }

                entity_set.add(Entities(**entity_dict))

        if "description" in entities:
            desc = entities["description"]
            if "urls" in desc:
                for url in desc["urls"]:
                    entity_dict = {
                        "post_id": post_id,
                        "account_id": account_id,
                        "body": url.get("expanded_url"),
                        "entity_type": EntityType.LINK,
                        "created_at": created_at,
                        "retrieved_at": retrieved_at,
                    }

                    entity_set.add(Entities(**entity_dict))

            if "mentions" in desc:
                for tag in desc["mentions"]:
                    entity_dict = {
                        "post_id": post_id,
                        "account_id": account_id,
                        "body": tag.get("username").lstrip("@"),
                        "entity_type": EntityType.USER_TAG,
                        "created_at": created_at,
                        "retrieved_at": retrieved_at,
                    }

                    entity_set.add(Entities(**entity_dict))

            if "hashtags" in desc:
                for hashtag in desc["hashtags"]:
                    entity_dict = {
                        "post_id": post_id,
                        "account_id": account_id,
                        "body": hashtag.get("tag").lstrip("#"),
                        "entity_type": EntityType.HASHTAG,
                        "created_at": created_at,
                        "retrieved_at": retrieved_at,
                    }

                    entity_set.add(Entities(**entity_dict))

        return entity_set

    def _get_entity_info(self, obj: dict) -> Set[Entities]:
        """
        Extracts entity information from the given JSON object and returns a set of Entity objects.

        Parameters:
        obj (dict): JSON object representing a tweet.

        Returns:
        Set[Entity]: A set of Entity objects representing hashtags, mentions, links, and other entities found in the tweet content.
        """

        def extract_entities(tweet_data, entities_set=None):
            """
            :param tweet_data: Dictionary containing tweet data.
            :param entities_set: Set to store extracted entities.
            :return: List of extracted actions.
            """
            if entities_set is None:
                entities_set = set()

            post_id = tweet_data.get("id")
            account_id = tweet_data.get("author_id")
            retrieved_at = _retrieved_at(tweet_data)
            if tweet_data.get("created_at"):
                action_date = created_at = _dt(tweet_data.get("created_at"))
            else:
                action_date = None

            if tweet_data.get("entities"):
                entities_set.update(
                    self._process_entity_field(
                        tweet_data["entities"],
                        post_id,
                        account_id,
                        action_date,
                        retrieved_at,
                    )
                )

            if "tweet" in tweet_data:
                extract_entities(tweet_data["tweet"], entities_set)

            return entities_set

        entity_set = extract_entities(obj)

        return entity_set

    def standardize(self, input_record) -> List[Any]:
        record, src = input_record

        outputs = []

        accounts = self._get_account_info(record)
        if accounts:
            accounts = list(accounts)
            outputs.extend(accounts)

        posts = self._get_post_info(record)
        if posts:
            posts = list(posts)
            outputs.extend(posts)

        actions = self._get_action_info(record)
        if actions:
            actions = list(actions)
            outputs.extend(actions)

        entities = self._get_entity_info(record)
        if entities:
            entities = list(entities)
            outputs.extend(entities)

        return outputs
