from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, List, Mapping, Optional, List, Tuple, DefaultDict
from collections import defaultdict

from smdt.standardizers.base import Standardizer, SourceInfo
from smdt.store.models.accounts import Accounts
from smdt.store.models.posts import Posts
from smdt.store.models.entities import Entities, EntityType
from smdt.store.models.actions import Actions, ActionType

import re

from smdt.standardizers.utils import extract_urls


def map2int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def extract_hashtags(text):
    HASHTAG_RE = re.compile(r"#([^#\s]+)#")
    return HASHTAG_RE.findall(text)


def extract_mentions(text):
    WEIBO_MENTION_RE = re.compile(r"@([^\s@#：:，,。.!?()（）【】\[\]<>\"“”‘’]+)")
    return WEIBO_MENTION_RE.findall(text)


@dataclass
class WeiboMisBotStandardizer(Standardizer):
    name: str = "weibo_misbot_stream"
    platform: str = "weibo"

    accountID2creationtime = dict()
    last_id = 0
    text2id = dict()

    def get_text_id(self, text: str) -> int:
        if text not in self.text2id:
            self.last_id += 1
            self.text2id[text] = self.last_id
        return self.text2id[text]

    def standardize(
        self, input_record: Tuple[Mapping[str, Any], SourceInfo]
    ) -> List[Any]:
        """
        Dispatch by member (file) name. 'row' is a dict produced by your csv reader.
        Yields: Accounts, Posts, Entities, Actions (append-only).
        """
        record, src = input_record
        outputs = []

        # set created_at as the begging of the time
        start_datetime = datetime(1970, 1, 1, tzinfo=timezone.utc)

        if src.path.endswith("extracted_user_creation_times.jsonl"):
            account_id = record.get("account_id")
            creation_time = record.get("estimated_creation_time")
            if creation_time:
                creation_time = datetime.fromisoformat(creation_time)
                self.accountID2creationtime[account_id] = creation_time
        elif src.path.endswith("train_data.jsonl") or src.path.endswith(
            "inference_data.jsonl"
        ):
            uid = record.get("uid")
            description = record.get("description", None)
            numerical = record.get(
                "numerical", []
            )  # followers count; follow count; and statuses count 3 dims
            categorical = record.get("categorical", [])
            tweets = record.get("tweet", [])

            creation_time = self.accountID2creationtime.get(uid, start_datetime)

            account = Accounts(
                account_id=uid,
                username=None,
                profile_name=None,
                bio=description,
                location=None,
                post_count=numerical[2] if len(numerical) > 2 else None,
                friend_count=numerical[1] if len(numerical) > 1 else None,
                follower_count=numerical[0] if len(numerical) > 0 else None,
                is_verified=bool(categorical[0]) if len(categorical) > 0 else None,
                profile_image_url=None,
                created_at=creation_time,
                retrieved_at=start_datetime,
            )
            outputs.append(account)

            for tweet_text in tweets:
                post = Posts(
                    account_id=uid,
                    post_id=self.get_text_id(tweet_text),
                    conversation_id=None,
                    body=tweet_text,
                    like_count=None,
                    view_count=None,
                    share_count=None,
                    comment_count=None,
                    quote_count=None,
                    bookmark_count=None,
                    location=None,
                    created_at=start_datetime,
                    retrieved_at=start_datetime,
                )
                outputs.append(post)

                mentions = extract_mentions(tweet_text)
                for mention in mentions:
                    entity = Entities(
                        created_at=start_datetime,
                        retrieved_at=start_datetime,
                        post_id=self.get_text_id(tweet_text),
                        body=mention,
                        entity_type=EntityType.USER_TAG,
                        account_id=uid,
                    )
                    outputs.append(entity)
                hashtags = extract_hashtags(tweet_text)
                for tag in hashtags:
                    entity = Entities(
                        created_at=start_datetime,
                        retrieved_at=start_datetime,
                        post_id=self.get_text_id(tweet_text),
                        body=tag,
                        entity_type=EntityType.HASHTAG,
                        account_id=uid,
                    )
                    outputs.append(entity)

                for url in extract_urls(text):
                    entity = Entities(
                        created_at=publish_time,
                        retrieved_at=start_datetime,
                        post_id=self.get_text_id(text),
                        body=url,
                        entity_type=EntityType.LINK,
                        account_id=user_from,
                    )
                    outputs.append(entity)

        elif (
            src.path.endswith("misinformation.jsonl")
            or src.path.endswith("verified_information.jsonl")
            or src.path.endswith("trend_information.jsonl")
        ):

            article = record.get("article", {})
            article_publish_time = article.get("publish_time", None)
            if article_publish_time:
                article_publish_time = datetime.fromtimestamp(
                    article_publish_time, tz=timezone.utc
                )
            article_content = article.get("article_content", None)
            post_id = self.get_text_id(article_content)
            post = Posts(
                account_id="NULL",
                post_id=post_id,
                body=article_content,
                like_count=map2int(article.get("attitude_count", None)),
                share_count=map2int(article.get("repost_count", None)),
                comment_count=map2int(article.get("comment_count", None)),
                created_at=article_publish_time,
                retrieved_at=start_datetime,
            )
            outputs.append(post)

            hashtags = extract_hashtags(article_content)
            for tag in hashtags:
                entity = Entities(
                    created_at=start_datetime,
                    retrieved_at=start_datetime,
                    post_id=post_id,
                    body=tag,
                    entity_type=EntityType.HASHTAG,
                    account_id="NULL",
                )
                outputs.append(entity)
            mentions = extract_mentions(article_content)
            for mention in mentions:
                entity = Entities(
                    created_at=start_datetime,
                    retrieved_at=start_datetime,
                    post_id=post_id,
                    body=mention,
                    entity_type=EntityType.USER_TAG,
                    account_id="NULL",
                )
                outputs.append(entity)
            for url in extract_urls(article_content):
                entity = Entities(
                    created_at=start_datetime,
                    retrieved_at=start_datetime,
                    post_id=post_id,
                    body=url,
                    entity_type=EntityType.LINK,
                    account_id="NULL",
                )
                outputs.append(entity)

            # Let's just use the graphs to create actions
            comment_graphs = record.get("comment_graphs", [])
            for g in comment_graphs:
                for node in g.get("nodes", []):
                    user_from = node.get("user_from", None)
                    user_to = node.get("user_to", None)
                    publish_time = node.get("publish_time", None)
                    text = node.get("text", None)
                    if publish_time:
                        publish_time = datetime.fromtimestamp(
                            publish_time, tz=timezone.utc
                        )
                    if user_from is not None and user_to is not None:
                        action = Actions(
                            created_at=publish_time,
                            action_type=ActionType.COMMENT,
                            originator_account_id=user_from,
                            originator_post_id=self.get_text_id(text),
                            target_account_id=user_to,
                            target_post_id=None,
                            retrieved_at=start_datetime,
                        )
                        outputs.append(action)

                    if text:
                        post = Posts(
                            account_id=user_from,
                            post_id=self.get_text_id(text),
                            conversation_id=None,
                            body=text,
                            like_count=None,
                            view_count=None,
                            share_count=None,
                            comment_count=None,
                            quote_count=None,
                            bookmark_count=None,
                            location=None,
                            created_at=publish_time,
                            retrieved_at=start_datetime,
                        )

                        hashtags = extract_hashtags(text)
                        for tag in hashtags:
                            entity = Entities(
                                created_at=publish_time,
                                retrieved_at=start_datetime,
                                post_id=self.get_text_id(text),
                                body=tag,
                                entity_type=EntityType.HASHTAG,
                                account_id=user_from,
                            )
                            outputs.append(entity)

                        mentions = extract_mentions(text)
                        for mention in mentions:
                            entity = Entities(
                                created_at=publish_time,
                                retrieved_at=start_datetime,
                                post_id=self.get_text_id(text),
                                body=mention,
                                entity_type=EntityType.USER_TAG,
                                account_id=user_from,
                            )
                            outputs.append(entity)

                        for url in extract_urls(text):
                            entity = Entities(
                                created_at=publish_time,
                                retrieved_at=start_datetime,
                                post_id=self.get_text_id(text),
                                body=url,
                                entity_type=EntityType.LINK,
                                account_id=user_from,
                            )
                            outputs.append(entity)

            nodes = record.get("repost_graph", {}).get("nodes", [])
            used_indexes = set()
            for src_index, trg_index in record.get("repost_graph", {}).get("edges", []):
                used_indexes.add(src_index - 1)
                target_post_id = None
                if trg_index == 0:
                    # Reposting the original post
                    target_post_id = self.get_text_id(
                        record.get("article", {}).get("article_content", None)
                    )

                src_node = nodes[src_index - 1]
                trg_node = nodes[trg_index - 1]
                user_from = src_node.get("name", None)
                user_to = trg_node.get("name", None)
                repost_publish_time = src_node.get("publish_time", None)

                publish_time = src_node.get("publish_time", None)
                text = src_node.get("text", None)
                if publish_time:
                    publish_time = datetime.fromtimestamp(publish_time, tz=timezone.utc)
                if repost_publish_time:
                    repost_publish_time = datetime.fromtimestamp(
                        repost_publish_time, tz=timezone.utc
                    )

                if user_from is not None and (
                    user_to is not None or target_post_id is not None
                ):
                    action = Actions(
                        created_at=repost_publish_time,
                        action_type=ActionType.QUOTE if text else ActionType.SHARE,
                        originator_account_id=user_from,
                        originator_post_id=self.get_text_id(text),
                        target_account_id=None if target_post_id else user_to,
                        target_post_id=target_post_id,
                        retrieved_at=start_datetime,
                    )
                    outputs.append(action)

                    if text:
                        post = Posts(
                            account_id=user_from,
                            post_id=self.get_text_id(text),
                            conversation_id=None,
                            body=text,
                            like_count=None,
                            view_count=None,
                            share_count=None,
                            comment_count=None,
                            quote_count=None,
                            bookmark_count=None,
                            location=None,
                            created_at=publish_time,
                            retrieved_at=start_datetime,
                        )
                        hashtags = extract_hashtags(text)
                        for tag in hashtags:
                            entity = Entities(
                                created_at=publish_time,
                                retrieved_at=start_datetime,
                                post_id=self.get_text_id(text),
                                body=tag,
                                entity_type=EntityType.HASHTAG,
                                account_id=user_from,
                            )
                            outputs.append(entity)
                        mentions = extract_mentions(text)
                        for mention in mentions:
                            entity = Entities(
                                created_at=publish_time,
                                retrieved_at=start_datetime,
                                post_id=self.get_text_id(text),
                                body=mention,
                                entity_type=EntityType.USER_TAG,
                                account_id=user_from,
                            )
                            outputs.append(entity)

                        for url in extract_urls(text):
                            entity = Entities(
                                created_at=publish_time,
                                retrieved_at=start_datetime,
                                post_id=self.get_text_id(text),
                                body=url,
                                entity_type=EntityType.LINK,
                                account_id=user_from,
                            )
                            outputs.append(entity)
        return outputs
