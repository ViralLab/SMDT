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
)
import glob
from pprint import pprint
import json


def map2int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def unix2datetime(unix_time: Optional[int]) -> Optional[datetime]:

    if unix_time is None:
        return None
    try:
        # Check if timestamp is likely in milliseconds (13 digits)
        # 10 digits is usually seconds (valid until year 2286)
        if unix_time > 9999999999:
            unix_time /= 1000

        return datetime.fromtimestamp(unix_time, tz=timezone.utc)
    except (ValueError, OSError):
        return None


@dataclass
class TwitterIOStandardizer(Standardizer):
    name: str = "twitter_io"
    platform: str = "twitter"

    hash2value = dict()
    last_loaded_country_code: Optional[str] = None

    def flatten_mapper(self):
        """
        Resolves recursive chains (A -> @B -> @C) into direct links (A -> @C).
        """
        flattened = {}

        for key in self.hash2value:
            # Start the chain
            current_val = self.hash2value[key]

            # Keep track of visited keys to prevent infinite loops (e.g., A->B->A)
            visited = {key}

            # While the value looks like another key in our dictionary...
            # We strip the '@' to check if the raw hash exists as a key.
            while current_val.startswith("@") and current_val[1:] in self.hash2value:
                next_key = current_val[1:]

                # If we've seen this key before, it's a loop. Stop to avoid crashing.
                if next_key in visited:
                    break

                visited.add(next_key)
                current_val = self.hash2value[next_key]

            # Store the final resolved value
            flattened[key] = current_val

        return flattened

    def clean_text_content(self, text: str) -> str:
        if not isinstance(text, str):
            return text

        # Regex: Matches '@', 'http://', or 'https://' followed by the hash
        # 's?' means the 's' is optional
        pattern = r"(@|https?://)([a-zA-Z0-9]+)"

        def replacer(match):
            prefix = match.group(1)  # The prefix (@, http://, or https://)
            hash_val = match.group(2)  # The hash string

            if prefix == "@":
                # It's a User Mention
                # Replace the WHOLE match (@HASH) with the mapper value (@Handle)
                # fallback to match.group(0) (the original text) if not found
                return self.hash2value.get(hash_val, match.group(0))

            else:
                # It's a URL (http or https)
                # Only replace if you have a specific URL mapper, otherwise ignore
                if hash_val in self.hash2value:
                    return self.hash2value[hash_val]
                return match.group(0)  # Keep original http://HASH

        return re.sub(pattern, replacer, text)

    def denonymize(self, record):
        new_record = record.copy()

        id_fields = ["accountid", "in_reply_to_accountid", "reposted_accountid"]
        for field in id_fields:
            if field in new_record and new_record[field] in self.hash2value:
                new_record[field] = self.hash2value[new_record[field].lstrip("@")]

        if "account_mentions" in new_record and isinstance(
            new_record["account_mentions"], list
        ):
            new_record["account_mentions"] = [
                self.hash2value.get(item, item).lstrip("@")
                for item in new_record["account_mentions"]
            ]

        if "post_text" in new_record:
            new_record["post_text"] = self.clean_text_content(new_record["post_text"])

        return new_record

    def standardize(self, input_record) -> List[Any]:
        record, src = input_record

        outputs = []

        country_code = (
            src.path.split("/")[-1].split(".")[0].split("_part")[0].split("_")[0]
        )
        # print(country_code)
        # input()
        mapping_path = f"/ctb/data/InformationOperationDataset/Mappings/"

        if country_code != self.last_loaded_country_code:
            for fpath in glob.glob(mapping_path + f"{country_code}*.json"):
                with open(fpath, "r") as f:
                    value2hash = json.load(f)
                    temp_mapper = {v: k for k, v in value2hash.items()}
                    self.hash2value.update(temp_mapper)
            self.flatten_mapper()
            self.last_loaded_country_code = country_code

        new_record = self.denonymize(record)
        # pprint(new_record)
        # input("WAIT")

        post = Posts(
            created_at=(
                new_record.get("post_time")
                if isinstance(new_record.get("post_time"), datetime)
                else unix2datetime(new_record.get("post_time"))
            ),
            account_id=new_record.get("accountid").lstrip("@"),
            post_id=new_record.get("postid"),
            conversation_id=None,
            body=new_record.get("post_text"),
            like_count=None,
            view_count=None,
            share_count=None,
            comment_count=None,
            quote_count=None,
            bookmark_count=None,
            location=None,
            retrieved_at=None,
        )

        outputs.append(post)

        if new_record.get("is_repost", False):
            target_account_id = new_record.get("reposted_accountid")
            target_post_id = new_record.get("reposted_postid")
            if target_account_id is not None and target_post_id is not None:
                action = Actions(
                    created_at=(
                        new_record.get("post_time")
                        if isinstance(new_record.get("post_time"), datetime)
                        else unix2datetime(new_record.get("post_time"))
                    ),
                    action_type=ActionType.SHARE,
                    originator_account_id=new_record.get("accountid").lstrip("@"),
                    originator_post_id=new_record.get("postid"),
                    target_account_id=target_account_id.lstrip("@"),
                    target_post_id=target_post_id,
                    retrieved_at=None,
                )
                outputs.append(action)

        if new_record.get("in_reply_to_accountid") is not None:
            target_account_id = new_record.get("in_reply_to_accountid").lstrip("@")
            target_post_id = new_record.get("in_reply_to_postid")
            if target_account_id is not None and target_post_id is not None:
                action = Actions(
                    created_at=(
                        new_record.get("post_time")
                        if isinstance(new_record.get("post_time"), datetime)
                        else unix2datetime(new_record.get("post_time"))
                    ),
                    action_type=ActionType.COMMENT,
                    originator_account_id=new_record.get("accountid").lstrip("@"),
                    originator_post_id=new_record.get("postid"),
                    target_account_id=target_account_id,
                    target_post_id=target_post_id,
                    retrieved_at=None,
                )
                outputs.append(action)
        if isinstance(new_record.get("post_time"), datetime):
            created_at_utc = new_record.get("post_time")
        else:
            created_at_utc = unix2datetime(new_record.get("post_time"))
        # if created_at_utc is not None:
        #     # 1. If it is a strict date (but not a datetime), convert it to midnight
        #     if type(created_at_utc) is date:
        #         created_at_utc = datetime.combine(created_at_utc, time.min)

        #     if created_at_utc.tzinfo is None:
        #         created_at_utc = created_at_utc.replace(tzinfo=timezone.utc)
        account = Accounts(
            created_at=created_at_utc,
            account_id=new_record.get("accountid").lstrip("@"),
            username=new_record.get("accountid").lstrip("@"),
            profile_name=None,
            bio=new_record.get("account_profile_description"),
            location=None,
            post_count=None,
            friend_count=map2int(int(new_record.get("following_count"))),
            follower_count=map2int(new_record.get("follower_count")),
            is_verified=None,
            profile_image_url=None,
            retrieved_at=None,
        )

        outputs.append(account)

        for mention in extract_mentions(new_record.get("post_text", "")):
            entity = Entities(
                created_at=(
                    new_record.get("post_time")
                    if isinstance(new_record.get("post_time"), datetime)
                    else unix2datetime(new_record.get("post_time"))
                ),
                entity_type=EntityType.USER_TAG,
                account_id=new_record.get("accountid").lstrip("@"),
                post_id=new_record.get("postid"),
                body=mention.lstrip("@").lower(),
                retrieved_at=None,
            )
            outputs.append(entity)

        for hashtag in extract_hashtags(new_record.get("post_text", "")):
            entity = Entities(
                created_at=(
                    new_record.get("post_time")
                    if isinstance(new_record.get("post_time"), datetime)
                    else unix2datetime(new_record.get("post_time"))
                ),
                entity_type=EntityType.HASHTAG,
                account_id=new_record.get("accountid").lstrip("@"),
                post_id=new_record.get("postid"),
                body=hashtag.lstrip("#").lower(),
                retrieved_at=None,
            )
            outputs.append(entity)

        for url in extract_urls(new_record.get("post_text", "")):
            entity = Entities(
                created_at=(
                    new_record.get("post_time")
                    if isinstance(new_record.get("post_time"), datetime)
                    else unix2datetime(new_record.get("post_time"))
                ),
                entity_type=EntityType.LINK,
                account_id=new_record.get("accountid").lstrip("@"),
                post_id=new_record.get("postid"),
                body=url,
                retrieved_at=None,
            )
            outputs.append(entity)

        return outputs
