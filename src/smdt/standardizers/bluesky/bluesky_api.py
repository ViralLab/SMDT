from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, List, Mapping, Optional

from smdt.standardizers.base import Standardizer, SourceInfo
from smdt.store.models.accounts import Accounts
from smdt.store.models.posts import Posts
from smdt.store.models.entities import Entities, EntityType
from smdt.store.models.actions import Actions, ActionType


# ---------------- helpers ----------------


def _s(x: Any) -> str:
    if isinstance(x, str):
        return x
    if x is None:
        return ""
    try:
        if x != x:  # NaN
            return ""
    except Exception:
        pass
    return str(x)


def _as_id(x: Any) -> Optional[str]:
    if x is None:
        return None
    try:
        s = str(x).strip()
        return s if s else None
    except Exception:
        return None


def _dt_iso(ts: Any) -> Optional[datetime]:
    """
    Parse ISO8601 timestamps and preserve timezone offsets if present.
    If the value ends with 'Z', map to +00:00. If no tz provided, default to UTC.
    """
    if ts is None:
        return None
    s = _s(ts).strip()
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


def _guess_kind(src: SourceInfo, row: Mapping[str, Any]) -> str:
    """
    Classify the row so we choose the right parsing branch.
    - 'user'      : a profile/user row (has 'did' and 'handle', no '$type' feed item)
    - 'post'      : app.bsky.feed.post records (stream)
    - 'repost'    : app.bsky.feed.repost records
    - 'like'      : app.bsky.feed.like records
    - 'other'     : unknown / unsupported → no output
    We also consider path/member names to help classification.
    """
    name = (src.member or src.path or "").lower()
    t = row.get("$type") or ""
    if ("user" in name or "profile" in name) and row.get("did") and row.get("handle"):
        return "user"
    if t == "app.bsky.feed.post":
        return "post"
    if t == "app.bsky.feed.repost":
        return "repost"
    if t == "app.bsky.feed.like":
        return "like"
    # sometimes user dumps have no $type but include did/handle
    if row.get("did") and row.get("handle") and not t:
        return "user"
    return "other"


def _post_id_from_uri(uri: Optional[str]) -> Optional[str]:
    """
    Bluesky URIs look like: at://<did>/<collection>/<rkey>
    We store the record key (rkey) as post_id for stability across dumps.
    """
    if not uri:
        return None
    try:
        parts = uri.split("/")
        return parts[-1] if parts else None
    except Exception:
        return None


def _did_from_uri(uri: Optional[str]) -> Optional[str]:
    # at://<did>/...
    if not uri:
        return None
    try:
        parts = uri.split("/")
        if len(parts) >= 3 and parts[0].startswith("at:"):
            return parts[2] if parts[2].startswith("did:") else parts[2]
        if len(parts) >= 3 and parts[0] == "at:" and parts[2].startswith("did:"):
            return parts[2]
    except Exception:
        pass
    return None


def _emit_facets_entities(
    post_id: str,
    account_id: Optional[str],
    created_at: datetime,
    retrieved_at: datetime,
    obj: Mapping[str, Any],
) -> Iterable[Entities]:
    # Richtext facets (tags/mentions/links)
    for facet in obj.get("facets", []) or []:
        for feat in facet.get("features", []) or []:
            ftype = feat.get("$type")
            if ftype == "app.bsky.richtext.facet#tag":
                tag = _s(feat.get("tag")).lstrip("#")
                if tag:
                    yield Entities(
                        created_at=created_at,
                        retrieved_at=retrieved_at,
                        entity_type=EntityType.HASHTAG,
                        account_id=account_id,
                        post_id=post_id,
                        body=tag,
                    )
            elif ftype == "app.bsky.richtext.facet#mention":
                did = _as_id(feat.get("did"))
                if did:
                    yield Entities(
                        created_at=created_at,
                        retrieved_at=retrieved_at,
                        entity_type=EntityType.USER_TAG,
                        account_id=account_id,
                        post_id=post_id,
                        body=did,
                    )
            elif ftype == "app.bsky.richtext.facet#link":
                uri = _s(feat.get("uri"))
                if uri:
                    yield Entities(
                        created_at=created_at,
                        retrieved_at=retrieved_at,
                        entity_type=EntityType.LINK,
                        account_id=account_id,
                        post_id=post_id,
                        body=uri,
                    )
    # Embeds (images)
    emb = obj.get("embed") or {}
    if emb.get("$type") == "app.bsky.embed.images":
        for img in emb.get("images", []) or []:
            # Bluesky image objects often have 'fullsize' and 'thumb'
            url = img.get("fullsize") or img.get("thumb")
            if url:
                yield Entities(
                    created_at=created_at,
                    retrieved_at=retrieved_at,
                    entity_type=EntityType.IMAGE,
                    account_id=account_id,
                    post_id=post_id,
                    body=_s(url),
                )


def _emit_post_relationship_actions(
    obj: Mapping[str, Any],
    author_did: Optional[str],
    created_at: datetime,
    retrieved_at: datetime,
) -> Iterable[Actions]:
    """
    From a post record, infer COMMENT and QUOTE actions:
      - reply.parent.uri  → COMMENT (originator=author_did, target_post=parent rkey)
      - embed.record.uri  → QUOTE   (originator=author_did, target_post=rkey)
    Target account ids can sometimes be derived from uri's DID; set when available.
    """
    if not author_did:
        return
    # reply -> COMMENT
    reply = obj.get("reply") or {}
    parent_uri = (reply.get("parent") or {}).get("uri")
    if parent_uri:
        tgt_post = _post_id_from_uri(parent_uri)
        tgt_acc = _did_from_uri(parent_uri)
        if tgt_post:
            yield Actions(
                created_at=created_at,
                retrieved_at=retrieved_at,
                action_type=ActionType.COMMENT,
                originator_account_id=author_did,
                originator_post_id=None,
                target_account_id=tgt_acc,
                target_post_id=tgt_post,
            )
    # quote -> QUOTE
    emb = obj.get("embed") or {}
    if emb.get("$type") == "app.bsky.embed.record":
        rec_uri = (emb.get("record") or {}).get("uri")
        if rec_uri:
            tgt_post = _post_id_from_uri(rec_uri)
            tgt_acc = _did_from_uri(rec_uri)
            if tgt_post or tgt_acc:
                yield Actions(
                    created_at=created_at,
                    retrieved_at=retrieved_at,
                    action_type=ActionType.QUOTE,
                    originator_account_id=author_did,
                    originator_post_id=None,
                    target_account_id=tgt_acc,
                    target_post_id=tgt_post,
                )


# ---------------- main standardizer ----------------


@dataclass
class BlueSkyAPIStandardizer(Standardizer):
    """
    Handles Bluesky API-style NDJSON streams and user-profile dumps.

    Emits:
      - Accounts: for user rows; also minimal accounts from stream rows when we only know actor DID
      - Posts: for app.bsky.feed.post
      - Entities: facets (tags/mentions/links) and embedded images
      - Actions: COMMENT/QUOTE from posts; SHARE/UPVOTE from repost/like events
    """

    name: str = "bluesky_api"

    def standardize(self, input_record) -> Iterable[Any]:
        record, src = input_record

        outputs = []

        retrieved_at = None
        kind = _guess_kind(src, record)

        # -------- USERS (profiles) --------
        if kind == "user":
            did = _as_id(record.get("did"))
            if not did:
                return outputs
            created = _dt_iso(record.get("createdAt")) or retrieved_at
            outputs.append(
                Accounts(
                    created_at=created,
                    retrieved_at=retrieved_at,
                    account_id=did,
                    username=_s(record.get("handle")) or None,
                    profile_name=_s(record.get("displayName")) or None,
                    bio=_s(record.get("description")) or None,
                    post_count=record.get("postsCount"),
                    friend_count=record.get("followsCount"),
                    follower_count=record.get("followersCount"),
                    is_verified=None,  # Bluesky has labels; map if you wish
                    profile_image_url=_s(record.get("avatar")) or None,
                )
            )
            return outputs

        # -------- POSTS --------
        if kind == "post":
            author = _as_id(record.get("author"))
            uri = _s(record.get("uri")) or None
            post_id = _post_id_from_uri(uri)
            created = _dt_iso(record.get("createdAt")) or retrieved_at
            text = _s(record.get("text"))

            if post_id and author:
                outputs.append(
                    Posts(
                        created_at=created,
                        retrieved_at=retrieved_at,
                        post_id=post_id,
                        account_id=author,
                        conversation_id=None,
                        body=text,
                        like_count=None,
                        view_count=None,
                        share_count=None,
                        comment_count=None,
                        quote_count=None,
                        bookmark_count=None,
                        location=None,
                    )
                )

                # entities from facets & embeds
                for ent in _emit_facets_entities(
                    post_id, author, created, retrieved_at, record
                ):
                    outputs.append(ent)

                # COMMENT / QUOTE inferred from reply/embed
                for act in _emit_post_relationship_actions(
                    record, author, created, retrieved_at
                ):
                    outputs.append(act)
            return outputs

        # -------- REPOST --------
        if kind == "repost":
            actor = _as_id(record.get("author"))
            created = _dt_iso(record.get("createdAt")) or retrieved_at
            subj = record.get("subject") or {}
            tgt_uri = _as_id(subj.get("uri"))
            tgt_post = _post_id_from_uri(tgt_uri)
            tgt_acc = _did_from_uri(tgt_uri)

            if actor and (tgt_post or tgt_acc):
                outputs.append(
                    Actions(
                        created_at=created,
                        retrieved_at=retrieved_at,
                        action_type=ActionType.SHARE,
                        originator_account_id=actor,
                        originator_post_id=None,
                        target_account_id=tgt_acc,
                        target_post_id=tgt_post,
                    )
                )
            return outputs

        # -------- LIKE --------
        if kind == "like":
            actor = _as_id(record.get("author"))
            created = _dt_iso(record.get("createdAt")) or retrieved_at
            subj = record.get("subject") or {}
            tgt_uri = _as_id(subj.get("uri"))
            tgt_post = _post_id_from_uri(tgt_uri)
            tgt_acc = _did_from_uri(tgt_uri)

            if actor and (tgt_post or tgt_acc):
                outputs.append(
                    Actions(
                        created_at=created,
                        retrieved_at=retrieved_at,
                        action_type=ActionType.UPVOTE,
                        originator_account_id=actor,
                        originator_post_id=None,
                        target_account_id=tgt_acc,
                        target_post_id=tgt_post,
                    )
                )
            return outputs

        if "user" in src.path:
            # sometimes user dumps have no $type but include did/handle
            did = _as_id(record.get("did"))
            if not did:
                return outputs
            created_at = _dt_iso(record.get("createdAt")) or retrieved_at
            outputs.append(
                Accounts(
                    created_at=created_at,
                    retrieved_at=retrieved_at,
                    account_id=did,
                    username=_s(record.get("handle")) or None,
                    profile_name=_s(record.get("displayName")) or None,
                    bio=_s(record.get("description")) or None,
                    post_count=record.get("postsCount"),
                    friend_count=record.get("followsCount"),
                    follower_count=record.get("followersCount"),
                    is_verified=None,
                    profile_image_url=_s(record.get("avatar")) or None,
                )
            )
            return outputs

        # -------- OTHER / UNKNOWN --------
        return outputs
