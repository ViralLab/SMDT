from __future__ import annotations

import gzip
import json
import pickle
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
from dateutil import parser

from smdt.enrichers.base import BaseEnricher
from smdt.enrichers.registry import register
from smdt.store.models.account_enrichments import AccountEnrichments
from smdt.store.standard_db import StandardDB

_MODEL_ID = "botometer_v1"


@dataclass
class BotometerConfig:
    only_missing: bool = True
    reset_cache: bool = False
    cache_dir: Optional[str] = None
    model_path: Optional[str] = None  # defaults to model.pkl.gz next to this file
    do_save_to_db: bool = True
    output_dir: Optional[str] = None  # required when do_save_to_db=False

    def __post_init__(self) -> None:
        if not self.do_save_to_db:
            if not self.output_dir:
                raise ValueError("output_dir is required when do_save_to_db=False")
            Path(self.output_dir).mkdir(parents=True, exist_ok=True)


@register(
    _MODEL_ID,
    target="accounts",
    description="Botometer-style bot detection score for accounts",
    requires=["numpy", "dateutil"],
)
class BotometerEnricher(BaseEnricher):
    """
    Scores accounts with a pre-trained botometer sklearn model and writes to
    account_enrichments.  JSONB payload: {"bot_score": float}
    """

    _TARGET = "accounts"
    _ENRICHER_ID = _MODEL_ID

    def __init__(self, db: StandardDB, *, config: Optional[Dict[str, Any]] = None):
        super().__init__(db, config=config)
        self.cfg = config if isinstance(config, BotometerConfig) else BotometerConfig(**(config or {}))
        self.MODEL_ID = _MODEL_ID
        self.applied_datetime = datetime.now(timezone.utc)
        self.model = None

        if self.cfg.reset_cache:
            self.cached_ids = set()
            self.reset_cache()
        else:
            self.cached_ids = set(self.load_cached_output_ids_from_file())

    def load_model(self) -> None:
        model_path = self.cfg.model_path or str(Path(__file__).parent / "model.pkl.gz")
        with gzip.open(model_path, "rb") as f:
            self.model = pickle.load(f)

    # ---- selection ---------------------------------------------------------

    def total_count(self) -> Optional[int]:
        where_clauses: List[str] = []
        params: List[Any] = []

        if self.cfg.only_missing:
            where_clauses.append(
                "NOT EXISTS (SELECT 1 FROM account_enrichments ae"
                " WHERE ae.account_id = a.account_id AND ae.model_id = %s)"
            )
            params.append(self.MODEL_ID)

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        q = f"SELECT COUNT(DISTINCT a.account_id) FROM accounts a {where_sql}"

        conn = self.db.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(q, params)
                row = cur.fetchone()
                return int(row[0]) if row else 0
        finally:
            conn.close()

    def fetch_batch(self, offset: int, limit: int) -> List[Dict[str, Any]]:
        where_clauses: List[str] = []
        params: List[Any] = []

        if self.cfg.only_missing:
            where_clauses.append(
                "NOT EXISTS (SELECT 1 FROM account_enrichments ae"
                " WHERE ae.account_id = a.account_id AND ae.model_id = %s)"
            )
            params.append(self.MODEL_ID)

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        # DISTINCT ON picks the most-recent snapshot per account_id.
        # post_count from accounts is a denormalised counter used as fallback.
        q = f"""
        WITH account_batch AS (
            SELECT DISTINCT ON (a.account_id)
                a.account_id,
                a.username          AS account_name,
                a.profile_name,
                a.created_at        AS creation_timestamp,
                a.friend_count,
                a.follower_count,
                a.post_count        AS account_post_count
            FROM accounts a
            {where_sql}
            ORDER BY a.account_id, a.created_at DESC
            LIMIT %s OFFSET %s
        ),
        post_aggregates AS (
            SELECT
                p.account_id,
                COUNT(*)                        AS post_count,
                ARRAY_AGG(p.created_at)         AS post_creation_timestamps,
                AVG(LENGTH(p.body))             AS avg_content_length
            FROM posts p
            WHERE p.account_id IN (SELECT account_id FROM account_batch)
            GROUP BY p.account_id
        ),
        action_aggregates AS (
            SELECT
                act.originator_account_id                                       AS account_id,
                COUNT(CASE WHEN act.action_type = 'SHARE'   THEN 1 END)        AS share_action_count,
                COUNT(CASE WHEN act.action_type = 'COMMENT' THEN 1 END)        AS comment_action_count,
                COUNT(act.target_account_id)                                    AS count_target_ids,
                COUNT(DISTINCT act.target_account_id)                           AS unique_target_ids
            FROM actions act
            WHERE act.originator_account_id IN (SELECT account_id FROM account_batch)
            GROUP BY act.originator_account_id
        ),
        mention_aggregates AS (
            SELECT
                e.account_id,
                COUNT(*) AS mention_count
            FROM entities e
            WHERE e.entity_type = 'USER_TAG'
              AND e.account_id IN (SELECT account_id FROM account_batch)
            GROUP BY e.account_id
        )
        SELECT
            ab.account_id,
            ab.account_name,
            ab.profile_name,
            ab.creation_timestamp,
            ab.friend_count,
            ab.follower_count,
            COALESCE(pa.post_count, ab.account_post_count, 0)              AS post_count,
            COALESCE(pa.post_creation_timestamps, ARRAY[]::timestamptz[])  AS post_creation_timestamps,
            COALESCE(pa.avg_content_length, 0)                             AS avg_content_length,
            COALESCE(aa.share_action_count, 0)                             AS share_action_count,
            COALESCE(aa.comment_action_count, 0)                           AS comment_action_count,
            COALESCE(aa.count_target_ids, 0)                               AS count_target_ids,
            COALESCE(aa.unique_target_ids, 0)                              AS unique_target_ids,
            COALESCE(ma.mention_count, 0)                                  AS mention_count
        FROM account_batch ab
        LEFT JOIN post_aggregates pa   ON ab.account_id = pa.account_id
        LEFT JOIN action_aggregates aa ON ab.account_id = aa.account_id
        LEFT JOIN mention_aggregates ma ON ab.account_id = ma.account_id
        """
        params.extend([limit, offset])

        conn = self.db.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(q, params)
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]
        finally:
            conn.close()

    # ---- inference ---------------------------------------------------------

    def process_batch(self, rows: List[Dict[str, Any]]) -> List[AccountEnrichments]:
        if not rows:
            return []

        out: List[AccountEnrichments] = []
        for row in rows:
            features = self._extract_features(row)
            fname = sorted(features.keys())
            fvec = np.array([features[f] for f in fname])
            bot_score = float(self.model.predict_proba(fvec.reshape(1, -1))[:, 1][0])
            out.append(
                AccountEnrichments(
                    model_id=self.MODEL_ID,
                    account_id=row["account_id"],
                    body={"bot_score": bot_score},
                    created_at=self.applied_datetime,
                    retrieved_at=self.applied_datetime,
                )
            )
        return out

    def _extract_features(self, user_obj: Dict[str, Any], t_anchor: Optional[int] = None) -> Dict[str, float]:
        features: Dict[str, float] = {
            "account_age_days": 0,
            "account_name_length": 0,
            "avg_content_length": 0,
            "ff_ratio": 1.0,
            "follower_count": 0,
            "friend_count": 0,
            "mention_count": 0,
            "post_count": 0,
            "post_count_verified": 0,
            "post_frequency": 0,
            "post_per_day": 0,
            "profile_name_length": 0,
            "share_count": 0,
            "unique_targets": 0,
            "unique_targets_ratio": 0,
        }

        if user_obj is None:
            return features

        if t_anchor is None:
            t_anchor = int(time.time())

        creation = user_obj.get("creation_timestamp")
        if creation is not None:
            try:
                created_at = (
                    creation.timestamp()
                    if isinstance(creation, datetime)
                    else time.mktime(parser.parse(str(creation)).timetuple())
                )
            except Exception:
                created_at = float(t_anchor)
        else:
            created_at = float(t_anchor)

        friends_count = int(user_obj.get("friend_count") or 0)
        followers_count = int(user_obj.get("follower_count") or 0)
        post_count = int(user_obj.get("post_count") or 0)

        features["account_age_days"] = max(1.0, (t_anchor - created_at) / 86400.0)
        features["post_count"] = post_count
        features["post_count_verified"] = post_count
        features["friend_count"] = friends_count
        features["follower_count"] = followers_count
        features["ff_ratio"] = (friends_count + 1) / (followers_count + 1)
        features["post_per_day"] = post_count / features["account_age_days"]
        features["account_name_length"] = len(user_obj.get("account_name") or "")
        features["profile_name_length"] = len(user_obj.get("profile_name") or "")
        features["avg_content_length"] = float(user_obj.get("avg_content_length") or 0)
        features["share_count"] = int(user_obj.get("share_action_count") or 0)
        features["mention_count"] = int(user_obj.get("mention_count") or 0)
        features["unique_targets"] = int(user_obj.get("unique_target_ids") or 0)
        features["unique_targets_ratio"] = features["unique_targets"] / (
            int(user_obj.get("count_target_ids") or 0) + 1
        )

        post_timestamps = user_obj.get("post_creation_timestamps") or []
        if post_count >= 2 and post_timestamps:
            try:
                parsed_ts = [
                    t if isinstance(t, datetime) else parser.parse(str(t))
                    for t in post_timestamps
                ]
                if len(parsed_ts) >= 2:
                    earliest = min(parsed_ts)
                    latest = max(parsed_ts)
                    span_days = (latest - earliest).total_seconds() / 86400.0 + 1
                    features["post_frequency"] = len(parsed_ts) / span_days
            except Exception:
                pass

        return features

    # ---- persistence -------------------------------------------------------

    def save_results(self, results: List[AccountEnrichments]) -> None:
        if not results:
            return

        if self.cfg.do_save_to_db:
            self.db.insert_with_fallbacks(results)
            return

        output_base = Path(self.cfg.output_dir)
        open_files: Dict[str, Any] = {}
        try:
            for r in results:
                date_str = r.created_at.strftime("%Y-%m-%d")
                if date_str not in open_files:
                    outp = output_base / f"{self.MODEL_ID}_{date_str}.jsonl"
                    open_files[date_str] = outp.open("a", encoding="utf-8")
                rec = {
                    "account_id": r.account_id,
                    "model_id": r.model_id,
                    "body": r.body,
                    "created_at": r.created_at.isoformat(),
                    "retrieved_at": r.retrieved_at.isoformat() if r.retrieved_at else None,
                }
                open_files[date_str].write(json.dumps(rec, ensure_ascii=False) + "\n")
        finally:
            for f in open_files.values():
                f.close()
