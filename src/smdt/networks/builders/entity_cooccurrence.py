from typing import Any, Dict, Tuple

import pandas as pd

from smdt.networks.base import NetworkBuilder


class EntityCooccurrenceNetworkBuilder(NetworkBuilder):
    """
    Build entity–entity co-occurrence networks using the `entities` table.

    Assumptions (from smdt.store.models.Entities):
      - Table name: entities
      - Columns:
          id, account_id, post_id, body, entity_type, created_at, retrieved_at
      - entity_type is an enum, e.g. HASHTAG, USER_TAG, LINK, EMAIL, IMAGE, VIDEO.
      - body stores the actual value (hashtag text, URL, mention, etc.).

    Semantics:
      - Nodes: distinct entity `body` values of the chosen `entity_type`.
      - Edge: (e1, e2) exists if e1 and e2 co-occur in at least one post (same post_id).
      - Weight: number of posts in which the pair co-occurs
                (after de-duplicating (post_id, body) pairs).
    """

    def _edge_query(self) -> Tuple[str, Dict[str, Any]]:
        filters: Dict[str, Any] = self.spec.filters

        # Example: "hashtag" -> "HASHTAG" (matches DB enum values)
        raw_type = filters.get("entity_type", "HASHTAG")
        entity_type = raw_type.upper()

        # Use psycopg/DB-API named placeholders: %(name)s
        time_clause = ""
        if "start_time" in filters:
            time_clause += " AND created_at >= %(start_time)s"
        if "end_time" in filters:
            time_clause += " AND created_at < %(end_time)s"

        # 1) Filter entities by type/time and deduplicate (post_id, body)
        # 2) Self-join on same post, body1 < body2 to get unordered pairs
        # 3) Count number of posts in which each pair appears
        sql = f"""
        WITH filtered_entities AS (
            SELECT DISTINCT
                post_id,
                body
            FROM entities
            WHERE entity_type = %(entity_type)s
              AND body IS NOT NULL
              {time_clause}
        ),
        pairs AS (
            SELECT
                LEAST(e1.body, e2.body)    AS src,
                GREATEST(e1.body, e2.body) AS dst,
                COUNT(*)                   AS weight
            FROM filtered_entities e1
            JOIN filtered_entities e2
              ON e1.post_id = e2.post_id
             AND e1.body < e2.body
            GROUP BY
                LEAST(e1.body, e2.body),
                GREATEST(e1.body, e2.body)
        )
        SELECT src, dst, weight
        FROM pairs
        """

        params: Dict[str, Any] = {"entity_type": entity_type}
        for k in ("start_time", "end_time"):
            if k in filters:
                params[k] = filters[k]

        return sql, params

    def _query_edges(self) -> pd.DataFrame:
        df = super()._query_edges()
        if df.empty:
            return df

        # Weighting mode
        if self.spec.weighting == "binary":
            df["weight"] = 1

        entity_type = self.spec.filters.get("entity_type", "HASHTAG").upper()
        df["edge_type"] = f"{entity_type}_COOCCURRENCE"
        return df[["src", "dst", "weight", "edge_type"]]

    def _derive_nodes(self, edges: pd.DataFrame) -> pd.DataFrame:
        if edges.empty:
            return pd.DataFrame(columns=["node_id", "label", "type"])

        all_vals = pd.unique(pd.concat([edges["src"], edges["dst"]], ignore_index=True))

        entity_type = self.spec.filters.get("entity_type", "HASHTAG").upper()
        return pd.DataFrame(
            {
                "node_id": all_vals,
                "label": all_vals,
                "type": entity_type,
            }
        )
