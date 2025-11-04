from typing import Any, Dict, Tuple
import pandas as pd

from smdt.networks.base import NetworkBuilder


class BipartiteNetworkBuilder(NetworkBuilder):
    """
    Build bipartite networks such as:
      - account–entity (e.g., user–hashtag, user–link, user–mention)
      - post–entity   (e.g., post–hashtag, post–link)

    Tables involved:
      - posts(post_id, account_id, created_at, retrieved_at, ...)
      - entities(id, account_id, post_id, body, entity_type, created_at, retrieved_at)

    Edge semantics:
      - account–entity: (account_id, entity_body) if the account authored a post containing that entity
      - post–entity: (post_id, entity_body) if the post contains that entity
      - weight: number of posts in which the pair co-occurs (or 1 if binary)
    """

    def _edge_query(self) -> Tuple[str, Dict[str, Any]]:
        filters: Dict[str, Any] = self.spec.filters
        left = filters.get("left", "account").lower()
        right = filters.get("right", "hashtag").lower()

        entity_type = (
            right.upper()
            if right in ["hashtag", "user_tag", "link", "email", "image", "video"]
            else None
        )

        if entity_type is None:
            raise NotImplementedError(
                f"Bipartite network not implemented for right='{right}'"
            )

        # Optional time filters – use psycopg style placeholders: %(name)s
        time_clause = ""
        if "start_time" in filters:
            time_clause += " AND p.created_at >= %(start_time)s"
        if "end_time" in filters:
            time_clause += " AND p.created_at < %(end_time)s"

        # ------------------------------------------------------
        # CASE 1: account–entity (most common)
        # ------------------------------------------------------
        if left == "account":
            sql = f"""
            SELECT
                p.account_id AS src,
                e.body        AS dst,
                COUNT(*)      AS weight
            FROM posts p
            JOIN entities e ON p.post_id = e.post_id
            WHERE e.entity_type = %(entity_type)s
              AND e.body IS NOT NULL
              {time_clause}
            GROUP BY p.account_id, e.body
            """
            params: Dict[str, Any] = {"entity_type": entity_type}

        # ------------------------------------------------------
        # CASE 2: post–entity
        # ------------------------------------------------------
        elif left == "post":
            sql = f"""
            SELECT
                p.post_id AS src,
                e.body    AS dst,
                COUNT(*)  AS weight
            FROM posts p
            JOIN entities e ON p.post_id = e.post_id
            WHERE e.entity_type = %(entity_type)s
              AND e.body IS NOT NULL
              {time_clause}
            GROUP BY p.post_id, e.body
            """
            params = {"entity_type": entity_type}

        else:
            raise NotImplementedError(
                f"Bipartite network not implemented for left='{left}', right='{right}'"
            )

        # Add time params if present
        for k in ("start_time", "end_time"):
            if k in filters:
                params[k] = filters[k]

        return sql, params

    def _query_edges(self) -> pd.DataFrame:
        df = super()._query_edges()
        if df.empty:
            return df

        if self.spec.weighting == "binary":
            df["weight"] = 1

        left = self.spec.filters.get("left", "account").upper()
        right = self.spec.filters.get("right", "ENTITY").upper()
        df["edge_type"] = f"{left}_{right}_BIPARTITE"

        return df[["src", "dst", "weight", "edge_type"]]

    def _derive_nodes(self, edges: pd.DataFrame) -> pd.DataFrame:
        if edges.empty:
            return pd.DataFrame(columns=["node_id", "bipartite", "type"])

        left_label = self.spec.filters.get("left", "account").upper()
        right_label = self.spec.filters.get("right", "ENTITY").upper()

        left_nodes = edges[["src"]].drop_duplicates().rename(columns={"src": "node_id"})
        left_nodes["bipartite"] = 0
        left_nodes["type"] = left_label

        right_nodes = (
            edges[["dst"]].drop_duplicates().rename(columns={"dst": "node_id"})
        )
        right_nodes["bipartite"] = 1
        right_nodes["type"] = right_label

        return pd.concat([left_nodes, right_nodes], ignore_index=True)
