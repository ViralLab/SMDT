from typing import Any, Dict, Tuple
import pandas as pd

from smdt.networks.base import NetworkBuilder


class CoActionNetworkBuilder(NetworkBuilder):
    """
    Build user–user co-action networks (e.g., co-comment, co-quote, co-share).

    Definition
    -----------
    Two users are connected if they both performed the same action_type
    (COMMENT, QUOTE, SHARE, etc.) on the same target_post_id.

    This captures "shared engagement" relationships — users reacting to
    the same content in similar ways.

    Edge semantics
    ---------------
      src, dst : account_id pairs (unordered)
      weight   : number of posts both users interacted with
      edge_type: CO_<ACTION_TYPE>

    Example:
      A → Post123 (COMMENT)
      B → Post123 (COMMENT)
        => edge (A, B) in the CO_COMMENT network
    """

    def _edge_query(self) -> Tuple[str, Dict[str, Any]]:
        filters: Dict[str, Any] = self.spec.filters
        action_type = self.spec.edge_kind.upper()

        start_time = filters.get("start_time")
        end_time = filters.get("end_time")

        # Construct time filter only when values are not None
        time_clause = ""
        if start_time is not None:
            time_clause += " AND created_at >= %(start_time)s"
        if end_time is not None:
            time_clause += " AND created_at < %(end_time)s"

        sql = f"""
        WITH filtered_actions AS (
            SELECT DISTINCT originator_account_id, target_post_id
            FROM actions
            WHERE action_type = %(action_type)s
              AND originator_account_id IS NOT NULL
              AND target_post_id IS NOT NULL
              {time_clause}
        ),
        pairs AS (
            SELECT
                LEAST(a1.originator_account_id, a2.originator_account_id)  AS src,
                GREATEST(a1.originator_account_id, a2.originator_account_id) AS dst,
                COUNT(*) AS weight
            FROM filtered_actions a1
            JOIN filtered_actions a2
              ON a1.target_post_id = a2.target_post_id
             AND a1.originator_account_id < a2.originator_account_id
            GROUP BY
                LEAST(a1.originator_account_id, a2.originator_account_id),
                GREATEST(a1.originator_account_id, a2.originator_account_id)
        )
        SELECT src, dst, weight
        FROM pairs
        """

        params: Dict[str, Any] = {"action_type": action_type}
        if start_time is not None:
            params["start_time"] = start_time
        if end_time is not None:
            params["end_time"] = end_time

        return sql, params

    def _query_edges(self) -> pd.DataFrame:
        """Run the query and ensure correct schema + weight normalization."""
        df = super()._query_edges()
        if df.empty:
            return df

        # Apply weighting mode
        if self.spec.weighting == "binary":
            df["weight"] = 1

        df["edge_type"] = f"CO_{self.spec.edge_kind.upper()}"
        return df[["src", "dst", "weight", "edge_type"]]

    def _derive_nodes(self, edges: pd.DataFrame) -> pd.DataFrame:
        """Derive node table: distinct user IDs."""
        if edges.empty:
            return pd.DataFrame(columns=["node_id", "type"])

        node_ids = pd.unique(pd.concat([edges["src"], edges["dst"]], ignore_index=True))
        return pd.DataFrame({"node_id": node_ids, "type": "ACCOUNT"})
