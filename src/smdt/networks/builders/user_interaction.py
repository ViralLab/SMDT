from typing import Any, Dict, LiteralString, Tuple
import pandas as pd

from smdt.networks.base import NetworkBuilder


class UserInteractionNetworkBuilder(NetworkBuilder):
    """
    Build user–user interaction networks based on the Actions table.

    Edge: originator_account_id → target_account_id
    Weight: number of occurrences of a given action_type.

    Supported action types (case-insensitive):
      - FOLLOW
      - UNFOLLOW
      - SHARE
      - QUOTE
      - COMMENT
      - UPVOTE
      - DOWNVOTE
      - BLOCK
    """

    def _edge_query(self) -> Tuple[str, Dict[str, Any]]:
        """Construct the SQL query for user interaction edges.

        Returns:
            Tuple of (sql_query, parameters).
        """
        filters: Dict[str, Any] = self.spec.filters
        action_type = self.spec.edge_kind.upper()

        start_time = filters.get("start_time")
        end_time = filters.get("end_time")

        time_clause = ""
        if start_time is not None:
            time_clause += " AND created_at >= %(start_time)s"
        if end_time is not None:
            time_clause += " AND created_at < %(end_time)s"

        sql = f"""
        SELECT
            originator_account_id AS src,
            target_account_id     AS dst,
            COUNT(*)              AS weight
        FROM actions
        WHERE action_type = %(action_type)s
        AND originator_account_id IS NOT NULL
        AND target_account_id IS NOT NULL
        {time_clause}
        GROUP BY originator_account_id, target_account_id
        """

        params: Dict[str, Any] = {"action_type": action_type}
        if start_time is not None:
            params["start_time"] = start_time
        if end_time is not None:
            params["end_time"] = end_time

        return sql, params

    def _query_edges(self) -> pd.DataFrame:
        """Run the query and add edge types.

        Returns:
            DataFrame of edges with columns: src, dst, weight, edge_type.
        """
        df = super()._query_edges()

        if df.empty:
            return df

        # Weighting mode
        if self.spec.weighting == "binary":
            df["weight"] = 1

        df["edge_type"] = self.spec.edge_kind.upper()
        return df[["src", "dst", "weight", "edge_type"]]
