from abc import ABC, abstractmethod
from typing import Any, Dict, Tuple

import pandas as pd

from smdt.store.standard_db import StandardDB
from .specs import NetworkSpec
from .types import NetworkResult


class NetworkBuilder(ABC):
    """Base class for all network builders.

    Assumptions about StandardDB:
        - db.connect() -> context manager yielding a DB-API connection
    """

    def __init__(self, db: StandardDB, spec: NetworkSpec):
        """Initialize the NetworkBuilder.

        Args:
            db: StandardDB instance.
            spec: NetworkSpec instance.
        """
        self.db = db
        self.spec = spec

    @abstractmethod
    def _edge_query(self) -> Tuple[str, Dict[str, Any]]:
        """Return (sql, params) defining the edge query.

        SQL should select at least: src, dst, weight.

        Returns:
            Tuple of (sql_query, parameters).
        """
        ...

    def _query_edges(self) -> pd.DataFrame:
        """Run the edge query and return a DataFrame.

        Uses only a DB-API connection (psycopg) and a cursor.

        Returns:
            DataFrame containing edges.
        """
        sql, params = self._edge_query()
        with self.db.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
                desc = cur.description

        if not rows or desc is None:
            return pd.DataFrame(columns=["src", "dst", "weight", "edge_type"])

        colnames = [c[0] for c in desc]
        df = pd.DataFrame(rows, columns=colnames)

        if df.empty:
            return pd.DataFrame(columns=["src", "dst", "weight", "edge_type"])

        return df

    def build(self) -> NetworkResult:
        """Build and return the complete network.

        Returns:
            NetworkResult object.
        """
        edges = self._query_edges()
        nodes = self._derive_nodes(edges)
        meta = self._summarize(nodes, edges)
        return NetworkResult(nodes=nodes, edges=edges, meta=meta)

    def _derive_nodes(self, edges: pd.DataFrame) -> pd.DataFrame:
        """Default node extraction: unique src ∪ dst.

        Args:
            edges: DataFrame of edges.

        Returns:
            DataFrame of nodes with 'node_id' column.
        """
        if edges.empty:
            return pd.DataFrame(columns=["node_id"])

        node_ids = pd.unique(pd.concat([edges["src"], edges["dst"]], ignore_index=True))
        return pd.DataFrame({"node_id": node_ids})

    def _summarize(self, nodes: pd.DataFrame, edges: pd.DataFrame) -> Dict[str, Any]:
        """Compute basic metadata about the network.

        Args:
            nodes: DataFrame of nodes.
            edges: DataFrame of edges.

        Returns:
            Dictionary of metadata.
        """
        return {
            "name": self.spec.name,
            "node_type": self.spec.node_type,
            "edge_kind": self.spec.edge_kind,
            "node_count": int(len(nodes)),
            "edge_count": int(len(edges)),
            "directed": self.spec.directed,
            "weighting": self.spec.weighting,
            "filters": self.spec.filters,
        }
