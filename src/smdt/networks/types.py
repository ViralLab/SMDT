from dataclasses import dataclass
from typing import Any, Dict
import pandas as pd


@dataclass
class NetworkResult:
    """Container for any built network."""

    nodes: pd.DataFrame  # node_id, label, type, ...
    edges: pd.DataFrame  # src, dst, weight, edge_type, ...
    meta: Dict[str, Any]  # metadata such as node_count, edge_count, filters
