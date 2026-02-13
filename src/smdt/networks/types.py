"""
Type definitions for network results.
"""

from dataclasses import dataclass
from typing import Any, Dict
import pandas as pd


@dataclass
class NetworkResult:
    """Container for any built network.

    Attributes:
        nodes: DataFrame containing node information (node_id, label, type, etc.).
        edges: DataFrame containing edge information (src, dst, weight, edge_type, etc.).
        meta: Dictionary containing metadata about the network construction.
    """

    nodes: pd.DataFrame
    edges: pd.DataFrame
    meta: Dict[str, Any]
