"""
Data structures for defining network specifications.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Literal

Weighting = Literal["binary", "count", "normalized", "custom"]


@dataclass
class NetworkSpec:
    """Configuration object describing how to build a network.

    Attributes:
        name: Name of the network.
        node_type: Type of nodes (e.g., "account", "entity", "post").
        edge_kind: Kind of edges (e.g., "reply", "quote", "cooccurrence").
        directed: Whether the network is directed.
        weighting: Method for edge weighting.
        filters: Dictionary of filters to apply during construction.
    """

    name: str
    node_type: str
    edge_kind: str
    directed: bool = True
    weighting: Weighting = "count"
    filters: Dict[str, Any] = field(default_factory=dict)
