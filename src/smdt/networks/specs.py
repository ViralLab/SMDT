from dataclasses import dataclass, field
from typing import Any, Dict, Literal

Weighting = Literal["binary", "count", "normalized", "custom"]


@dataclass
class NetworkSpec:
    """Configuration object describing how to build a network."""

    name: str
    node_type: str  # "account", "entity", "post", etc.
    edge_kind: str  # "reply", "quote", "cooccurrence", ...
    directed: bool = True
    weighting: Weighting = "count"
    filters: Dict[str, Any] = field(default_factory=dict)
