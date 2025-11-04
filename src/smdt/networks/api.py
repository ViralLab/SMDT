from datetime import datetime
from typing import Literal, Optional, Iterator, Dict, Any
import pandas as pd

from smdt.store.standard_db import StandardDB
from .specs import NetworkSpec
from .types import NetworkResult
from .streams import iter_edge_chunks
from .builders import (
    UserInteractionNetworkBuilder,
    EntityCooccurrenceNetworkBuilder,
    BipartiteNetworkBuilder,
    CoActionNetworkBuilder,
)

# ---------------------------------------------------------------------
# Shared configuration
# ---------------------------------------------------------------------
Weighting = Literal["binary", "count"]

_ALLOWED_INTERACTIONS = {"QUOTE", "SHARE", "COMMENT", "FOLLOW", "BLOCK"}
_ALLOWED_ENTITY_TYPES = {"HASHTAG", "USER_TAG", "LINK", "EMAIL", "IMAGE", "VIDEO"}


# ---------------------------------------------------------------------
# USER INTERACTION NETWORKS
# ---------------------------------------------------------------------
def user_interaction(
    db: StandardDB,
    *,
    interaction: str,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    weighting: Weighting = "count",
    min_weight: Optional[int] = None,
) -> NetworkResult:
    """
    Build a user–user interaction network.

    Supported interaction types (case-insensitive):
      QUOTE, SHARE, COMMENT, FOLLOW, BLOCK

    Nodes:
      - Accounts
    Edges:
      - (originator_account_id → target_account_id) if that interaction occurred.
    Weight:
      - Number of interactions (or 1 if weighting='binary').
    """
    interaction_norm = interaction.strip().upper()
    if interaction_norm not in _ALLOWED_INTERACTIONS:
        allowed_str = ", ".join(sorted(_ALLOWED_INTERACTIONS))
        raise ValueError(
            f"Invalid interaction='{interaction}'. Must be one of: {allowed_str}"
        )

    spec = NetworkSpec(
        name=f"user_{interaction_norm.lower()}_network",
        node_type="account",
        edge_kind=interaction_norm.lower(),
        directed=True,
        weighting=weighting,
        filters={
            "interaction": interaction_norm,
            "start_time": start_time,
            "end_time": end_time,
        },
    )

    builder = UserInteractionNetworkBuilder(db, spec)
    result = builder.build()

    if min_weight is not None and not result.edges.empty:
        result.edges = result.edges[result.edges["weight"] >= min_weight].reset_index(
            drop=True
        )

    return result


def iter_user_interaction_edges(
    db: StandardDB,
    *,
    interaction: str,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    chunksize: int = 100_000,
) -> Iterator[pd.DataFrame]:
    """
    Stream edges for a user–user interaction network.
    Yields DataFrame chunks with columns: src, dst, weight.
    """
    interaction_norm = interaction.strip().upper()
    if interaction_norm not in _ALLOWED_INTERACTIONS:
        allowed_str = ", ".join(sorted(_ALLOWED_INTERACTIONS))
        raise ValueError(
            f"Invalid interaction='{interaction}'. Must be one of: {allowed_str}"
        )

    spec = NetworkSpec(
        name=f"user_{interaction_norm.lower()}_network",
        node_type="account",
        edge_kind=interaction_norm.lower(),
        directed=True,
        filters={
            "interaction": interaction_norm,
            "start_time": start_time,
            "end_time": end_time,
        },
    )

    builder = UserInteractionNetworkBuilder(db, spec)
    return iter_edge_chunks(builder, chunksize=chunksize)


# ---------------------------------------------------------------------
# ENTITY COOCCURRENCE NETWORKS
# ---------------------------------------------------------------------
def entity_cooccurrence(
    db: StandardDB,
    *,
    entity_type: str,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    weighting: Weighting = "count",
    min_weight: Optional[int] = None,
) -> NetworkResult:
    """
    Build an entity–entity co-occurrence network.

    Supported entity types:
      HASHTAG, USER_TAG, LINK, EMAIL, IMAGE, VIDEO
    """
    entity_norm = entity_type.strip().upper()
    if entity_norm not in _ALLOWED_ENTITY_TYPES:
        allowed_str = ", ".join(sorted(_ALLOWED_ENTITY_TYPES))
        raise ValueError(
            f"Invalid entity_type='{entity_type}'. Must be one of: {allowed_str}"
        )

    spec = NetworkSpec(
        name=f"{entity_norm.lower()}_cooccurrence",
        node_type="entity",
        edge_kind="cooccurrence",
        directed=False,
        weighting=weighting,
        filters={
            "entity_type": entity_norm,
            "start_time": start_time,
            "end_time": end_time,
        },
    )

    builder = EntityCooccurrenceNetworkBuilder(db, spec)
    result = builder.build()

    if min_weight is not None and not result.edges.empty:
        result.edges = result.edges[result.edges["weight"] >= min_weight].reset_index(
            drop=True
        )

    return result


# ---------------------------------------------------------------------
# BIPARTITE NETWORKS
# ---------------------------------------------------------------------
def bipartite(
    db: StandardDB,
    *,
    left: Literal["account", "post"],
    right: str,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    weighting: Weighting = "count",
    min_weight: Optional[int] = None,
) -> NetworkResult:
    """
    Build a bipartite network.

    Supported combinations:
      - left='account', right in {'HASHTAG','USER_TAG','LINK','EMAIL','IMAGE','VIDEO'}
      - left='post',    right in {'HASHTAG','USER_TAG','LINK','EMAIL','IMAGE','VIDEO'}

    Nodes:
      - left nodes: accounts or posts
      - right nodes: entity bodies
    """
    left_norm = left.lower()
    right_norm = right.strip().upper()
    if right_norm not in _ALLOWED_ENTITY_TYPES:
        allowed_str = ", ".join(sorted(_ALLOWED_ENTITY_TYPES))
        raise ValueError(f"Invalid right='{right}'. Must be one of: {allowed_str}")

    spec = NetworkSpec(
        name=f"{left_norm}_{right_norm.lower()}_bipartite",
        node_type="bipartite",
        edge_kind="bipartite",
        directed=False,
        weighting=weighting,
        filters={
            "left": left_norm,
            "right": right_norm.lower(),
            "start_time": start_time,
            "end_time": end_time,
        },
    )

    builder = BipartiteNetworkBuilder(db, spec)
    result = builder.build()

    if min_weight is not None and not result.edges.empty:
        result.edges = result.edges[result.edges["weight"] >= min_weight].reset_index(
            drop=True
        )

    return result


def iter_bipartite_edges(
    db: StandardDB,
    *,
    left: Literal["account", "post"],
    right: str,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    chunksize: int = 100_000,
) -> Iterator[pd.DataFrame]:
    """
    Stream edges for a bipartite network in chunks.
    Yields DataFrames with columns: src, dst, weight.
    """
    left_norm = left.lower()
    right_norm = right.strip().upper()
    if right_norm not in _ALLOWED_ENTITY_TYPES:
        allowed_str = ", ".join(sorted(_ALLOWED_ENTITY_TYPES))
        raise ValueError(f"Invalid right='{right}'. Must be one of: {allowed_str}")

    spec = NetworkSpec(
        name=f"{left_norm}_{right_norm.lower()}_bipartite",
        node_type="bipartite",
        edge_kind="bipartite",
        directed=False,
        filters={
            "left": left_norm,
            "right": right_norm.lower(),
            "start_time": start_time,
            "end_time": end_time,
        },
    )

    builder = BipartiteNetworkBuilder(db, spec)
    return iter_edge_chunks(builder, chunksize=chunksize)


def coaction(
    db,
    action: str,
    *,
    start_time=None,
    end_time=None,
    weighting="count",
    directed=False,
) -> NetworkResult:
    filters: Dict[str, Any] = {}
    if start_time is not None:
        filters["start_time"] = start_time
    if end_time is not None:
        filters["end_time"] = end_time

    spec = NetworkSpec(
        name=f"co_{action.lower()}_network",
        node_type="account",
        edge_kind=action.upper(),
        directed=directed,
        weighting=weighting,
        filters=filters,
    )
    builder = CoActionNetworkBuilder(db, spec)
    return builder.build()


def iter_coaction_edges(
    db, action: str, *, start_time=None, end_time=None, chunksize=100_000
):
    filters = {}
    if start_time is not None:
        filters["start_time"] = start_time
    if end_time is not None:
        filters["end_time"] = end_time

    spec = NetworkSpec(
        name=f"co_{action.lower()}_network",
        node_type="account",
        edge_kind=action.upper(),
        directed=False,
        filters=filters,
    )
    builder = CoActionNetworkBuilder(db, spec)
    return iter_edge_chunks(builder, chunksize=chunksize)
