from datetime import datetime, timedelta
from typing import Literal, Optional, Iterator, Dict, Any, Tuple, List
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


# -------------------------------------------------------------------
# Internal helper: iterate fixed-size time windows
# -------------------------------------------------------------------


def _iter_time_windows(
    start_time: datetime,
    end_time: datetime,
    step: timedelta,
):
    """
    Yield half-open time windows [t_i, t_{i+1}) from start_time to end_time.

    Example:
        for ws, we in _iter_time_windows(t0, t1, timedelta(hours=1)):
            ...
    """
    current = start_time
    while current < end_time:
        nxt = min(current + step, end_time)
        yield current, nxt
        current = nxt


# -------------------------------------------------------------------
# Temporal variants: networks over time windows
# -------------------------------------------------------------------


def user_interaction_over_time(
    db,
    interaction: str,
    *,
    start_time: datetime,
    end_time: datetime,
    step: timedelta,
    weighting: str = "count",
    min_weight: int = 1,
) -> List[Dict[str, Any]]:
    """
    Build a sequence of user–user interaction networks over fixed time windows.

    Parameters
    ----------
    db
        StandardDB instance.
    interaction
        Action type (e.g. "QUOTE", "COMMENT", "SHARE", "FOLLOW").
    start_time, end_time
        Overall time range for slicing.
    step
        Window size as a datetime.timedelta (e.g. timedelta(hours=1)).
    weighting
        Passed through to user_interaction().
    min_weight
        Passed through to user_interaction().

    Returns
    -------
    List[dict]
        Each element has keys:
          - "window_start": datetime
          - "window_end": datetime
          - "network": NetworkResult
    """
    results: List[Dict[str, Any]] = []

    for ws, we in _iter_time_windows(start_time, end_time, step):
        net = user_interaction(
            db,
            interaction=interaction,
            start_time=ws,
            end_time=we,
            weighting=weighting,
            min_weight=min_weight,
        )
        results.append(
            {
                "window_start": ws,
                "window_end": we,
                "network": net,
            }
        )

    return results


def entity_cooccurrence_over_time(
    db,
    entity_type: str,
    *,
    start_time: datetime,
    end_time: datetime,
    step: timedelta,
    weighting: str = "count",
    min_weight: int = 1,
) -> List[Dict[str, Any]]:
    """
    Build a sequence of entity–entity co-occurrence networks over fixed time windows.

    Parameters
    ----------
    db
        StandardDB instance.
    entity_type
        Entity type, e.g. "HASHTAG", "USER_TAG", "LINK".
    start_time, end_time
        Overall time range for slicing.
    step
        Window size as a datetime.timedelta (e.g. timedelta(hours=1)).
    weighting
        Passed through to entity_cooccurrence().
    min_weight
        Passed through to entity_cooccurrence().

    Returns
    -------
    List[dict]
        Each element has keys:
          - "window_start": datetime
          - "window_end": datetime
          - "network": NetworkResult
    """
    results: List[Dict[str, Any]] = []

    for ws, we in _iter_time_windows(start_time, end_time, step):
        net = entity_cooccurrence(
            db,
            entity_type=entity_type,
            start_time=ws,
            end_time=we,
            weighting=weighting,
            min_weight=min_weight,
        )
        results.append(
            {
                "window_start": ws,
                "window_end": we,
                "network": net,
            }
        )

    return results


def bipartite_over_time(
    db,
    left: str,
    right: str,
    *,
    start_time: datetime,
    end_time: datetime,
    step: timedelta,
    weighting: str = "count",
) -> List[Dict[str, Any]]:
    """
    Build a sequence of bipartite networks over fixed time windows.

    Parameters
    ----------
    db
        StandardDB instance.
    left
        Left node type (e.g. "account", "post").
    right
        Right node type (e.g.  'HASHTAG', 'USER_TAG','LINK','EMAIL','IMAGE','VIDEO').
    start_time, end_time
        Overall time range for slicing.
    step
        Window size as a datetime.timedelta (e.g. timedelta(hours=1)).
    weighting
        Passed through to bipartite().

    Returns
    -------
    List[dict]
        Each element has keys:
          - "window_start": datetime
          - "window_end": datetime
          - "network": NetworkResult
    """
    results: List[Dict[str, Any]] = []

    for ws, we in _iter_time_windows(start_time, end_time, step):
        net = bipartite(
            db,
            left=left,
            right=right,
            start_time=ws,
            end_time=we,
            weighting=weighting,
        )
        results.append(
            {
                "window_start": ws,
                "window_end": we,
                "network": net,
            }
        )

    return results


def coaction_over_time(
    db,
    action: str,
    *,
    start_time: datetime,
    end_time: datetime,
    step: timedelta,
    weighting: str = "count",
    min_weight: int = 1,
) -> List[Dict[str, Any]]:
    """
    Build a sequence of user–user co-action networks over fixed time windows.

    Co-action networks connect users who both performed the same action
    (e.g., COMMENT, SHARE, QUOTE) on the same target_post_id.

    Parameters
    ----------
    db
        StandardDB instance.
    action
        Action type, e.g. "COMMENT", "SHARE", "QUOTE".
    start_time, end_time
        Overall time range for slicing.
    step
        Window size as a datetime.timedelta (e.g. timedelta(hours=1)).
    weighting
        Passed through to coaction().
    min_weight
        Minimum edge weight to keep per window (applied after building).

    Returns
    -------
    List[dict]
        Each element has keys:
          - "window_start": datetime
          - "window_end": datetime
          - "network": NetworkResult
    """
    results: List[Dict[str, Any]] = []

    for ws, we in _iter_time_windows(start_time, end_time, step):
        net = coaction(
            db,
            action=action,
            start_time=ws,
            end_time=we,
            weighting=weighting,
        )

        # Optional edge filtering by weight
        if min_weight is not None and not net.edges.empty:
            net.edges = net.edges[net.edges["weight"] >= min_weight]

        results.append(
            {
                "window_start": ws,
                "window_end": we,
                "network": net,
            }
        )

    return results
