from typing import Iterator

import pandas as pd

from .base import NetworkBuilder


def iter_edge_chunks(
    builder: NetworkBuilder,
    chunksize: int = 100_000,
) -> Iterator[pd.DataFrame]:
    """
    Stream edge rows from the database in chunks.

    Parameters
    ----------
    builder
        A concrete NetworkBuilder instance (UserInteractionNetworkBuilder,
        EntityCooccurrenceNetworkBuilder, BipartiteNetworkBuilder, ...).
    chunksize
        Number of rows per chunk to load from the DB.

    Yields
    ------
    pandas.DataFrame
        DataFrames with at least columns: src, dst, weight.
    """
    sql, params = builder._edge_query()

    with builder.db.connect() as conn:
        for chunk in pd.read_sql_query(
            sql,
            conn,
            params=params,
            chunksize=chunksize,
        ):
            if chunk.empty:
                continue
            yield chunk
