"""
Utilities for streaming network data from the database.
"""

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
        with conn.cursor() as cur:
            cur.execute(sql, params)

            # Column names from cursor description
            desc = cur.description
            if not desc:
                return  # no results at all

            colnames = [c[0] for c in desc]

            while True:
                rows = cur.fetchmany(chunksize)
                if not rows:
                    break

                chunk = pd.DataFrame(rows, columns=colnames)
                if chunk.empty:
                    continue

                yield chunk
