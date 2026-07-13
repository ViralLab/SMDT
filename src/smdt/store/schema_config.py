"""Configurable hypertable tuning (chunk interval, space partitioning,
compression) for the standard/pseudonymized schemas.

The schema SQL files (schemas/standard_schema.sql,
schemas/pseudo_std_schema.sql) define table structure and indexes, which
are the same regardless of dataset. Chunk sizing, space partitioning, and
compression are a different kind of decision -- they were tuned for the
Election2023 Twitter dataset used in this project's own benchmarks (see
hpc.md / query_benchmarking.md for the measurements behind these defaults),
and a different dataset (different volume, different natural partitioning
dimension) may want different values. This module makes those specific
knobs configurable via SchemaConfig, applied programmatically after the
static schema SQL via apply_hypertable_config(), instead of being hardcoded
in the SQL files.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Dict, Optional


@dataclass(frozen=True)
class ChunkConfig:
    """Hypertable tuning for one table.

    Attributes:
        time_column: The hypertable's time partitioning column.
        chunk_time_interval: How much time each chunk covers. Smaller
            chunks mean more precise chunk exclusion for narrow time-range
            queries, but more chunks to fan out across for any query that
            doesn't filter by time (e.g. a point lookup by id) -- see
            query_benchmarking.md for measured evidence of that tradeoff.
        space_partition_column: Optional additional partitioning dimension
            (e.g. "entity_type"). None means no space partitioning.
        space_partitions: Number of space partitions, if space_partition_column
            is set.
        compress_segmentby: Column list to segment compressed chunks by
            (should match a common equality-filter column). None means
            compression is never enabled on this table.
        compress_after: Age at which chunks are automatically compressed.
            None means compression capability is enabled (if
            compress_segmentby is set) but no policy is scheduled -- chunks
            stay uncompressed until compressed explicitly or a policy is
            added later.
        reorder_index: Index name to physically cluster chunks by (via
            TimescaleDB's reorder policy), for scan locality on the
            table's primary access pattern. None means no reorder policy.
    """

    time_column: str = "created_at"
    chunk_time_interval: timedelta = timedelta(days=7)
    space_partition_column: Optional[str] = None
    space_partitions: Optional[int] = None
    compress_segmentby: Optional[str] = None
    compress_after: Optional[timedelta] = None
    reorder_index: Optional[str] = None


def _default_chunk_configs() -> Dict[str, ChunkConfig]:
    """Reproduces exactly what was previously hardcoded in the schema SQL
    files -- tuned for the Election2023 Twitter dataset. compress_after is
    None everywhere (matching the schema files' previous commented-out
    policies) since that's the actual behavior this project has been
    benchmarking against; set it explicitly to schedule compression."""
    return {
        "communities": ChunkConfig(
            chunk_time_interval=timedelta(days=30),
            compress_segmentby="community_id",
            reorder_index="communities_comm_created_uk",
        ),
        "accounts": ChunkConfig(
            chunk_time_interval=timedelta(days=30),
            compress_segmentby="account_id",
            reorder_index="accounts_acct_created_uk",
        ),
        "posts": ChunkConfig(
            chunk_time_interval=timedelta(days=7),
            compress_segmentby="account_id",
            reorder_index="posts_acct_time_idx",
        ),
        "entities": ChunkConfig(
            chunk_time_interval=timedelta(days=7),
            space_partition_column="entity_type",
            space_partitions=6,
            compress_segmentby="entity_type, account_id",
            reorder_index="entities_acct_type_time_idx",
        ),
        "actions": ChunkConfig(
            chunk_time_interval=timedelta(days=7),
            space_partition_column="action_type",
            space_partitions=8,
            compress_segmentby="action_type",
            reorder_index="actions_type_time_idx",
        ),
    }


@dataclass(frozen=True)
class SchemaConfig:
    """Hypertable tuning for all of the standard/pseudonymized schema's
    hypertables. Defaults reproduce this project's own Election2023 Twitter
    benchmark tuning -- override per table for a dataset with a different
    volume/shape, e.g.:

        cfg = SchemaConfig(tables={
            **SchemaConfig().tables,
            "posts": replace(SchemaConfig().tables["posts"],
                              chunk_time_interval=timedelta(days=1)),
        })
    """

    tables: Dict[str, ChunkConfig] = field(default_factory=_default_chunk_configs)
