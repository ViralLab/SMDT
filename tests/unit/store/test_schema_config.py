"""Unit tests for SchemaConfig/ChunkConfig defaults -- no DB needed.

Real DB behavior (does init_schema() actually apply these correctly, is it
idempotent, does a custom config actually change chunk/partition/
compression state) is covered in tests/integration/test_schema_config.py.
"""

from dataclasses import replace
from datetime import timedelta

from smdt.store.schema_config import ChunkConfig, SchemaConfig


def test_default_tables_match_the_previously_hardcoded_schema():
    """Defaults must reproduce exactly what used to be hardcoded in the SQL
    schema files (30-day chunks for accounts/communities, 7-day for
    posts/entities/actions; entity_type x6 and action_type x8 space
    partitions; no scheduled compression policy)."""
    cfg = SchemaConfig()
    assert set(cfg.tables) == {"communities", "accounts", "posts", "entities", "actions"}

    assert cfg.tables["communities"].chunk_time_interval == timedelta(days=30)
    assert cfg.tables["accounts"].chunk_time_interval == timedelta(days=30)
    assert cfg.tables["posts"].chunk_time_interval == timedelta(days=7)
    assert cfg.tables["entities"].chunk_time_interval == timedelta(days=7)
    assert cfg.tables["actions"].chunk_time_interval == timedelta(days=7)

    assert cfg.tables["entities"].space_partition_column == "entity_type"
    assert cfg.tables["entities"].space_partitions == 6
    assert cfg.tables["actions"].space_partition_column == "action_type"
    assert cfg.tables["actions"].space_partitions == 8

    for table, cc in cfg.tables.items():
        assert cc.compress_after is None, f"{table} should have no scheduled compression policy by default"
        assert cc.compress_segmentby is not None, f"{table} should still have compression capability enabled"


def test_no_space_partitioning_by_default_for_non_entities_actions_tables():
    cfg = SchemaConfig()
    for table in ("communities", "accounts", "posts"):
        assert cfg.tables[table].space_partition_column is None
        assert cfg.tables[table].space_partitions is None


def test_chunk_config_is_overridable_per_table_without_affecting_others():
    """The documented override pattern -- replace() one table's config,
    leave the rest at defaults."""
    base = SchemaConfig()
    custom = SchemaConfig(tables={
        **base.tables,
        "posts": replace(base.tables["posts"], chunk_time_interval=timedelta(days=1)),
    })

    assert custom.tables["posts"].chunk_time_interval == timedelta(days=1)
    # Everything else stays at the default.
    assert custom.tables["entities"] == base.tables["entities"]
    assert custom.tables["accounts"] == base.tables["accounts"]
    # Original default instance is untouched (frozen dataclasses, no mutation).
    assert base.tables["posts"].chunk_time_interval == timedelta(days=7)


def test_chunk_config_frozen_and_hashable():
    """frozen=True is load-bearing: SchemaConfig gets passed into a
    ProcessPoolExecutor initializer's initargs (pseudonymizer worker
    processes), which pickles it -- and equality/immutability matter for
    the override pattern above."""
    a = ChunkConfig(chunk_time_interval=timedelta(days=7))
    b = ChunkConfig(chunk_time_interval=timedelta(days=7))
    assert a == b
    try:
        a.chunk_time_interval = timedelta(days=1)
        assert False, "ChunkConfig should be frozen"
    except AttributeError:
        pass
