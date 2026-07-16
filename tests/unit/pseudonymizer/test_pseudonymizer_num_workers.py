"""Regression test: PseudonymizeConfig(num_workers=1) (the default) must
keep using Pseudonymizer's original single-process transform path --
num_workers=1 is unchanged from before the parallel-transform feature was
added.

Real ProcessPoolExecutor-based (num_workers > 1) behavior needs a real
Postgres database (workers construct their own Hasher/Redactor, but
_copy_table_parallel still reads from/writes to real StandardDB instances),
so that path is covered separately in the integration tests.
"""

from smdt.pseudonymizer.pseudonymizer import (
    Pseudonymizer,
    PseudonymizeConfig,
    _transform_row_impl,
)
from smdt.pseudonymizer.policy import DEFAULT_POLICY


def test_pseudonymize_config_defaults_to_single_worker():
    cfg = PseudonymizeConfig(src_db_name="x", dst_db_name="y", pepper=b"p")
    assert cfg.num_workers == 1


def test_transform_row_impl_matches_instance_method():
    """The module-level pure function (used by worker processes) must
    produce identical output to Pseudonymizer._transform_row (the serial
    path) for the same inputs -- they're supposed to be the same logic,
    just parameterized differently."""
    cfg = PseudonymizeConfig(src_db_name="x", dst_db_name="y", pepper=b"fixed-pepper")
    p = Pseudonymizer(cfg, policy=DEFAULT_POLICY)

    row = {
        "post_id": "1",
        "account_id": "acct1",
        "body": "hello @someone check http://example.com",
        "platform": "test",
    }

    via_instance = p._transform_row("posts", dict(row))
    via_pure_fn = _transform_row_impl(
        "posts", dict(row), p.policy, p.hasher, p.redactor, p.pii_engine, p.cfg.pii_policy
    )
    assert via_instance == via_pure_fn
