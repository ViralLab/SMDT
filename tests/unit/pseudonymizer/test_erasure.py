import pytest

from smdt.pseudonymizer.erasure import Eraser, ErasureMode, ErasureTarget


def test_pseudonymized_target_requires_pepper() -> None:
    """A pseudonymized target needs a pepper to recompute the matching hash."""
    with pytest.raises(ValueError, match="pepper"):
        Eraser(
            targets=[
                ErasureTarget(db_name="x", mode=ErasureMode.SCRUB, is_pseudonymized=True)
            ]
        )


def test_plaintext_only_target_does_not_require_pepper() -> None:
    """No pepper is needed if every target is plaintext (literal matching)."""
    eraser = Eraser(
        targets=[
            ErasureTarget(db_name="x", mode=ErasureMode.SCRUB, is_pseudonymized=False)
        ]
    )
    assert eraser.hasher is None


def test_invalid_identity_column_rejected_before_touching_the_db() -> None:
    """An unsupported identity_column raises before any DB connection is attempted.

    Uses an unreachable db_name to prove the ValueError fires from validation,
    not from a connection failure.
    """
    eraser = Eraser(
        targets=[
            ErasureTarget(
                db_name="unreachable_db_should_never_be_dialed",
                mode=ErasureMode.SCRUB,
                is_pseudonymized=False,
            )
        ]
    )
    with pytest.raises(ValueError, match="identity_column"):
        eraser.erase("someone", identity_column="bio")
