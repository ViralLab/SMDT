import pytest

from smdt.pseudonymizer.pseudonyms import Algorithm, Hasher


def _hasher(normalizer=None):
    return Hasher(algo=Algorithm.SHA256, pepper=b"pepper", normalizer=normalizer)


def test_md5_is_not_a_supported_algorithm() -> None:
    """MD5 was removed as an option; only SHA256/SHA512/WHIRLPOOL remain."""
    assert not hasattr(Algorithm, "MD5")
    assert {a.value for a in Algorithm} == {"sha256", "sha512", "whirlpool"}


def test_make_is_deterministic() -> None:
    """The same value + pepper should always hash to the same output."""
    h = _hasher()
    assert h.make("JohnDoe") == h.make("JohnDoe")


def test_make_returns_full_native_digest_length_sha256() -> None:
    """No truncation: SHA256 output is always the full 64 hex chars (256 bits)."""
    h = Hasher(algo=Algorithm.SHA256, pepper=b"pepper")
    assert len(h.make("x")) == 64


def test_make_returns_full_native_digest_length_sha512() -> None:
    """No truncation: SHA512 output is always the full 128 hex chars (512 bits)."""
    h = Hasher(algo=Algorithm.SHA512, pepper=b"pepper")
    assert len(h.make("x")) == 128


def test_hasher_has_no_output_hex_len_field() -> None:
    """output_hex_len was removed entirely -- no configurable truncation knob."""
    h = _hasher()
    assert not hasattr(h, "output_hex_len")


def test_different_pepper_produces_different_hash() -> None:
    """The pepper must actually participate in the hash (keyed, not a bare hash)."""
    a = Hasher(algo=Algorithm.SHA256, pepper=b"pepper-a").make("JohnDoe")
    b = Hasher(algo=Algorithm.SHA256, pepper=b"pepper-b").make("JohnDoe")
    assert a != b


def test_none_and_empty_string_semantics_preserved() -> None:
    """None stays None; empty string stays empty string (not hashed)."""
    h = _hasher()
    assert h.make(None) is None
    assert h.make("") == ""


def test_lowercase_normalizer_makes_case_variants_consistent() -> None:
    """With the recommended lowercase normalizer, case shouldn't affect the hash.

    This is what keeps accounts.username="JohnDoe" consistent with a "@JohnDoe"
    mention found in free text (both go through the same normalizer).
    """
    h = _hasher(normalizer=lambda s: s.strip().lower())
    assert h.make("JohnDoe") == h.make("johndoe") == h.make("  JohnDoe  ")


def test_without_normalizer_case_variants_differ() -> None:
    """Sanity check: without an explicit lowercase normalizer, case matters.

    Confirms the consistency in the previous test comes from the normalizer,
    not from some other implicit case-folding in Hasher itself.
    """
    h = _hasher(normalizer=None)
    assert h.make("JohnDoe") != h.make("johndoe")
