import pytest

pytest.importorskip("presidio_analyzer")
pytest.importorskip("presidio_anonymizer")

from smdt.pseudonymizer.pii_engine import PiiEngine
from smdt.pseudonymizer.pii_policy import PiiAction, PiiPolicy, PiiRule
from smdt.pseudonymizer.pseudonyms import Algorithm, Hasher

NLP_CONFIG = {
    "nlp_engine_name": "spacy",
    "models": [{"lang_code": "en", "model_name": "en_core_web_sm"}],
}


@pytest.fixture(scope="module")
def hasher():
    return Hasher(algo=Algorithm.SHA256, pepper=b"pepper", normalizer=lambda s: s.strip().lower())


@pytest.fixture(scope="module")
def engine(hasher):
    try:
        import spacy

        spacy.load("en_core_web_sm")
    except Exception:
        pytest.skip("en_core_web_sm not installed; skipping PiiEngine tests")
    return PiiEngine(hasher=hasher, nlp_configuration=NLP_CONFIG)


def _policy(table, column, rules):
    return PiiPolicy(rules={table: {column: rules}})


def test_redact_returns_none_and_empty_unchanged(engine) -> None:
    """None/empty input should pass through without invoking the analyzer."""
    policy = _policy("posts", "body", {"PHONE_NUMBER": PiiRule(PiiAction.REPLACE)})
    assert engine.redact(None, "posts", "body", policy) is None
    assert engine.redact("", "posts", "body", policy) == ""


def test_redact_unconfigured_column_returns_text_unchanged(engine) -> None:
    """A table/column with no PiiPolicy rules should not be scanned at all."""
    policy = PiiPolicy(rules={})
    text = "call me at 212-555-0182"
    assert engine.redact(text, "posts", "body", policy) == text


def test_hash_action_is_deterministic(engine) -> None:
    """The same input text should hash to the same output across calls."""
    policy = _policy("posts", "body", {"PHONE_NUMBER": PiiRule(PiiAction.HASH)})
    text = "call 212-555-0182 now"
    out1 = engine.redact(text, "posts", "body", policy)
    out2 = engine.redact(text, "posts", "body", policy)
    assert out1 == out2
    assert "212-555-0182" not in out1


def test_mention_hash_matches_direct_hasher_call(engine, hasher) -> None:
    """MENTION hashing must strip the leading '@' so it matches column-level hashing.

    Regression test for the bug where "@JohnDoe" (including '@') hashed
    differently from accounts.username="JohnDoe" hashed via Hasher.make directly.
    """
    policy = _policy("posts", "body", {"MENTION": PiiRule(PiiAction.HASH)})
    out = engine.redact("hey @JohnDoe!", "posts", "body", policy, platform="twitter")
    expected = "@u_" + hasher.make("JohnDoe")
    assert expected in out


def test_replace_with_fixed_placeholder(engine) -> None:
    """REPLACE with no explicit replacement defaults to "[<ENTITY_TYPE>]"."""
    policy = _policy("posts", "body", {"PHONE_NUMBER": PiiRule(PiiAction.REPLACE)})
    out = engine.redact("call 212-555-0182 now", "posts", "body", policy)
    assert out == "call [PHONE_NUMBER] now"


def test_replace_with_callable_transform(engine) -> None:
    """REPLACE with a callable applies the transform to the matched text."""
    policy = _policy(
        "posts", "body", {"PHONE_NUMBER": PiiRule(PiiAction.REPLACE, replacement=lambda t: "REDACTED")}
    )
    out = engine.redact("call 212-555-0182 now", "posts", "body", policy)
    assert out == "call REDACTED now"


def test_drop_action_removes_span_entirely(engine) -> None:
    """DROP should remove the matched span rather than leaving a placeholder."""
    policy = _policy("posts", "body", {"PHONE_NUMBER": PiiRule(PiiAction.DROP)})
    out = engine.redact("call 212-555-0182 now", "posts", "body", policy)
    assert "212-555-0182" not in out
    assert "[PHONE_NUMBER]" not in out


def test_platform_selects_hashtag_pattern(engine) -> None:
    """Weibo's double-hash hashtags should be caught only when platform='weibo'."""
    policy = _policy(
        "posts", "body", {"HASHTAG": PiiRule(PiiAction.REPLACE, replacement=lambda t: t.lower())}
    )
    weibo_out = engine.redact("cool #热门话题# post", "posts", "body", policy, platform="weibo")
    assert weibo_out == "cool #热门话题# post".lower()

    # Same text under the generic (twitter-style) pattern set: the weibo-style
    # double-wrap isn't recognized by the single-leading-'#' pattern, so it's
    # left untouched.
    twitter_out = engine.redact(
        "cool #热门话题# post", "posts", "body", policy, platform="twitter"
    )
    assert twitter_out == "cool #热门话题# post"


def test_only_configured_entity_types_are_touched(engine) -> None:
    """Enabling PHONE_NUMBER shouldn't also redact an email address in the same text."""
    policy = _policy("posts", "body", {"PHONE_NUMBER": PiiRule(PiiAction.REPLACE)})
    out = engine.redact("call 212-555-0182 or email john@example.com", "posts", "body", policy)
    assert "[PHONE_NUMBER]" in out
    assert "john@example.com" in out
