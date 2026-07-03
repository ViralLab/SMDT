import pytest

pytest.importorskip("presidio_analyzer")

from smdt.pseudonymizer.platform_patterns import platform_recognizers


def _match_texts(recognizer, text, entity_type):
    """Run a single PatternRecognizer directly (no NLP engine needed)."""
    results = recognizer.analyze(text, entities=[entity_type])
    return [text[r.start : r.end] for r in results]


def test_generic_platform_returns_mention_and_hashtag_recognizers() -> None:
    """Unknown/Twitter-style platforms get the generic @mention/#hashtag pair."""
    recs = platform_recognizers("twitter")
    names = {r.name for r in recs}
    assert names == {"generic_mention", "generic_hashtag"}


def test_unknown_platform_falls_back_to_generic() -> None:
    """Platforms without a dedicated entry (reddit, gab, ...) use the generic set."""
    recs = platform_recognizers("some_platform_never_registered")
    assert {r.name for r in recs} == {"generic_mention", "generic_hashtag"}


def test_none_platform_falls_back_to_generic() -> None:
    """No platform info at all should still yield a usable default."""
    recs = platform_recognizers(None)
    assert {r.name for r in recs} == {"generic_mention", "generic_hashtag"}


def test_generic_mention_matches_whole_at_handle() -> None:
    """Generic MENTION pattern matches '@handle' including the leading '@'."""
    recs = {r.name: r for r in platform_recognizers("twitter")}
    matches = _match_texts(recs["generic_mention"], "hey @JohnDoe how are you", "MENTION")
    assert matches == ["@JohnDoe"]


def test_generic_mention_does_not_match_email_local_part() -> None:
    """'@' preceded by a word char (e.g. inside an email) should not match as a mention."""
    recs = {r.name: r for r in platform_recognizers("twitter")}
    matches = _match_texts(recs["generic_mention"], "reach me at john@example.com", "MENTION")
    assert matches == []


def test_generic_hashtag_matches_single_leading_hash() -> None:
    """Generic HASHTAG pattern matches Twitter-style single leading '#'."""
    recs = {r.name: r for r in platform_recognizers("twitter")}
    matches = _match_texts(recs["generic_hashtag"], "see #ElectionNight now", "HASHTAG")
    assert matches == ["#ElectionNight"]


def test_weibo_platform_returns_weibo_recognizers() -> None:
    """Weibo gets its own recognizer set, not the generic one."""
    recs = platform_recognizers("weibo")
    assert {r.name for r in recs} == {"weibo_mention", "weibo_hashtag"}


def test_weibo_hashtag_requires_double_hash_wrapping() -> None:
    """Weibo hashtags are wrapped on both sides: '#topic#', unlike Twitter's '#tag'."""
    recs = {r.name: r for r in platform_recognizers("weibo")}
    matches = _match_texts(recs["weibo_hashtag"], "cool #热门话题# right", "HASHTAG")
    assert matches == ["#热门话题#"]


def test_weibo_hashtag_does_not_match_single_leading_hash() -> None:
    """A lone leading '#' (Twitter-style) should not match the Weibo double-wrap pattern."""
    recs = {r.name: r for r in platform_recognizers("weibo")}
    matches = _match_texts(recs["weibo_hashtag"], "see #ElectionNight now", "HASHTAG")
    assert matches == []


def test_weibo_mention_matches_unicode_handles() -> None:
    """Weibo mentions may contain CJK characters, unlike the ASCII-only generic pattern."""
    recs = {r.name: r for r in platform_recognizers("weibo")}
    matches = _match_texts(recs["weibo_mention"], "cc @张三 请看", "MENTION")
    assert matches == ["@张三"]
