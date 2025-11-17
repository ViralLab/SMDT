import types
import sys
import pytest

from smdt.standardizers import utils


@pytest.mark.parametrize(
    "input_val,expected",
    [
        ("abc", "abc"),
        (None, ""),
        (float("nan"), ""),
        (123, "123"),
    ],
)
def test_to_text_various(input_val, expected) -> None:
    """_to_text should coerce values (including None and NaN) to safe strings."""
    result = utils._to_text(input_val)
    assert result == expected


def test_uniq_preserves_order() -> None:
    """_uniq should remove duplicates while preserving first-seen order."""
    seq = ["a", "b", "a", "c", "b"]
    result = utils._uniq(seq)
    assert result == ["a", "b", "c"]


@pytest.mark.parametrize(
    "text,expected",
    [
        ("Contact me at Foo.Bar@example.com", ["foo.bar@example.com"]),
        ("no emails here", []),
        ("two@a.COM and TWO@a.com", ["two@a.com"]),
    ],
)
def test_extract_emails_param(text, expected) -> None:
    """extract_emails should find and normalize emails to lowercase and unique."""
    result = utils.extract_emails(text)
    assert result == expected


@pytest.mark.parametrize(
    "text,expected",
    [
        ("Hello @Alice and @bob", ["alice", "bob"]),
        ("@dup @dup", ["dup"]),
        ("no mentions", []),
    ],
)
def test_extract_mentions_param(text, expected) -> None:
    """extract_mentions should extract @usernames and normalize to lowercase and unique."""
    result = utils.extract_mentions(text)
    assert result == expected


@pytest.mark.parametrize(
    "text,expected",
    [
        ("#Tag #other", ["tag", "other"]),
        ("no tags", []),
        ("#dup #dup", ["dup"]),
    ],
)
def test_extract_hashtags_param(text, expected) -> None:
    """extract_hashtags should extract hashtags and normalize to lowercase and unique."""
    result = utils.extract_hashtags(text)
    assert result == expected


def test_extract_urls_with_monkeypatched_urlextract(monkeypatch) -> None:
    """extract_urls should call urlextract.URLExtract.find_urls; we monkeypatch a fake module."""

    # Create a fake urlextract module with a URLExtract class
    mod = types.ModuleType("urlextract")

    class URLExtract:
        def find_urls(self, text):
            # return some urls with and without scheme and some junk
            return ["example.com/path", "http://ok.com", "not-a-url:foo"]

    setattr(mod, "URLExtract", URLExtract)

    monkeypatch.setitem(sys.modules, "urlextract", mod)

    result = utils.extract_urls("see example.com/path and http://ok.com")
    # normalized should ensure scheme for example.com and keep http://ok.com
    assert "http://example.com/path" in result
    assert "http://ok.com" in result
