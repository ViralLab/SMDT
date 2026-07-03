"""Built-in per-platform Presidio recognizers for MENTION/HASHTAG detection.

Presidio's generic recognizers don't know about platform-specific mention/
hashtag syntax — e.g. Weibo wraps hashtags in a leading *and* trailing '#'
("#topic#"), unlike Twitter's single leading '#'. These are registered as
Presidio `ad_hoc_recognizers` per call, selected by the row's `platform`
column, so the same entity types (MENTION, HASHTAG) resolve through one
PiiPolicy regardless of which platform's syntax actually matched.
"""

from __future__ import annotations
from functools import lru_cache
from typing import List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from presidio_analyzer import PatternRecognizer


def _require_presidio():
    try:
        from presidio_analyzer import Pattern, PatternRecognizer
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            "presidio-analyzer is required for platform pattern recognizers. "
            "Install with: pip install 'smdt[pii]'"
        ) from e
    return Pattern, PatternRecognizer


@lru_cache(maxsize=None)
def _generic_recognizers() -> tuple:
    """@mention / #hashtag — the shape shared by most platforms (Twitter-like)."""
    Pattern, PatternRecognizer = _require_presidio()
    return (
        PatternRecognizer(
            supported_entity="MENTION",
            name="generic_mention",
            patterns=[
                Pattern(name="mention", regex=r"(?<!\w)@(\w{1,64})", score=0.9)
            ],
        ),
        PatternRecognizer(
            supported_entity="HASHTAG",
            name="generic_hashtag",
            patterns=[
                Pattern(name="hashtag", regex=r"(?<!\w)#(\w{1,139})", score=0.9)
            ],
        ),
    )


@lru_cache(maxsize=None)
def _weibo_recognizers() -> tuple:
    """Weibo wraps hashtags in a leading *and* trailing '#': "#topic#"."""
    Pattern, PatternRecognizer = _require_presidio()
    return (
        PatternRecognizer(
            supported_entity="MENTION",
            name="weibo_mention",
            patterns=[
                Pattern(
                    name="mention",
                    regex=r"(?<!\w)@([\w一-鿿]{1,64})",
                    score=0.9,
                )
            ],
        ),
        PatternRecognizer(
            supported_entity="HASHTAG",
            name="weibo_hashtag",
            patterns=[Pattern(name="hashtag", regex=r"#([^#\n]{1,139})#", score=0.9)],
        ),
    )


# Platforms without a dedicated entry fall back to the generic Twitter-like
# pattern set (covers gab/koo/parler/reddit/scored/telegram/truthsocial/
# voatco/bluesky, which all use plain '@handle'/'#tag' syntax).
_PLATFORM_BUILDERS = {
    "weibo": _weibo_recognizers,
}


def platform_recognizers(platform: Optional[str]) -> List["PatternRecognizer"]:
    """Return the built-in ad-hoc recognizers for a given platform.

    Args:
        platform: Canonical platform (e.g. "twitter", "weibo"), or None.

    Returns:
        List of PatternRecognizer instances for MENTION/HASHTAG detection.
    """
    builder = _PLATFORM_BUILDERS.get(platform, _generic_recognizers)
    return list(builder())
