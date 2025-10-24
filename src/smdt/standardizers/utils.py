from __future__ import annotations
import re
from typing import List, Tuple, Dict, Any
from urllib.parse import urlparse

# ---------------------------
# Helpers for mentions, hashtags, emails
# ---------------------------

_EMAIL_RE = re.compile(
    r"(?<![A-Za-z0-9._%+\-])([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})(?![A-Za-z0-9._%+\-])",
    re.IGNORECASE,
)

_MENTION_RE = re.compile(r"(?<!\w)@(?P<user>[A-Za-z0-9_]{1,15})(?![A-Za-z0-9_])")
_HASHTAG_RE = re.compile(r"(?<!\w)#(?P<tag>[A-Za-z0-9_]{1,100})(?![A-Za-z0-9_])")


def _to_text(x) -> str:
    """Coerce arbitrary values (incl. pandas NaN) to a safe string for regex."""
    if isinstance(x, str):
        return x
    if x is None:
        return ""
    try:
        # NaN is not equal to itself
        if x != x:  # catches float('nan'), pandas NA, etc.
            return ""
    except Exception:
        pass
    return str(x)


def _uniq(seq: List[str]) -> List[str]:
    seen, out = set(), []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def extract_emails(text: str, lowercase: bool = True) -> List[str]:
    text = _to_text(text)
    emails = [m.group(1) for m in _EMAIL_RE.finditer(text or "")]
    if lowercase:
        emails = [e.lower() for e in emails]
    return _uniq(emails)


def extract_mentions(text: str, lowercase: bool = True) -> List[str]:
    text = _to_text(text)
    mentions = [m.group("user") for m in _MENTION_RE.finditer(text or "")]
    if lowercase:
        mentions = [m.lower() for m in mentions]
    return _uniq(mentions)


def extract_hashtags(text: str, lowercase: bool = True) -> List[str]:
    text = _to_text(text)
    tags = [m.group("tag") for m in _HASHTAG_RE.finditer(text or "")]
    if lowercase:
        tags = [t.lower() for t in tags]
    return _uniq(tags)


# ---------------------------
# URLs with urlextract
# ---------------------------


def extract_urls(
    text: str, ensure_scheme: bool = True, unique: bool = True
) -> List[str]:
    try:
        from urlextract import URLExtract
    except ImportError as e:
        raise ImportError(
            "URL extraction requires the 'urlextract' library. "
            "Install with: pip install urlextract"
        ) from e

    extractor = URLExtract()
    text = _to_text(text)
    urls = extractor.find_urls(text or "")

    # normalize
    normed = []
    for u in urls:
        if ensure_scheme and not u.lower().startswith(("http://", "https://")):
            u = "http://" + u
        try:
            p = urlparse(u)
            if not p.netloc:  # skip junk
                continue
        except Exception:
            continue
        normed.append(u)

    return _uniq(normed) if unique else normed


# ---------------------------
# All-in-one
# ---------------------------


def extract_all(text: str) -> Dict[str, List[str]]:
    return {
        "emails": extract_emails(text),
        "urls": extract_urls(text),
        "mentions": extract_mentions(text),
        "hashtags": extract_hashtags(text),
    }
