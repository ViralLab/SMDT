from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Pattern, Callable
from urllib.parse import urlparse
from urlextract import URLExtract

try:
    import regex as re  # type: ignore

    _UNICODE = True
except Exception:  # pragma: no cover
    import re  # type: ignore

    _UNICODE = False


@dataclass
class Redactor:
    """Redacts sensitive information from text.

    Attributes:
        handle_mapper: Function to map user handles.
        map_host: Optional function to map hostnames.
    """

    handle_mapper: Callable[[str], str]
    map_host: Optional[Callable[[str], str]] = None

    def __post_init__(self):
        L = "\\p{L}" if _UNICODE else "A-Za-z"
        N = "\\p{Nd}" if _UNICODE else "0-9"
        # Patterns
        self._re_mention: Pattern = re.compile(rf"(?<!\w)@([{L}{N}_.]{{1,64}})")
        self._re_email: Pattern = re.compile(
            r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
        )

        self._urlx = URLExtract()

    def _normalize_host(self, url_or_host: str) -> str | None:
        """Extract and normalize the hostname from a URL or host string.

        Args:
            url_or_host: URL or hostname string.

        Returns:
            Normalized hostname, or None if extraction fails.
        """
        if not url_or_host:
            return None
        s = url_or_host.strip()
        try:
            parsed = urlparse(
                s if s.startswith(("http://", "https://")) else "http://" + s
            )
            host = parsed.hostname or s
        except Exception:
            host = s
        if not host:
            return None
        host = host.lower()
        if host.startswith("www."):
            host = host[4:]
        try:
            host = host.encode("idna").decode("ascii")
        except Exception:
            pass
        return host

    def redact(self, text: Optional[str]) -> Optional[str]:
        """Redact sensitive information from text.

        Redacts mentions, emails, and URLs.

        Args:
            text: Input text.

        Returns:
            Redacted text, or None if input is None.
        """
        if text is None:
            return None
        if text == "":
            return ""
        s = text

        # Mentions
        def _m_sub(m):
            h = m.group(1)
            return "@u_" + self.handle_mapper(h.lower())

        s = self._re_mention.sub(_m_sub, s)

        # Emails
        s = self._re_email.sub("[EMAIL]", s)

        # URLs → domain tokens using URLExtract (handle multiple return shapes)
        try:
            urls_with_idx = self._urlx.find_urls(s, with_indices=True)
        except TypeError:
            # older urlextract uses get_indices
            urls_with_idx = self._urlx.find_urls(s, get_indices=True)

        if urls_with_idx:
            out, pos = [], 0
            for item in urls_with_idx:
                # Accept (url, (start, end)) or (url, start, end)
                if (
                    isinstance(item, tuple)
                    and len(item) == 2
                    and isinstance(item[1], tuple)
                ):
                    url, (start, end) = item
                elif isinstance(item, tuple) and len(item) == 3:
                    url, start, end = item
                else:
                    # Fallback: skip unknown shape
                    continue

                out.append(s[pos:start])
                host = self._normalize_host(url)
                if self.map_host and host:
                    out.append("URL_" + self.map_host(host))
                else:
                    out.append("[URL]")
                pos = end
            out.append(s[pos:])
            s = "".join(out)

        return s

    def sanitize_entity_body(
        self, entity_type: str, raw: Optional[str]
    ) -> Optional[str]:
        """Sanitize the body of an entity based on its type.

        Args:
            entity_type: Type of the entity (e.g., HASHTAG, USER_TAG, LINK, EMAIL).
            raw: Raw body text.

        Returns:
            Sanitized body text, or None if input is None.
        """
        if raw is None:
            return None
        val = raw.strip()
        et = (entity_type or "").upper()

        if et == "HASHTAG":
            v = val[1:] if val.startswith("#") else val
            return v.lower()

        if et == "USER_TAG":
            h = val[1:] if val.startswith("@") else val
            return "@u_" + self.handle_mapper(h.lower())

        if et == "LINK":
            try:
                urls = self._urlx.find_urls(val)
            except TypeError:
                urls = self._urlx.find_urls(val)  # same call; just for symmetry

            host = self._normalize_host(urls[0]) if urls else self._normalize_host(val)
            if host:
                return ("dom_" + self.map_host(host)) if self.map_host else host
            return "dom_"

        if et == "EMAIL":
            parts = val.split("@")
            return parts[-1].lower() if len(parts) >= 2 else "[EMAIL]"

        if et in ("IMAGE", "VIDEO"):
            return "media"  # or return val if you chose to keep raw

        return val
