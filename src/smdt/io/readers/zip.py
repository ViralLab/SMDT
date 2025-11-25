# smdt/io/readers/zip.py
from __future__ import annotations

import zipfile
from fnmatch import fnmatch
from typing import Any, Callable, Iterable, Mapping, Optional

from .base import Reader
from .registry import read_from_filelike


class ZipReader(Reader):
    """Reader for zip archives."""
    name = "zip"

    def __init__(
        self, *, member_filter: Optional[Callable[[str], bool]] = None
    ) -> None:
        """Initialize the ZipReader.

        Args:
            member_filter: Optional callable to filter members by name.
        """
        self.member_filter = member_filter

    def supports(self, uri: str, *, content_type: Optional[str] = None) -> bool:
        """Check if the reader supports the given URI.

        Supports .zip.

        Args:
            uri: URI to check.
            content_type: Optional content type hint.

        Returns:
            True if supported, False otherwise.
        """
        u = uri.lower()
        return u.endswith((".zip",))

    def stream(self, uri: str, **kwargs: Any) -> Iterable[Mapping[str, Any]]:
        """Stream records from a zip archive.

        Iterates over members, filtering them, and delegating to appropriate readers.

        Args:
            uri: URI to read from.
            **kwargs: Additional arguments.
                include (tuple): Patterns to include.
                exclude (tuple): Patterns to exclude.
                member_filter (callable): Custom filter function.

        Yields:
            Dictionary representing a record.
        """
        include = tuple(kwargs.get("include", ())) or None
        exclude = tuple(kwargs.get("exclude", ())) or None
        member_filter = kwargs.get("member_filter", self.member_filter)

        # Don't leak archive-only kwargs to child readers
        child_kwargs = {
            k: v
            for k, v in kwargs.items()
            if k not in ("member_filter", "include", "exclude")
        }

        def want(name: str) -> bool:
            if member_filter and not member_filter(name):
                return False
            if include and not any(fnmatch(name, pat) for pat in include):
                return False
            if exclude and any(fnmatch(name, pat) for pat in exclude):
                return False
            return True

        with zipfile.ZipFile(uri) as zf:
            for info in zf.infolist():
                if info.is_dir():
                    continue
                mname = info.filename
                if not want(mname):
                    continue

                # Single handoff point (reader selection + filelike vs tempfile fallback).
                # Ensures nested compression (e.g., *.csv.gz inside .zip) is handled via member_name.
                with zf.open(info, "r") as fobj:
                    yield from read_from_filelike(
                        fobj, member_name=mname, **child_kwargs
                    )


from . import registry

registry.register(ZipReader())
