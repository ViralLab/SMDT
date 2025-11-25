from __future__ import annotations
import tarfile
from fnmatch import fnmatch
from typing import Any, Callable, Iterable, Mapping, Optional

from .base import Reader
from .registry import read_from_filelike


class TarReader(Reader):
    """Reader for tar archives (uncompressed or compressed)."""
    name = "tar"

    def __init__(
        self, *, member_filter: Optional[Callable[[str], bool]] = None
    ) -> None:
        """Initialize the TarReader.

        Args:
            member_filter: Optional callable to filter members by name.
        """
        self.member_filter = member_filter

    def supports(self, uri: str, *, content_type: Optional[str] = None) -> bool:
        """Check if the reader supports the given URI.

        Supports .tar, .tar.gz, .tgz, .tar.bz2, .tar.xz.

        Args:
            uri: URI to check.
            content_type: Optional content type hint.

        Returns:
            True if supported, False otherwise.
        """
        u = uri.lower()
        return u.endswith((".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tar.xz"))

    def stream(self, uri: str, **kwargs: Any) -> Iterable[Mapping[str, Any]]:
        """Stream records from a tar archive.

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

        # Don’t leak archive-only kwargs to child readers
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

        with tarfile.open(uri, "r:*") as tf:
            for member in tf:
                if not member.isfile():
                    continue
                mname = member.name
                if not want(mname):
                    continue

                fobj = tf.extractfile(member)
                if fobj is None:
                    continue

                # Single handoff point: reader selection + filelike vs tempfile fallback.
                # Ensures nested compression (e.g., *.csv.gz inside tar) is handled via member_name.
                with fobj:
                    yield from read_from_filelike(
                        fobj, member_name=mname, **child_kwargs
                    )


from . import registry

registry.register(TarReader())
