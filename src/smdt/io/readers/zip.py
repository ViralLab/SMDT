from __future__ import annotations
import zipfile
from typing import Iterable, Optional, Any, Mapping, Callable
from pathlib import Path

from .base import Reader
from .registry import get_reader


class ZipReader(Reader):
    """
    Generic ZIP reader that delegates each member file to an appropriate reader.
    - Iterates over members in the archive.
    - For each member, finds a registered reader (by extension/content_type).
    - Streams records yielded by that reader.
    """

    name = "zip"

    def __init__(
        self,
        member_filter: Optional[Callable[[str], bool]] = None,
    ):
        self.member_filter = member_filter

    def supports(self, uri: str, *, content_type: Optional[str] = None) -> bool:
        return uri.lower().endswith(".zip")

    def stream(self, uri: str, **kwargs) -> Iterable[Mapping[str, Any]]:
        with zipfile.ZipFile(uri, "r") as zf:
            for info in zf.infolist():
                member_name = info.filename
                if self.member_filter and not self.member_filter(member_name):
                    continue
                # Skip directories
                if member_name.endswith("/"):
                    continue

                # delegate based on member extension
                reader = get_reader(member_name)
                if not reader:
                    # no reader for this member → skip or warn
                    continue

                with zf.open(info, "r") as raw:
                    # Some readers expect a path, others accept file-like
                    if hasattr(reader, "stream_from_filelike"):
                        yield from reader.stream_from_filelike(
                            raw, name=member_name, **kwargs
                        )
                    else:
                        # fallback: write to temp file if needed (not ideal for big zips)
                        # or extend your Reader base to support filelike by default
                        raise RuntimeError(
                            f"Reader {reader.name} does not support file-like input (member={member_name})"
                        )


from . import registry

registry.register(ZipReader())
