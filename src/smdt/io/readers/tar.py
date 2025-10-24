# src/smdt/io/readers/tar.py
from __future__ import annotations
import tarfile, tempfile, shutil
from pathlib import Path
from typing import Optional, Callable, Iterable, Mapping, Any
from fnmatch import fnmatch

from .base import Reader
from .registry import get_reader


class TarReader(Reader):
    name = "tar"

    def __init__(self, *, member_filter: Optional[Callable[[str], bool]] = None):
        self.member_filter = member_filter

    def supports(self, uri: str, *, content_type: Optional[str] = None) -> bool:
        u = uri.lower()
        return u.endswith((".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tar.xz"))

    def stream(self, uri: str, **kwargs) -> Iterable[Mapping[str, Any]]:
        member_filter = kwargs.get("member_filter", self.member_filter)
        include = tuple(kwargs.get("include", ())) or None
        exclude = tuple(kwargs.get("exclude", ())) or None

        # Don’t leak archive-only kwargs to child readers
        child_kwargs = {
            k: v
            for k, v in kwargs.items()
            if k not in ("member_filter", "include", "exclude")
        }

        def want(name: str) -> bool:
            if member_filter:
                return member_filter(name)
            ok_inc = True if not include else any(fnmatch(name, pat) for pat in include)
            ok_exc = (
                True if not exclude else not any(fnmatch(name, pat) for pat in exclude)
            )
            return ok_inc and ok_exc

        # Lazy iteration over members (avoid getmembers())
        with tarfile.open(uri, "r:*") as tf:
            for member in tf:
                if not member.isfile():
                    continue
                mname = member.name
                if not want(mname):
                    continue

                reader = get_reader(mname)
                if not reader:
                    continue

                fobj = tf.extractfile(member)
                if fobj is None:
                    continue

                stream_filelike = getattr(reader, "stream_from_filelike", None)
                if callable(stream_filelike):
                    # Ensure the member stream is closed after streaming
                    with fobj:
                        yield from stream_filelike(fobj, name=mname, **child_kwargs)
                else:
                    # Fallback: materialize member to a temp file for readers that need a path
                    suffix = "".join(Path(mname).suffixes) or ".bin"
                    with tempfile.NamedTemporaryFile(
                        suffix=suffix, delete=False
                    ) as tmp:
                        with fobj:
                            shutil.copyfileobj(fobj, tmp, length=1024 * 1024)
                        tmp_path = tmp.name
                    try:
                        yield from reader.stream(tmp_path, **child_kwargs)
                    finally:
                        try:
                            Path(tmp_path).unlink(missing_ok=True)
                        except Exception:
                            pass


from . import registry

registry.register(TarReader())
