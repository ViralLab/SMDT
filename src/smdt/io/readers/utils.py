# utils.py
from __future__ import annotations
from typing import IO, Optional
from pathlib import Path
import io
import gzip, bz2, lzma

try:
    import zstandard as zstd  # optional
except Exception:  # pragma: no cover
    zstd = None  # type: ignore


def file_ext(uri: str) -> str:
    """Get the lowercase file extension from a URI.

    Handles multi-part extensions like .tar.gz.

    Args:
        uri: URI or file path.

    Returns:
        Lowercase extension.
    """
    p = Path(uri)
    suf = "".join(p.suffixes).lower()
    return suf if suf else p.suffix.lower()


def open_local_binary(uri: str) -> IO[bytes]:
    """Open a local file for binary reading.

    Args:
        uri: File path.

    Returns:
        Binary file object.
    """
    return open(uri, "rb")


def maybe_decompress(f: IO[bytes], name_or_ext: Optional[str]) -> IO[bytes]:
    """Wrap a file object with a decompressor if indicated by the extension.

    Supports .gz, .bz2, .xz, .zst.

    Args:
        f: Binary file object.
        name_or_ext: Filename or extension to check.

    Returns:
        Decompressed file object or the original file object.

    Raises:
        RuntimeError: If zstandard is required but not installed.
    """
    if not name_or_ext:
        return f
    low = name_or_ext.lower()
    if low.endswith(".gz"):
        return gzip.GzipFile(fileobj=f)
    if low.endswith(".bz2"):
        return bz2.BZ2File(f)
    if low.endswith(".xz"):
        return lzma.LZMAFile(f)
    if low.endswith(".zst") or low.endswith(".zstd"):
        if zstd is None:
            raise RuntimeError(
                "zstandard is required to read .zst; install 'smdt[zstd]'"
            )
        d = zstd.ZstdDecompressor()
        return d.stream_reader(f)  # returns a file-like object
    return f


def open_for_reading(uri: str) -> IO[bytes]:
    """Open any local file for binary reading, with compression handled.

    Returns a file-like that, when closed, closes both the decompressor (if any)
    and the underlying base file.

    Args:
        uri: File path.

    Returns:
        Binary file object (possibly wrapped).
    """
    base = open_local_binary(uri)
    wrapped = maybe_decompress(base, file_ext(uri))

    if wrapped is base:
        # no compression wrapper; caller will close 'base'
        return base

    class _CloseBoth(io.IOBase):
        __slots__ = ("_p", "_b")  # primary (wrapped), base

        def __init__(self, primary: IO[bytes], basef: IO[bytes]) -> None:
            self._p = primary
            self._b = basef

        # --- minimal delegation for binary reads ---
        def read(self, *args, **kwargs):  # type: ignore[override]
            return self._p.read(*args, **kwargs)

        def readinto(self, b):  # optional optimization if available
            if hasattr(self._p, "readinto"):
                return self._p.readinto(b)  # type: ignore[attr-defined]
            data = self._p.read(len(b))
            n = len(data)
            b[:n] = data
            return n

        def readable(self) -> bool:  # type: ignore[override]
            return True

        def seek(self, *args, **kwargs):  # type: ignore[override]
            if hasattr(self._p, "seek"):
                return self._p.seek(*args, **kwargs)
            raise io.UnsupportedOperation("seek")

        def tell(self):  # type: ignore[override]
            if hasattr(self._p, "tell"):
                return self._p.tell()
            raise io.UnsupportedOperation("tell")

        def close(self) -> None:  # type: ignore[override]
            # Close primary first, then base, then mark ourselves closed
            try:
                try:
                    if hasattr(self._p, "close"):
                        self._p.close()
                finally:
                    self._b.close()
            finally:
                super().close()

        # Optional: expose 'closed' property via IOBase
        @property
        def closed(self) -> bool:  # type: ignore[override]
            # If either was closed, consider this closed
            try:
                return super().closed
            except Exception:
                return False

    return _CloseBoth(wrapped, base)
