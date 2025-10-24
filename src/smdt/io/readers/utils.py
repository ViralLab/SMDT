from __future__ import annotations
import gzip, bz2, lzma, io
from pathlib import Path
from urllib.parse import urlparse
from typing import BinaryIO, Optional

# Optional zstd (install: pip install zstandard)
try:
    import zstandard as zstd  # type: ignore
except Exception:  # keep import cheap even if not installed
    zstd = None


def file_ext(uri: str) -> str:
    """
    Return combined suffixes in lowercase (preserves multi-extensions like '.jsonl.gz').
    """
    p = Path(urlparse(uri).path)
    return "".join(p.suffixes).lower()


def open_local_binary(uri: str) -> BinaryIO:
    return open(uri, "rb")


def maybe_decompress(f: BinaryIO, uri: str) -> BinaryIO:
    """
    Wrap `f` with a decompressor based on file extension.
    Supports: .gz, .bz2, .xz, .zst
    """
    ext = file_ext(uri)
    if ext.endswith(".gz"):
        return gzip.GzipFile(fileobj=f, mode="rb")
    if ext.endswith(".bz2"):
        return bz2.BZ2File(f, "rb")
    if ext.endswith(".xz"):
        return lzma.LZMAFile(f, "rb")
    if ext.endswith(".zst"):
        if zstd is None:
            raise RuntimeError(
                "Zstandard file detected but 'zstandard' is not installed. "
                "Install it with: pip install zstandard  (or 'smdt[zstd]')"
            )
        # Streamed decompression; wrap in a buffered reader for line iteration
        d = zstd.ZstdDecompressor()
        r = d.stream_reader(f)  # returns a file-like object
        return io.BufferedReader(r, buffer_size=1024 * 1024)
    return f
