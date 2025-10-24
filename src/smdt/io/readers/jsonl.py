from __future__ import annotations
import io, gzip, json_repair
from typing import Iterable, Mapping, Any, Optional

from .base import Reader
from .utils import open_local_binary, maybe_decompress, file_ext


class JsonlReader(Reader):
    name = "jsonl"

    def supports(self, uri: str, *, content_type: Optional[str] = None) -> bool:
        ext = file_ext(uri)
        return (
            ext.endswith(".jsonl")
            or ext.endswith(".ndjson")
            or ext.endswith(".jsons")
            or ext.endswith(".jsonl.gz")
            or ext.endswith(".ndjson.gz")
            or ext.endswith(".jsons.gz")
            or ext.endswith(".jsonl.bz2")
            or ext.endswith(".ndjson.bz2")
            or ext.endswith(".jsons.bz2")
            or ext.endswith(".jsonl.xz")
            or ext.endswith(".ndjson.xz")
            or ext.endswith(".jsons.xz")
            or ext.endswith(".jsonl.zst")
            or ext.endswith(".ndjson.zst")
            or ext.endswith(".jsons.zst")
        )

    def _iter_text_lines(self, text_fh: io.TextIOBase) -> Iterable[str]:
        # fast path: try a single readline() to surface the first record quickly
        first = text_fh.readline()
        if first:
            yield first
        for line in text_fh:
            yield line

    def stream(self, uri: str, **kwargs) -> Iterable[Mapping[str, Any]]:
        ext = file_ext(uri)
        # Special-case .gz paths: gzip can give us a text stream directly
        if ext.endswith(".gz"):
            with gzip.open(uri, mode="rt") as tf:
                for line in tf:
                    if not line:
                        continue
                    yield json_repair.loads(line)
            return

        # All other cases: open binary, wrap with decompressor, then text
        f = open_local_binary(uri)
        wrapped = maybe_decompress(f, uri)  # returns a binary, streaming file-like
        text_stream = io.TextIOWrapper(wrapped, encoding="utf-8", newline="")
        try:
            for line in self._iter_text_lines(text_stream):
                line = line.strip()
                if not line:
                    continue
                yield json_repair.loads(line)
        finally:
            try:
                text_stream.close()
            finally:
                f.close()

    def stream_from_filelike(self, f, **kwargs) -> Iterable[Mapping[str, Any]]:
        # Archive members (zip/tar) arrive as binary; wrap once as text
        text_stream = io.TextIOWrapper(f, encoding="utf-8", newline="")
        for line in self._iter_text_lines(text_stream):
            if not line:
                continue
            yield json_repair.loads(line)


from . import registry

registry.register(JsonlReader())
