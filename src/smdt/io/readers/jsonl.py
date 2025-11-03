from __future__ import annotations
import io
import orjson
import json_repair
from typing import Iterable, Mapping, Any, Optional, BinaryIO

from .base import Reader
from .utils import open_for_reading, maybe_decompress, file_ext


class JsonlReader(Reader):
    name = "jsonl"

    def supports(self, uri: str, *, content_type: Optional[str] = None) -> bool:
        ext = file_ext(uri)
        return ext.endswith(
            (
                ".jsonl",
                ".ndjson",
                ".jsons",
                ".jsonl.gz",
                ".ndjson.gz",
                ".jsons.gz",
                ".jsonl.bz2",
                ".ndjson.bz2",
                ".jsons.bz2",
                ".jsonl.xz",
                ".ndjson.xz",
                ".jsons.xz",
                ".jsonl.zst",
                ".ndjson.zst",
                ".jsons.zst",
            )
        )

    def _iter_text_lines(self, text_fh: io.TextIOBase) -> Iterable[str]:
        first = text_fh.readline()
        if first:
            yield first
        yield from text_fh  # preserves streaming

    # ---- path-based ----
    def stream(self, uri: str, **kwargs) -> Iterable[Mapping[str, Any]]:
        f = open_for_reading(uri)  # handles all compression
        text_stream = io.TextIOWrapper(f, encoding="utf-8", newline="")
        try:
            for line in self._iter_text_lines(text_stream):
                s = line.strip()
                if not s:
                    continue
                try:
                    yield orjson.loads(s)
                except orjson.JSONDecodeError:
                    yield json_repair.loads(s)
                except Exception:
                    # skip malformed line but keep streaming
                    continue
        finally:
            try:
                text_stream.close()
            finally:
                f.close()

    # ---- file-like (archive member) ----
    def stream_from_filelike(
        self, f: BinaryIO, **kwargs
    ) -> Iterable[Mapping[str, Any]]:
        member_name: Optional[str] = kwargs.pop("member_name", kwargs.pop("name", None))
        f_dec = maybe_decompress(f, member_name) if member_name else f
        text_stream = io.TextIOWrapper(f_dec, encoding="utf-8", newline="")
        for line in self._iter_text_lines(text_stream):
            s = line.strip()
            if not s:
                continue
            try:
                yield orjson.loads(s)
            except orjson.JSONDecodeError:
                yield json_repair.loads(s)
            except Exception:
                continue


from . import registry

registry.register(JsonlReader())
