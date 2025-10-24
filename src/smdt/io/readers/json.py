from __future__ import annotations
import io
import json_repair
from typing import Iterable, Mapping, Any, Optional, BinaryIO

from .base import Reader, MissingOptionalDependency
from .utils import open_local_binary, maybe_decompress, file_ext


class JsonReader(Reader):
    """
    Reader for plain JSON files.

    Modes:
      - Default (stream_array=False): load the full JSON doc with json.load().
          • If top-level is an array -> yield each element
          • If top-level is an object -> yield that object once
      - Streaming arrays (stream_array=True): use ijson to stream each element
          • Memory-safe for huge top-level arrays
          • Requires the optional 'ijson' dependency

    Compression:
      - Transparent .gz / .bz2 / .xz / .zst via maybe_decompress()
    """

    name = "json"

    def supports(self, uri: str, *, content_type: Optional[str] = None) -> bool:
        ext = file_ext(uri)
        return (
            ext.endswith(".json")
            or ext.endswith(".json.gz")
            or ext.endswith(".json.bz2")
            or ext.endswith(".json.xz")
            or ext.endswith(".json.zst")
        )

    # ---------- path-based ----------

    def stream(self, uri: str, **kwargs) -> Iterable[Mapping[str, Any]]:
        """
        Kwargs:
          - stream_array: bool = False
              If True, stream a top-level array using ijson (memory-safe).
          - ijson_prefix: str = 'item'
              The prefix used with ijson.items() when streaming arrays.
          - (any other kwargs are ignored here)
        """
        stream_array: bool = kwargs.pop("stream_array", False)
        ijson_prefix: str = kwargs.pop("ijson_prefix", "item")

        f = open_local_binary(uri)
        wrapped = maybe_decompress(f, uri)
        try:
            if stream_array:
                yield from self._stream_array_with_ijson(wrapped, ijson_prefix)
            else:
                yield from self._load_entire_doc(wrapped)
        finally:
            try:
                wrapped.close()
            finally:
                f.close()

    # ---------- file-like ----------

    def stream_from_filelike(
        self, f: BinaryIO, **kwargs
    ) -> Iterable[Mapping[str, Any]]:
        """
        Handle already-opened binary file-like (e.g. zip/tar member).
        Respects the same kwargs as .stream().
        """
        stream_array: bool = kwargs.pop("stream_array", False)
        ijson_prefix: str = kwargs.pop("ijson_prefix", "item")

        if stream_array:
            # file-like is already the decompressed member; stream via ijson
            yield from self._stream_array_with_ijson(f, ijson_prefix)
        else:
            # full-load path: wrap as text and json.load
            yield from self._load_entire_doc(f)

    # ---------- internals ----------

    def _load_entire_doc(self, bin_f: BinaryIO) -> Iterable[Mapping[str, Any]]:
        """
        Load full JSON document and yield:
          - each item if it's a top-level list
          - the object itself if it's a dict
        """
        text_stream = io.TextIOWrapper(bin_f, encoding="utf-8")
        data = json_repair.load(text_stream)
        if isinstance(data, list):
            for obj in data:
                yield obj
        elif isinstance(data, dict):
            yield data
        else:
            raise ValueError(f"Unsupported JSON top-level type: {type(data)}")

    def _stream_array_with_ijson(
        self, bin_f: BinaryIO, prefix: str
    ) -> Iterable[Mapping[str, Any]]:
        """
        Stream a top-level array using ijson.items(file_like, prefix).
        Raises MissingOptionalDependency if ijson isn't installed.

        Note:
          This expects the JSON root to be an array; if the file is an object,
          ijson will yield nothing. If you need auto-detection, avoid ijson and
          use _load_entire_doc() instead (but that loads the whole doc).
        """
        try:
            import ijson  # optional dependency
        except Exception as e:
            raise MissingOptionalDependency(
                "Streaming large JSON arrays requires 'ijson'. "
                "Install: pip install 'smdt[ijson]'"
            ) from e

        # Wrap as text for ijson to handle encoding properly
        text_stream = io.TextIOWrapper(bin_f, encoding="utf-8")
        for obj in ijson.items(text_stream, prefix):
            yield obj


# register
from . import registry

registry.register(JsonReader())
