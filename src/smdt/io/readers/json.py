from __future__ import annotations
import io
import json_repair
from typing import Iterable, Mapping, Any, Optional, BinaryIO

from .base import Reader, MissingOptionalDependency
from .utils import open_for_reading, maybe_decompress, file_ext


class JsonReader(Reader):
    """
    Reader for JSON and JSON(.gz/.bz2/.xz/.zst) files.

    Modes:
      - Default: load full JSON doc via json_repair.load()
          • If top-level is a list → yield each element
          • If top-level is a dict → yield once
      - stream_array=True: memory-safe streaming of large top-level arrays via ijson

    Compression: handled transparently via open_for_reading() and maybe_decompress()
    """

    name = "json"

    # -------------------- detection --------------------

    def supports(self, uri: str, *, content_type: Optional[str] = None) -> bool:
        """Check if the reader supports the given URI.

        Supports .json and its compressed variants.

        Args:
            uri: URI to check.
            content_type: Optional content type hint.

        Returns:
            True if supported, False otherwise.
        """
        ext = file_ext(uri)
        return ext.endswith((".json", ".json.gz", ".json.bz2", ".json.xz", ".json.zst"))

    # -------------------- path-based --------------------

    def stream(self, uri: str, **kwargs) -> Iterable[Mapping[str, Any]]:
        """Stream records from a JSON file.

        Args:
            uri: URI to read from.
            **kwargs: Additional arguments.
                stream_array (bool): If True, stream a top-level array using ijson.
                ijson_prefix (str): Prefix passed to ijson.items() when streaming arrays.

        Yields:
            Dictionary representing a record.
        """
        stream_array: bool = kwargs.pop("stream_array", False)
        ijson_prefix: str = kwargs.pop("ijson_prefix", "item")

        f = open_for_reading(uri)  # handles compression
        try:
            if stream_array:
                yield from self._stream_array_with_ijson(f, ijson_prefix)
            else:
                yield from self._load_entire_doc(f)
        finally:
            try:
                f.close()
            except Exception:
                pass

    # -------------------- file-like --------------------

    def stream_from_filelike(
        self, f: BinaryIO, **kwargs
    ) -> Iterable[Mapping[str, Any]]:
        """Stream records from a file-like object.

        Handle already-opened binary file-like (e.g., from .tar or .zip).
        Automatically decompresses based on member_name if needed.

        Args:
            f: File-like object.
            **kwargs: Additional arguments.

        Yields:
            Dictionary representing a record.
        """
        stream_array: bool = kwargs.pop("stream_array", False)
        ijson_prefix: str = kwargs.pop("ijson_prefix", "item")
        member_name: Optional[str] = kwargs.pop("member_name", kwargs.pop("name", None))

        # Decompress again if this member itself is compressed (tar → foo.json.gz)
        f_dec = maybe_decompress(f, member_name) if member_name else f

        if stream_array:
            yield from self._stream_array_with_ijson(f_dec, ijson_prefix)
        else:
            yield from self._load_entire_doc(f_dec)

    # -------------------- internals --------------------

    def _load_entire_doc(self, bin_f: BinaryIO) -> Iterable[Mapping[str, Any]]:
        """Load full JSON document (via json_repair) and yield items.

        Yields:
            - each item if top-level list
            - the object itself if dict

        Args:
            bin_f: Binary file object.

        Yields:
            Dictionary representing a record.

        Raises:
            ValueError: If the top-level JSON type is not a list or dict.
        """
        text_stream = io.TextIOWrapper(bin_f, encoding="utf-8")
        data = json_repair.load(text_stream)

        if isinstance(data, list):
            for obj in data:
                yield obj
        elif isinstance(data, dict):
            yield data
        else:
            raise ValueError(f"Unsupported top-level JSON type: {type(data)}")

    def _stream_array_with_ijson(
        self, bin_f: BinaryIO, prefix: str
    ) -> Iterable[Mapping[str, Any]]:
        """Stream large top-level arrays safely using ijson.items().

        Args:
            bin_f: Binary file object.
            prefix: ijson prefix string.

        Yields:
            Dictionary representing a record.

        Raises:
            MissingOptionalDependency: If ijson is not installed.
        """
        try:
            import ijson
        except Exception as e:
            raise MissingOptionalDependency(
                "Streaming large JSON arrays requires 'ijson'. "
                "Install with: pip install 'smdt[ijson]'"
            ) from e

        text_stream = io.TextIOWrapper(bin_f, encoding="utf-8")
        for obj in ijson.items(text_stream, prefix):
            yield obj


# register
from . import registry

registry.register(JsonReader())
