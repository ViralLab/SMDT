from __future__ import annotations

from typing import Iterable, Mapping, Any, Optional, BinaryIO, Sequence

import pyarrow as pa
import pyarrow.parquet as pq

from .base import Reader
from .utils import open_for_reading, maybe_decompress, file_ext


class ParquetReader(Reader):
    """Reader for Parquet files (including compressed containers)."""

    name = "parquet"

    # Support both "parquet.[codec]" and "[codec].parquet" naming styles
    SUPPORTED_EXTENSIONS = (
        ".parquet",
        ".parquet.gz",
        ".parquet.gzip",
        ".parquet.bz2",
        ".parquet.xz",
        ".parquet.zst",
        ".gz.parquet",
        ".gzip.parquet",
        ".bz2.parquet",
        ".xz.parquet",
        ".zst.parquet",
    )

    def supports(self, uri: str, *, content_type: Optional[str] = None) -> bool:
        """Check if the reader supports the given URI.

        Supports .parquet and common compressed variants.
        """
        ext = file_ext(uri).lower()
        return any(ext.endswith(suffix) for suffix in self.SUPPORTED_EXTENSIONS)

    # ------------------------------------------------------------------
    # Internal helper
    # ------------------------------------------------------------------
    def _iter_parquet_file(
        self,
        pf: pq.ParquetFile,
        *,
        columns: Optional[Sequence[str]] = None,
        batch_size: Optional[int] = None,
    ) -> Iterable[Mapping[str, Any]]:
        """Iterate over rows of a ParquetFile as dictionaries."""
        # Only pass batch_size if it's an int; PyArrow will choke on None
        if batch_size is not None:
            batch_iter = pf.iter_batches(columns=columns, batch_size=batch_size)
        else:
            batch_iter = pf.iter_batches(columns=columns)

        for batch in batch_iter:
            # each batch.to_pylist() is a list[dict] mapping col -> value
            for row in batch.to_pylist():
                yield row

    # ------------------------------------------------------------------
    # Streaming from a path
    # ------------------------------------------------------------------
    def stream(
        self,
        uri: str,
        **kwargs: Any,
    ) -> Iterable[Mapping[str, Any]]:
        """Stream records from a Parquet file on disk.

        Args:
            uri: Path or URI to a Parquet (or compressed Parquet) file.
            **kwargs:
                columns: Optional list of column names to read; reads all if None.
                batch_size: Optional batch size (int) used by PyArrow when iterating.

        Yields:
            Parsed rows as dictionaries.
        """
        columns: Optional[Sequence[str]] = kwargs.pop("columns", None)
        raw_batch_size = kwargs.pop("batch_size", None)

        batch_size: Optional[int]
        if raw_batch_size is None:
            batch_size = None
        else:
            # Be forgiving if someone passes a string
            batch_size = int(raw_batch_size)

        with open_for_reading(uri) as raw_fh:
            pf = pq.ParquetFile(raw_fh)
            yield from self._iter_parquet_file(
                pf,
                columns=columns,
                batch_size=batch_size,
            )

    # ------------------------------------------------------------------
    # Streaming from a file-like object (e.g., archive member)
    # ------------------------------------------------------------------
    def stream_from_filelike(
        self,
        f: BinaryIO,
        **kwargs: Any,
    ) -> Iterable[Mapping[str, Any]]:
        """Stream records from a file-like object containing Parquet data.

        Args:
            f: Binary file-like object.
            **kwargs:
                member_name / name: Optional name of the member (for compression detection).
                columns: Optional list of column names to read; reads all if None.
                batch_size: Optional batch size (int) used by PyArrow when iterating.

        Yields:
            Parsed rows as dictionaries.
        """
        member_name: Optional[str] = kwargs.pop("member_name", kwargs.pop("name", None))
        columns: Optional[Sequence[str]] = kwargs.pop("columns", None)
        raw_batch_size = kwargs.pop("batch_size", None)

        batch_size: Optional[int]
        if raw_batch_size is None:
            batch_size = None
        else:
            batch_size = int(raw_batch_size)

        # If this is a compressed member (e.g., *.parquet.gz inside a tar),
        # let maybe_decompress handle it. Otherwise, use the raw file-like.
        f_dec: BinaryIO = maybe_decompress(f, member_name) if member_name else f

        pf = pq.ParquetFile(f_dec)
        yield from self._iter_parquet_file(
            pf,
            columns=columns,
            batch_size=batch_size,
        )


# Register reader
from . import registry

registry.register(ParquetReader())
