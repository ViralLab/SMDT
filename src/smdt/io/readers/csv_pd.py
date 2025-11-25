from __future__ import annotations
from typing import Iterable, Mapping, Any, Optional, BinaryIO
import inspect

from .base import Reader, MissingOptionalDependency
from .utils import file_ext, open_for_reading, maybe_decompress

from smdt.standardizers.row import Record


class PandasCsvReader(Reader):
    """Reader for CSV/TSV files using pandas."""
    name = "csv_pd"

    def _pd(self):
        """Lazy import of pandas.

        Returns:
            The pandas module.

        Raises:
            MissingOptionalDependency: If pandas is not installed.
        """
        try:
            import pandas as pd

            return pd
        except Exception as e:
            raise MissingOptionalDependency(
                "Pandas CSV reader requires pandas. Install 'smdt[pandas]'"
            ) from e

    def supports(self, uri: str, *, content_type: Optional[str] = None) -> bool:
        """Check if the reader supports the given URI.

        Supports .csv, .tsv, .tab and their compressed variants.

        Args:
            uri: URI to check.
            content_type: Optional content type hint.

        Returns:
            True if supported, False otherwise.
        """
        ext = file_ext(uri)
        return ext.endswith(
            (
                ".csv",
                ".csv.gz",
                ".csv.bz2",
                ".csv.xz",
                ".csv.zst",
                ".tsv",
                ".tsv.gz",
                ".tsv.bz2",
                ".tsv.xz",
                ".tsv.zst",
                ".tab",
                ".tab.gz",
                ".tab.bz2",
                ".tab.xz",
                ".tab.zst",
            )
        )

    def stream(self, uri: str, **kwargs: Any) -> Iterable[Mapping[str, Any]]:
        """Stream records from a CSV/TSV file.

        Args:
            uri: URI to read from.
            **kwargs: Additional arguments passed to pandas.read_csv.

        Yields:
            Dictionary representing a record.
        """
        # open_for_reading handles compression automatically
        f = open_for_reading(uri)
        try:
            yield from self._stream_from_filelike(f, member_name=uri, **kwargs)
        finally:
            try:
                f.close()
            except Exception:
                pass

    def stream_from_filelike(
        self, f: BinaryIO, **kwargs: Any
    ) -> Iterable[Mapping[str, Any]]:
        """Stream records from a file-like object.

        Args:
            f: File-like object.
            **kwargs: Additional arguments.

        Yields:
            Dictionary representing a record.
        """
        # If archive passes a compressed member (e.g., *.csv.gz), wrap it
        member_name = kwargs.pop("member_name", kwargs.pop("name", None))
        f_dec = maybe_decompress(f, member_name) if member_name else f
        return self._stream_from_filelike(f_dec, member_name=member_name, **kwargs)

    # ---------- core ----------
    def _stream_from_filelike(
        self, f: BinaryIO, *, member_name: Optional[str], **kwargs: Any
    ) -> Iterable[Mapping[str, Any]]:
        """Internal method to stream from a file-like object using pandas.

        Args:
            f: File-like object.
            member_name: Name of the member (for logging/debugging).
            **kwargs: Additional arguments for pandas.read_csv.

        Yields:
            Dictionary representing a record.
        """
        pd = self._pd()
        params = set(inspect.signature(pd.read_csv).parameters)
        # Let caller override chunksize; default to 10_000
        chunksize = kwargs.pop("chunksize", 10_000)
        safe_kwargs = {k: v for k, v in kwargs.items() if k in params}
        safe_kwargs["chunksize"] = chunksize

        # Let caller pass sep='\t' for TSV, dtype, encoding, usecols, etc.
        # pd.read_csv(..., chunksize=N) yields DataFrame chunks
        for chunk in pd.read_csv(f, **safe_kwargs):
            # Build shared column metadata *once per chunk*
            cols = list(chunk.columns)
            index = {name: i for i, name in enumerate(cols)}

            # Iterate rows as tuples instead of dicts
            # Each `tup` is (col0_value, col1_value, ...)
            for tup in chunk.itertuples(index=False, name=None):
                # Record is a Mapping[str, Any], backed by (tup, index, cols)
                rec = Record(tup, index, cols)
                yield rec


# register
from . import registry

registry.register(PandasCsvReader())
