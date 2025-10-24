from __future__ import annotations
import sys
from typing import Iterable, Mapping, Any, Optional, BinaryIO

from .base import Reader, MissingOptionalDependency
from .utils import file_ext, open_local_binary, maybe_decompress

import inspect


class PandasCsvReader(Reader):
    """
    Streams CSV rows using pandas in chunked mode.
    Yields dictionaries per row, avoids loading the whole file into memory.

    Supported:
      - *.csv
      - *.csv.gz, *.csv.bz2, *.csv.xz, *.csv.zst  (decompressed via maybe_decompress)

    Optional dependencies:
      - pandas (required):        pip install 'smdt[pandas]'
      - zstandard (for .zst):     pip install 'smdt[zstd]'
    """

    name = "csv_pd"

    def _pd(self):
        try:
            import pandas as pd

            return pd
        except Exception as e:
            raise MissingOptionalDependency(
                "Pandas CSV reader requires pandas. "
                "Install the optional extra: pip install 'smdt[pandas]'"
            ) from e

    def supports(self, uri: str, *, content_type: Optional[str] = None) -> bool:
        ext = file_ext(uri)
        return (
            ext.endswith(".csv")
            or ext.endswith(".csv.gz")
            or ext.endswith(".csv.bz2")
            or ext.endswith(".csv.xz")
            or ext.endswith(".csv.zst")
            or ext.endswith(".tsv")
            or ext.endswith(".tsv.gz")
            or ext.endswith(".tsv.bz2")
            or ext.endswith(".tsv.xz")
            or ext.endswith(".tsv.zst")
        )

    def stream(self, uri: str, **kwargs) -> Iterable[Mapping[str, Any]]:
        """
        Parameters (forwarded to pandas.read_csv):
          - chunksize: int (default 10_000)
          - dtype, sep, encoding, usecols, etc.

        Note:
          We pass a *decompressed* file-like to pandas, so no need to set
          compression=... here. All compression is handled by maybe_decompress().
        """
        pd = self._pd()
        chunksize = kwargs.pop("chunksize", 10_000)
        _VALID_READ_CSV_KWARGS = set(inspect.signature(pd.read_csv).parameters)
        kwargs = {k: v for k, v in kwargs.items() if k in _VALID_READ_CSV_KWARGS}

        f = open_local_binary(uri)
        wrapped = maybe_decompress(f, uri)  # handles .gz/.bz2/.xz/.zst if installed
        try:
            for chunk in pd.read_csv(wrapped, chunksize=chunksize, **kwargs):
                for rec in chunk.to_dict(orient="records"):
                    yield rec
        finally:
            try:
                wrapped.close()
            finally:
                f.close()

    def stream_from_filelike(
        self, f: BinaryIO, **kwargs
    ) -> Iterable[Mapping[str, Any]]:
        """
        Handle an already-opened binary file-like (e.g., from a zip/tar member).
        Archive modules (zipfile/tarfile) already deliver decompressed streams,
        so we can feed them directly to pandas.
        """
        pd = self._pd()
        chunksize = kwargs.pop("chunksize", 10_000)
        _VALID_READ_CSV_KWARGS: set[str] = set(
            inspect.signature(pd.read_csv).parameters
        )
        kwargs = {k: v for k, v in kwargs.items() if k in _VALID_READ_CSV_KWARGS}
        for chunk in pd.read_csv(f, chunksize=chunksize, **kwargs):
            for rec in chunk.to_dict(orient="records"):
                yield rec


from . import registry

registry.register(PandasCsvReader())
