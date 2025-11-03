from __future__ import annotations
from typing import Iterable, Mapping, Any, Optional, BinaryIO
import inspect

from .base import Reader, MissingOptionalDependency
from .utils import file_ext, open_for_reading, maybe_decompress


class PandasCsvReader(Reader):
    name = "csv_pd"

    def _pd(self):
        try:
            import pandas as pd

            return pd
        except Exception as e:
            raise MissingOptionalDependency(
                "Pandas CSV reader requires pandas. Install 'smdt[pandas]'"
            ) from e

    def supports(self, uri: str, *, content_type: Optional[str] = None) -> bool:
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
        # If archive passes a compressed member (e.g., *.csv.gz), wrap it
        member_name = kwargs.pop("member_name", kwargs.pop("name", None))
        f_dec = maybe_decompress(f, member_name) if member_name else f
        return self._stream_from_filelike(f_dec, member_name=member_name, **kwargs)

    # ---------- core ----------
    def _stream_from_filelike(
        self, f: BinaryIO, *, member_name: Optional[str], **kwargs: Any
    ) -> Iterable[Mapping[str, Any]]:
        pd = self._pd()
        # chunksize = kwargs.pop("chunksize", 10_000)
        params = set(inspect.signature(pd.read_csv).parameters)
        chunksize = kwargs.pop("chunksize", 10_000)
        safe_kwargs = {k: v for k, v in kwargs.items() if k in params}
        safe_kwargs["chunksize"] = chunksize

        # Let caller pass sep='\t' for TSV, dtype, encoding, usecols, etc.
        for chunk in pd.read_csv(f, **safe_kwargs):
            for rec in chunk.to_dict(orient="records"):
                yield rec


# register
from . import registry

registry.register(PandasCsvReader())
