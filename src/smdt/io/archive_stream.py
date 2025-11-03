# smdt/io/readers/archive_stream.py
from __future__ import annotations

import logging
import tarfile
import zipfile
from typing import Any, Callable, Dict, Iterable, Iterator, Mapping, Tuple

from smdt.io.readers.registry import read_from_filelike
from smdt.standardizers.base import SourceInfo

log = logging.getLogger(__name__)

ReaderKwargsFn = Callable[[str, str | None], Dict[str, Any]]
Record = Tuple[Mapping[str, Any], SourceInfo]


def _yield_from_filelike(
    archive_path: str,
    member_name: str,
    file_like,
    *,
    hints: Dict[str, Any] | None,
    reader_kwargs_for: ReaderKwargsFn,
    reader_name_fallback: str | None,
) -> Iterator[Record]:
    """
    Stream records from a file-like object using the registry handoff.
    Ensures nested compression is handled via member_name.
    """
    # Derive sub-reader name hint (if any) and resolve kwargs
    sub_reader_name = reader_name_fallback
    rk = reader_kwargs_for(member_name, sub_reader_name)

    src = SourceInfo(path=archive_path, member=member_name, hints=hints)

    # Single handoff point: will use stream_from_filelike(...) if available,
    # otherwise materialize to a temp file and call read(...).
    for rec in read_from_filelike(file_like, member_name=member_name, **rk):
        yield rec, src


def stream_archive_records(
    *,
    archive_path: str,
    members: Iterable[Any],
    hints: Dict[str, Any] | None,
    reader_kwargs_for: ReaderKwargsFn,
    reader_name_fallback: str | None,
) -> Iterator[Record]:
    """
    Yield (record, SourceInfo) for each included archive member in the given order.

    Parameters
    ----------
    archive_path : str
        Path to the .zip or .tar.* archive on disk.
    members : Iterable[Any]
        Iterable of plan members with attributes:
          - m.name : str
          - m.included : bool
          - m.reader_name : Optional[str]  (hint; not required)
    hints : Optional[Dict[str, Any]]
        Extra metadata propagated into SourceInfo.
    reader_kwargs_for : Callable[[member_path, reader_name], dict]
        Resolves reader kwargs by extension/reader name.
    reader_name_fallback : Optional[str]
        Reader name from the parent file plan, used as a hint.
    """
    lower = archive_path.lower()

    if lower.endswith(".zip"):
        with zipfile.ZipFile(archive_path, "r") as zf:
            for m in members:
                if not getattr(m, "included", False):
                    continue
                mname = getattr(m, "name", None)
                if not mname:
                    continue
                try:
                    with zf.open(mname, "r") as fobj:
                        yield from _yield_from_filelike(
                            archive_path,
                            mname,
                            fobj,
                            hints=hints,
                            reader_kwargs_for=reader_kwargs_for,
                            reader_name_fallback=reader_name_fallback,
                        )
                except KeyError:
                    log.debug("Zip member missing: %s::%s", archive_path, mname)
        return

    # tar / tgz / tbz2 / txz
    with tarfile.open(archive_path, "r:*") as tf:
        for m in members:
            if not getattr(m, "included", False):
                continue
            mname = getattr(m, "name", None)
            if not mname:
                continue
            try:
                ti = tf.getmember(mname)
            except KeyError:
                log.debug("Tar member missing: %s::%s", archive_path, mname)
                continue
            fobj = tf.extractfile(ti)
            if not fobj:
                log.debug("Tar member had no stream: %s::%s", archive_path, mname)
                continue
            try:
                with fobj:
                    yield from _yield_from_filelike(
                        archive_path,
                        mname,
                        fobj,
                        hints=hints,
                        reader_kwargs_for=reader_kwargs_for,
                        reader_name_fallback=reader_name_fallback,
                    )
            except Exception:
                # keep iterating other members; detailed reader errors will be logged upstream
                raise
