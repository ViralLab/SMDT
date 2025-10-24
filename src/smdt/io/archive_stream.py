from __future__ import annotations

import logging
import tarfile
import zipfile
from typing import Any, Callable, Dict, Iterable, Iterator, Mapping, Tuple

from smdt.io.readers import get_reader
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
    Stream records from a file-like object using the best available reader.

    Uses `reader.stream_from_filelike(f)` when available; otherwise falls back to
    `reader.stream(member_name, **kwargs)` (which may re-open by path/name).
    """
    sub_reader = get_reader(member_name)
    if not sub_reader:
        log.debug("No reader for member: %s::%s", archive_path, member_name)
        return

    src = SourceInfo(path=archive_path, member=member_name, hints=hints)
    # Prefer the sub-reader's self-reported name if present
    sub_reader_name = getattr(sub_reader, "name", None) or reader_name_fallback
    rk = reader_kwargs_for(member_name, sub_reader_name)

    stream_from_filelike = getattr(sub_reader, "stream_from_filelike", None)
    if callable(stream_from_filelike):
        for rec in stream_from_filelike(file_like, **rk):
            yield rec, src
    else:
        # Fallback: some readers only accept a path/name
        for rec in sub_reader.stream(member_name, **rk):
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
        Iterable of plan members where each `m` has attributes:
        - m.name : str
        - m.included : bool
        - m.reader_name : Optional[str]
    hints : Optional[Dict[str, Any]]
        Extra metadata propagated into SourceInfo.
    reader_kwargs_for : Callable[[member_path, reader_name], dict]
        Function that resolves reader kwargs by extension/reader name.
    reader_name_fallback : Optional[str]
        Reader name from the parent file plan, used if sub-reader has no name.

    Yields
    ------
    Iterator[Tuple[Mapping[str, Any], SourceInfo]]
    """
    lower = archive_path.lower()
    if lower.endswith(".zip"):
        with zipfile.ZipFile(archive_path, "r") as zf:
            for m in members:
                if not (
                    getattr(m, "included", False) and getattr(m, "reader_name", None)
                ):
                    continue
                try:
                    with zf.open(m.name, "r") as fobj:
                        yield from _yield_from_filelike(
                            archive_path,
                            m.name,
                            fobj,
                            hints=hints,
                            reader_kwargs_for=reader_kwargs_for,
                            reader_name_fallback=reader_name_fallback,
                        )
                except KeyError:
                    log.debug("Zip member missing: %s::%s", archive_path, m.name)
        return

    # tar / tgz / tbz2 / txz
    with tarfile.open(archive_path, "r:*") as tf:
        for m in members:
            if not (getattr(m, "included", False) and getattr(m, "reader_name", None)):
                continue
            ti = tf.getmember(m.name)
            fobj = tf.extractfile(ti)
            if not fobj:
                log.debug("Tar member had no stream: %s::%s", archive_path, m.name)
                continue
            try:
                yield from _yield_from_filelike(
                    archive_path,
                    m.name,
                    fobj,
                    hints=hints,
                    reader_kwargs_for=reader_kwargs_for,
                    reader_name_fallback=reader_name_fallback,
                )
            finally:
                try:
                    fobj.close()
                except Exception:
                    pass
