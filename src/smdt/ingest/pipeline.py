from __future__ import annotations

import logging
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import (
    Any,
    Callable,
    DefaultDict,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Tuple,
    Type,
)

import multiprocessing as mp
import tqdm

from smdt.io.readers import read
from smdt.io.archive_stream import stream_archive_records
from smdt.ingest.dedup import dedup_best
from smdt.ingest.plan import Plan
from smdt.standardizers.base import SourceInfo, Standardizer
from smdt.store.models import (
    AccountEnrichments,
    Accounts,
    Actions,
    Entities,
    PostEnrichments,
    Posts,
)
from smdt.store.standard_db import StandardDB

log = logging.getLogger(__name__)

DBModel = Any
ProgressCallback = Callable[[str, Mapping[str, Any]], None]

UNIQUE_KEYS: Dict[Type, Tuple[str, ...]] = {
    # --- THIS LINE IS THE FIX ---
    # It now matches your database's UNIQUE index: (account_id, created_at)
    Accounts: ("account_id", "created_at"),
    # --- THESE KEYS ARE CORRECT ---
    # This is a good logical key for deduplication within a batch.
    Posts: ("post_id", "created_at"),
    # This is a logical key (not DB-enforced) and is correct for dedup_best.
    Entities: ("post_id", "body", "created_at", "retrieved_at"),
    # This is a logical key (not DB-enforced) and is correct for dedup_best.
    Actions: (
        "originator_account_id",
        "originator_post_id",
        "target_account_id",
        "target_post_id",
        "action_type",
        "created_at",
        "retrieved_at",
    ),
    # These two keys perfectly match your database's UNIQUE constraints.
    PostEnrichments: ("post_id", "model_id"),
    AccountEnrichments: ("account_id", "model_id"),
}

COMPRESSED_SUFFIXES = {".gz", ".bz2", ".xz", ".zst"}
DEFAULT_READER_KW: Dict[str, Dict[str, Any]] = {
    "tsv": {"sep": "\t"},
    "tab": {"sep": "\t"},
}


@dataclass
class PipelineConfig:
    """Configuration toggles for the pipeline.

    Attributes:
        batch_size: Number of records to process in a single standardizer batch.
        chunk_size: Number of values to insert into the DB in a single chunk.
        reader_kwargs: Dictionary of reader-specific keyword arguments.
        on_conflict: Dictionary mapping model types to on-conflict strategies.
        progress: Callback function for progress updates.
        do_sequential: If True, process files sequentially instead of in parallel.
        num_workers: Number of worker processes to use for parallel processing.
    """

    batch_size: int = 1_000  # records → standardizer batch size
    chunk_size: int = 100_000  # values fallback chunk size for DB
    reader_kwargs: Dict[str, Dict[str, Any]] | None = None
    on_conflict: Dict[Type, str] | None = None
    progress: ProgressCallback | None = None

    # if False, use multiprocessing over files
    do_sequential: bool = False
    num_workers: int = 2  # number of worker processes for multiprocessing


# ---------------------------------------------------------------------------
# Global helpers used by workers (must be top-level for pickling)
# ---------------------------------------------------------------------------


def _normalize_ext_global(path: str) -> str:
    """Normalize file extension for global usage.

    Args:
        path: File path.

    Returns:
        Normalized extension (lowercase, no dot), or empty string.
    """
    suffixes = [s.lower() for s in Path(path).suffixes]
    while suffixes and suffixes[-1] in COMPRESSED_SUFFIXES:
        suffixes.pop()
    return suffixes[-1].lstrip(".") if suffixes else ""


def _reader_kwargs_for_global(
    path: str,
    reader_name: Optional[str],
    reader_kwargs_cfg: Optional[Dict[str, Dict[str, Any]]],
) -> Dict[str, Any]:
    """Get reader keyword arguments for a file (global version).

    Args:
        path: File path.
        reader_name: Name of the reader.
        reader_kwargs_cfg: Configuration for reader kwargs.

    Returns:
        Dictionary of reader kwargs.
    """
    merged: Dict[str, Any] = {}
    ext = _normalize_ext_global(path)
    if ext in DEFAULT_READER_KW:
        merged.update(DEFAULT_READER_KW[ext])
    if reader_kwargs_cfg:
        if reader_name and reader_name in reader_kwargs_cfg:
            merged.update(reader_kwargs_cfg[reader_name])
        if ext and ext in reader_kwargs_cfg:
            merged.update(reader_kwargs_cfg[ext])
    return merged


def _iter_file_records_global(fp, hints, reader_kwargs_cfg):
    """Iterate (record, SourceInfo) for a plan file, aware of archives.

    Args:
        fp: FilePlan object.
        hints: Hints dictionary.
        reader_kwargs_cfg: Reader kwargs configuration.

    Yields:
        Tuple of (record, SourceInfo).
    """
    if not fp.is_archive:
        src = SourceInfo(path=fp.path, member=None, hints=hints)
        rk = _reader_kwargs_for_global(fp.path, fp.reader_name, reader_kwargs_cfg)
        for rec in read(fp.path, **rk):
            yield rec, src
        return

    # Archive case
    yield from stream_archive_records(
        archive_path=fp.path,
        members=fp.members,
        hints=hints,
        reader_kwargs_for=lambda path, reader_name: _reader_kwargs_for_global(
            path, reader_name, reader_kwargs_cfg
        ),
        reader_name_fallback=fp.reader_name,
    )


def _process_file_worker_with_db(
    args: Tuple[
        Any,  # fp
        Any,  # hints
        Optional[Dict[str, Dict[str, Any]]],  # reader_kwargs
        int,  # batch_size
        int,  # chunk_size
        Dict[Type, str],  # on_conflict
        StandardDB,  # parent db object (we'll call connect())
        Standardizer,  # standardizer
    ],
) -> Tuple[Mapping[str, int], float, str]:
    """Worker that processes a single file and writes directly to DB.

    Each worker calls db.connect() to get its own connection.

    Args:
        args: Tuple containing worker arguments.

    Returns:
        Tuple of (counters, elapsed_time, file_path).
    """
    (
        fp,
        hints,
        reader_kwargs_cfg,
        batch_size,
        chunk_size,
        on_conflict,
        parent_db,
        standardizer,
    ) = args

    # Each process opens its own connection
    db: StandardDB = parent_db

    t0_file = perf_counter()
    buffers: DefaultDict[Type, List[DBModel]] = defaultdict(list)
    counters = Counter()

    def _flush_buffer_worker(model_cls: Type, items: List[DBModel]) -> None:
        if not items:
            return

        key_fields = UNIQUE_KEYS.get(model_cls)
        if key_fields:
            try:
                items[:] = dedup_best(items, key_fields=key_fields)
            except Exception as e:
                log.error(
                    "Dedup failed in worker for %s (key=%s): %s",
                    model_cls.__name__,
                    key_fields,
                    e,
                    exc_info=True,
                )

        try:
            db.insert_with_fallbacks(
                items,
                include_id=False,
                on_conflict=on_conflict.get(model_cls),
                chunk_size=chunk_size,
            )
            counters[f"flush_{model_cls.__name__}"] += len(items)
        except Exception as e:
            counters["failed_models"] += len(items)
            log.error(
                "[worker %s] Flush failed for %s with %d items: %s",
                fp.path,
                model_cls.__name__,
                len(items),
                e,
                exc_info=True,
            )
        finally:
            items.clear()

    def _add_models_to_buffers_local(models: Iterable[DBModel]) -> None:
        for model in models:
            cls = type(model)
            buffers[cls].append(model)
            counters[f"models_{cls.__name__}"] += 1
            counters["models"] += 1
            if len(buffers[cls]) >= batch_size:
                _flush_buffer_worker(cls, buffers[cls])
            # print(len(buffers[cls]))

    try:
        rec_iter = _iter_file_records_global(fp, hints, reader_kwargs_cfg)

        # Nice per-file progress bar (records in this file)
        from pathlib import Path

        file_name = Path(fp.path).name

        for record, src in tqdm.tqdm(
            rec_iter,
            desc=f"{file_name} (records)",
            leave=False,
        ):
            try:
                sub_result = standardizer.standardize((record, src))
                if sub_result is not None:
                    _add_models_to_buffers_local(sub_result)
                else:
                    counters["empty_standardize"] += 1
                counters["records"] += 1
            except Exception:
                counters["record_errors"] += 1
                log.warning("[worker %s] Error in standardize", fp.path, exc_info=True)

        counters["files"] += 1
    except Exception:
        counters["record_errors"] += 1
        log.error(
            "[worker %s] Unhandled error while processing file",
            fp.path,
            exc_info=True,
        )

    # Final flush
    for cls_, items in list(buffers.items()):
        _flush_buffer_worker(cls_, items)

    return counters, perf_counter() - t0_file, fp.path


# ---------------------------------------------------------------------------
# Main API
# ---------------------------------------------------------------------------


def run_pipeline(
    plan: Plan,
    db: StandardDB,
    standardizer: Standardizer,
    *,
    config: PipelineConfig | None = None,
    hints: Dict[str, Any] | None = None,
) -> None:
    """Run the pipeline over files in `plan`.

    Standardizes records via `standardizer`, and inserts models into `db`
    with fallbacks and on-conflict policies.

    If `config.do_sequential` is False, use multiprocessing at the *file* level.

    Args:
        plan: Ingestion plan.
        db: Database handler.
        standardizer: Standardizer instance.
        config: Pipeline configuration.
        hints: Optional hints dictionary.
    """
    cfg = config or PipelineConfig()
    on_conflict = dict(cfg.on_conflict or {})

    buffers: DefaultDict[Type, List[DBModel]] = defaultdict(list)
    counters: Counter[str] = Counter()
    failures_by_class: Counter[str] = Counter()
    t0_all = perf_counter()

    # ---------------- helpers ----------------

    def _notify(event: str, **info: Any) -> None:
        if cfg.progress:
            cfg.progress(event, info)

    def _normalize_ext(path: str) -> str:
        suffixes = [s.lower() for s in Path(path).suffixes]
        while suffixes and suffixes[-1] in COMPRESSED_SUFFIXES:
            suffixes.pop()
        return suffixes[-1].lstrip(".") if suffixes else ""

    def _reader_kwargs_for(path: str, reader_name: Optional[str]) -> Dict[str, Any]:
        merged: Dict[str, Any] = {}
        ext = _normalize_ext(path)
        if ext in DEFAULT_READER_KW:
            merged.update(DEFAULT_READER_KW[ext])
        if cfg.reader_kwargs:
            if reader_name and reader_name in cfg.reader_kwargs:
                merged.update(cfg.reader_kwargs[reader_name])
            if ext and ext in cfg.reader_kwargs:
                merged.update(cfg.reader_kwargs[ext])
        return merged

    def _add_models_to_buffers(models: Iterable[DBModel]) -> None:
        """Accumulate models to per-class buffers and bump counters."""
        nonlocal counters, file_models
        for model in models:
            cls = type(model)
            buffers[cls].append(model)
            file_models += 1
            counters[f"models_{cls.__name__}"] += 1

    def _flush_buffer(model_cls: Type, items: List[DBModel]) -> None:
        """Flush a single model buffer using COPY→multi-values→row-by-row strategy."""
        if not items:
            return

        # Dedup inside the buffer (best-effort)
        key_fields = UNIQUE_KEYS.get(model_cls)
        if key_fields:
            try:
                items[:] = dedup_best(items, key_fields=key_fields)
            except Exception as e:
                log.error(
                    "Dedup failed for %s (key=%s): %s",
                    model_cls.__name__,
                    key_fields,
                    e,
                    exc_info=True,
                )

        # Try DB insertion with built-in fallbacks
        t0 = perf_counter()
        try:
            db.insert_with_fallbacks(
                items,
                include_id=False,
                on_conflict=on_conflict.get(model_cls),
                chunk_size=cfg.chunk_size,
            )
            elapsed = perf_counter() - t0
            _notify(
                "flush", model=model_cls.__name__, count=len(items), elapsed=elapsed
            )
            log.info(
                "Flushed %d %s rows in %.2fs", len(items), model_cls.__name__, elapsed
            )
        except Exception as e:
            counters["failed_models"] += len(items)
            failures_by_class[model_cls.__name__] += len(items)
            _notify(
                "flush_error",
                model=model_cls.__name__,
                count=len(items),
                error=str(e),
                row_failures=counters.get("row_failures", 0),
                failed_models_total=counters.get("failed_models", 0),
                failed_models_by_class=dict(failures_by_class),
            )
            log.error(
                "Flush failed fatally for %s with %d items: %s",
                model_cls.__name__,
                len(items),
                e,
                exc_info=True,
            )
        finally:
            items.clear()

    def _flush_all_buffers() -> None:
        for cls_, items in list(buffers.items()):
            _flush_buffer(cls_, items)

    # ---------------- record iteration (files & archives) ----------------

    def _iter_file_records(fp):
        if not fp.is_archive:
            src = SourceInfo(path=fp.path, member=None, hints=hints)
            rk = _reader_kwargs_for(fp.path, fp.reader_name)
            for rec in read(fp.path, **rk):
                yield rec, src
            return

        # Delegate archive reading to helper module (respects member order & filters)
        yield from stream_archive_records(
            archive_path=fp.path,
            members=fp.members,
            hints=hints,
            reader_kwargs_for=_reader_kwargs_for,
            reader_name_fallback=fp.reader_name,
        )

    # ------------------------------- main loop -------------------------------
    if cfg.do_sequential == True:
        try:
            for fp in tqdm.tqdm(plan.files, desc="Pipeline files", colour="red"):
                t0_file = perf_counter()
                _notify("file_start", path=fp.path)
                log.info("Processing file %s", fp.path)

                file_records = 0
                file_models = 0
                prev_record_errors = counters.get("record_errors", 0)
                prev_row_failures = counters.get("row_failures", 0)

                rec_iter: Iterable[Tuple[Any, SourceInfo]] = _iter_file_records(fp)

                try:
                    for record, src in tqdm.tqdm(
                        rec_iter,
                        desc="standardize(sequential)",
                        leave=False,
                        colour="blue",
                    ):
                        try:
                            sub_result = standardizer.standardize((record, src))
                            if sub_result is not None:
                                _add_models_to_buffers(sub_result)
                            else:
                                # You had this debug print originally
                                print(record, src)
                                raise ValueError("No models returned from standardizer")

                            # Flush overgrown class buffers
                            for cls in list(buffers.keys()):
                                if len(buffers[cls]) >= cfg.batch_size:
                                    _flush_buffer(cls, buffers[cls])
                        except Exception as e:
                            counters["record_errors"] += 1
                            log.warning(
                                "Error in sequential standardize: %s", e, exc_info=True
                            )
                        file_records += 1

                    # per-file counters aggregation (common to both branches)
                    counters["files"] += 1
                    counters["records"] += file_records
                    counters["models"] += file_models

                except Exception as e:
                    # Catch any unhandled per-file error so we still emit file_end
                    counters["record_errors"] += 1
                    log.error(
                        "Unhandled error while processing %s: %s",
                        fp.path,
                        e,
                        exc_info=True,
                    )

                finally:
                    # Always emit file_end and optional per-file flush
                    file_record_errors = (
                        counters.get("record_errors", 0) - prev_record_errors
                    )
                    file_row_failures = (
                        counters.get("row_failures", 0) - prev_row_failures
                    )

                    _notify(
                        "file_end",
                        path=fp.path,
                        records=file_records,
                        models=file_models,
                        record_errors=file_record_errors,
                        row_failures=file_row_failures,
                        elapsed=perf_counter() - t0_file,
                    )
                    log.info(
                        "Finished %s with %d records, %d models (record_errors=%d, row_failures=%d)",
                        fp.path,
                        file_records,
                        file_models,
                        file_record_errors,
                        file_row_failures,
                    )

            # end for each file

        finally:
            # Final flush and summary (now truly for the whole run)
            _flush_all_buffers()
            _notify(
                "done",
                files=counters["files"],
                records=counters["records"],
                models=counters["models"],
                record_errors=counters.get("record_errors", 0),
                row_failures=counters.get("row_failures", 0),
                failed_models_total=counters.get("failed_models", 0),
                failed_models_by_class=dict(failures_by_class),
                elapsed=perf_counter() - t0_all,
            )
            log.info(
                "Pipeline finished: %d files, %d records, %d models in %.2fs "
                "(record_errors=%d, row_failures=%d, failed_models_total=%d, failed_by_class=%s)",
                counters["files"],
                counters["records"],
                counters["models"],
                perf_counter() - t0_all,
                counters.get("record_errors", 0),
                counters.get("row_failures", 0),
                counters.get("failed_models", 0),
                dict(failures_by_class),
            )

    else:
        ctx = mp.get_context("fork")  # use "spawn" on macOS/Windows if needed

        worker_args = [
            (
                fp,
                hints,
                cfg.reader_kwargs,
                cfg.batch_size,
                cfg.chunk_size,
                on_conflict,
                db,  # pass the parent db (for db.connect())
                standardizer,
            )
            for fp in plan.files
        ]

        try:
            with ctx.Pool(processes=cfg.num_workers) as pool:
                for counters_delta, elapsed_file, path in tqdm.tqdm(
                    pool.imap_unordered(_process_file_worker_with_db, worker_args),
                    total=len(worker_args),
                    desc="Pipeline files (mp)",
                    colour="red",
                ):
                    for k, v in counters_delta.items():
                        counters[k] += v

                    _notify(
                        "file_end",
                        path=path,
                        records=counters_delta.get("records", 0),
                        models=counters_delta.get("models", 0),
                        record_errors=counters_delta.get("record_errors", 0),
                        elapsed=elapsed_file,
                    )

        finally:
            _notify(
                "done",
                files=counters["files"],
                records=counters["records"],
                models=counters["models"],
                record_errors=counters.get("record_errors", 0),
                elapsed=perf_counter() - t0_all,
            )
