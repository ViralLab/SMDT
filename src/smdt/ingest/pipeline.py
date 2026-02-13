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

import tqdm

from smdt.io.readers import read
from smdt.io.archive_stream import stream_archive_records
from smdt.ingest.dedup import dedup_best
from smdt.ingest.plan import Plan
from smdt.standardizers.base import SourceInfo, Standardizer
from smdt.store.models import (
    Communities,
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
    Communities: ("community_id", "id", "created_at", "community_type"),
    Accounts: ("account_id", "created_at"),
    Posts: ("post_id", "created_at"),
    Entities: ("post_id", "body", "created_at", "retrieved_at"),
    Actions: (
        "originator_account_id",
        "originator_post_id",
        "target_account_id",
        "target_post_id",
        "target_community_id",
        "target_community_id",
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
        checkpoint_file: Optional file path for checkpointing completed files.
        reset_checkpoint: If True, reset the checkpoint file at the start of the run.
    """

    batch_size: int = 1_000  # records -> standardizer batch size
    chunk_size: int = 100_000  # values fallback chunk size for DB
    reader_kwargs: Dict[str, Dict[str, Any]] | None = None
    on_conflict: Dict[Type, str] | None = None
    progress: ProgressCallback | None = None
    checkpoint_file: str | None = None
    reset_checkpoint: bool | None = False


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
        """Flush a buffer of models to the database.

        Args:
            model_cls: The class of the models to flush.
            items: List of model instances to insert.
        """
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
        """Add models to local buffers and trigger flush if full.

        Args:
            models: Iterable of models to add.
        """
        for model in models:
            cls = type(model)
            buffers[cls].append(model)
            counters[f"models_{cls.__name__}"] += 1
            counters["models"] += 1
            if len(buffers[cls]) >= batch_size:
                _flush_buffer_worker(cls, buffers[cls])

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

    Args:
        plan: Ingestion plan.
        db: Database handler.
        standardizer: Standardizer instance.
        config: Pipeline configuration.
        hints: Optional hints dictionary.
    """
    cfg = config or PipelineConfig()
    on_conflict = dict(cfg.on_conflict or {})

    # Checkpoint Handling
    completed_files: set[str] = set()
    checkpoint_file_path: Path | None = None

    if cfg.checkpoint_file:
        checkpoint_file_path = Path(cfg.checkpoint_file)

        if cfg.reset_checkpoint and checkpoint_file_path.exists():
            log.info("Resetting checkpoint file: %s", checkpoint_file_path)
            # Create a backup just in case
            if checkpoint_file_path.stat().st_size > 0:
                backup = checkpoint_file_path.with_suffix(".bak")
                import shutil

                shutil.copy2(checkpoint_file_path, backup)
                log.info("Backed up old checkpoint to %s", backup)

            # Now wipe the file
            open(checkpoint_file_path, "w").close()

        if checkpoint_file_path.exists():
            with checkpoint_file_path.open("r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        completed_files.add(line.strip())
            log.info(
                "Loaded %d completed files from checkpoint %s",
                len(completed_files),
                checkpoint_file_path,
            )

    files_to_process = [fp for fp in plan.files if str(fp.path) not in completed_files]

    if len(files_to_process) < len(plan.files):
        log.info(
            "Skipping %d files already in checkpoint. Remaining: %d",
            len(plan.files) - len(files_to_process),
            len(files_to_process),
        )

    def _mark_file_completed(path: str) -> None:
        """Mark a file as completed in the checkpoint file.

        Args:
            path: Path of the completed file.
        """
        if checkpoint_file_path:
            with checkpoint_file_path.open("a", encoding="utf-8") as f:
                f.write(f"{path}\n")

    buffers: DefaultDict[Type, List[DBModel]] = defaultdict(list)
    counters: Counter[str] = Counter()
    failures_by_class: Counter[str] = Counter()
    t0_all = perf_counter()

    # ---------------- helpers ----------------

    def _notify(event: str, **info: Any) -> None:
        """Call the progress callback if configured.

        Args:
            event: Name of the event.
            info: Additional information about the event.
        """
        if cfg.progress:
            cfg.progress(event, info)

    def _normalize_ext(path: str) -> str:
        """Normalize file extension.

        Args:
            path: File path.

        Returns:
            Normalized extension string.
        """
        suffixes = [s.lower() for s in Path(path).suffixes]
        while suffixes and suffixes[-1] in COMPRESSED_SUFFIXES:
            suffixes.pop()
        return suffixes[-1].lstrip(".") if suffixes else ""

    def _reader_kwargs_for(path: str, reader_name: Optional[str]) -> Dict[str, Any]:
        """Get reader keyword arguments for a file.

        Args:
            path: File path.
            reader_name: Optional reader name.

        Returns:
            Dictionary of reader keyword arguments.
        """
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
        """Flush all model buffers."""
        for cls_, items in list(buffers.items()):
            _flush_buffer(cls_, items)

    # ---------------- record iteration (files & archives) ----------------

    def _iter_file_records(fp):
        """Iterate over records in a file (handling archives).

        Args:
            fp: File plan object.

        Yields:
            Tuple of (record, source_info).
        """
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
    try:
        for fp in tqdm.tqdm(files_to_process, desc="Pipeline files", colour="red"):
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
                    desc="standardize",
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
                            "Error in standardize: %s", e, exc_info=True
                        )
                    file_records += 1

                # per-file counters aggregation (common to both branches)
                counters["files"] += 1
                counters["records"] += file_records
                counters["models"] += file_models

                # If we finished the file loop without a top-level crash, mark it
                _mark_file_completed(fp.path)

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
                file_row_failures = counters.get("row_failures", 0) - prev_row_failures

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
