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
from smdt.ingest.plan import Plan
from smdt.ingest.dedup import dedup_best
from smdt.standardizers.base import Standardizer, SourceInfo
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
    Accounts: ("account_id", "created_at", "retrieved_at"),
    Posts: ("post_id", "created_at"),
    Entities: ("post_id", "body", "created_at", "retrieved_at"),
    Actions: (
        "originator_account_id",
        "originator_post_id",
        "target_account_id",
        "target_post_id",
        "action_type",
        "created_at",
        "retrieved_at",
    ),
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
    batch_size: int = 2000
    chunk_size: int = 5000
    reader_kwargs: Dict[str, Dict[str, Any]] | None = None
    on_conflict: Dict[Type, str] | None = None
    progress: ProgressCallback | None = None
    flush_per_file: bool = True


def run_pipeline(
    plan: Plan,
    db: StandardDB,
    standardizer: Standardizer,
    *,
    config: PipelineConfig | None = None,
    hints: Dict[str, Any] | None = None,
) -> None:
    cfg = config or PipelineConfig()
    on_conflict = dict(cfg.on_conflict or {})

    buffers: DefaultDict[Type, List[DBModel]] = defaultdict(list)
    counters = Counter()
    failures_by_class = Counter()
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

    def _preview_model(model: Any, max_len: int = 300) -> str:
        try:
            cols = getattr(model, "insert_columns")(False)
            vals = getattr(model, "insert_values")(False)
            parts = []
            for c, v in zip(cols, vals):
                s = repr(v)
                if len(s) > 80:
                    s = s[:77] + "..."
                parts.append(f"{c}={s}")
            preview = ", ".join(parts)
            return (
                preview if len(preview) <= max_len else (preview[: max_len - 3] + "...")
            )
        except Exception as e:
            return f"<unpreviewable payload: {e!r}>"

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
                chunk_size=cfg.chunk_size,  # used by multi-values fallback
            )
            elapsed = perf_counter() - t0
            _notify(
                "flush", model=model_cls.__name__, count=len(items), elapsed=elapsed
            )
            log.info(
                "Flushed %d %s rows in %.2fs", len(items), model_cls.__name__, elapsed
            )

        except Exception as e:
            # If insert_with_fallbacks bubbled up, treat as a hard failure for this batch.
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

    def _iter_file_records(fp) -> Iterable[Tuple[Mapping[str, Any], SourceInfo]]:
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

    for fp in tqdm.tqdm(plan.files, desc="Pipeline files", colour="red"):
        t0_file = perf_counter()
        _notify("file_start", path=fp.path)
        log.info("Processing file %s", fp.path)

        file_records = 0
        file_models = 0
        prev_record_errors = counters.get("record_errors", 0)
        prev_row_failures = counters.get("row_failures", 0)

        rec_iter = _iter_file_records(fp)
        pbar = tqdm.tqdm(rec_iter)

        for rec, src in pbar:
            file_records += 1
            try:
                for model in standardizer.standardize(rec, src):
                    cls = type(model)
                    buffers[cls].append(model)
                    file_models += 1
                    counters[f"models_{cls.__name__}"] += 1
                    if len(buffers[cls]) >= cfg.batch_size:
                        _flush_buffer(cls, buffers[cls])
            except Exception as e:
                counters["record_errors"] += 1
                log.warning(
                    "Error standardizing record in %s%s: %s",
                    src.path,
                    f"::{src.member}" if src.member else "",
                    e,
                    exc_info=True,
                )
            pbar.set_postfix(
                {"Buffer sizes": {k.__name__: len(v) for k, v in buffers.items()}}
            )

        counters["files"] += 1
        counters["records"] += file_records
        counters["models"] += file_models

        file_record_errors = counters.get("record_errors", 0) - prev_record_errors
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

        if cfg.flush_per_file:
            _flush_all_buffers()

    # Final flush and summary
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
