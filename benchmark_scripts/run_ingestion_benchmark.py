"""Cumulative ingestion-scalability benchmark for the paper.

Feeds the pre-sliced Election2023 Twitter dataset (see prepare_slices.py)
into run_pipeline one checkpoint at a time (1K, 10K, 100K, 1M, 10M
cumulative records), against one continuously-growing database, and
records ingestion throughput, DB size, per-table row counts, TimescaleDB
chunk counts, and process memory at each checkpoint.

This is a single, resumable, cumulative run -- NOT independent runs per
scale point -- so the numbers reflect how the system behaves as the
database actually grows, not a series of clean-slate measurements.

Runs in one of two modes, picked with --num-workers (default 1):
  --num-workers 1  (default) single-threaded, today's baseline behavior.
  --num-workers N  (N > 1)   file-level parallel ingestion (see the
                              num_workers feature added to PipelineConfig).
Each mode gets its own database, checkpoint file, results file, and log
(see config.py's db_name()/checkpoint_file()/etc.) so the two are never
mixed and can be run and compared independently.

Safe to interrupt (e.g. screen session killed) and re-run:
  - Already-completed checkpoints (recorded in that mode's results file)
    are skipped.
  - The pipeline's own file-level checkpoint prevents re-processing an
    already-ingested slice file.
  - That mode's db_initialized.marker ensures the destructive
    `initialize=True` path is only ever used once, on the very first
    checkpoint of the very first run for that mode -- never on a resumed
    run, even if the process restarts.

Usage (run inside a `screen` session -- the 10M checkpoint can take hours):
    cd /cta/users/anajafi/SMDT/benchmark_scripts
    ../.venv/bin/python prepare_slices.py             # one-time prep, not timed
    ../.venv/bin/python run_ingestion_benchmark.py --num-workers 1
    ../.venv/bin/python run_ingestion_benchmark.py --num-workers 8

Requires DB_USER / DB_PASSWORD (and optionally DB_HOST / DB_PORT) in the
environment, same as any other StandardDB usage.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import resource
import sys
import threading
import time
from pathlib import Path

import psutil

from config import (
    BATCH_SIZE,
    CHUNK_SIZE,
    DATA_DIR,
    LOGS_DIR,
    MEMORY_SAMPLE_INTERVAL_SECONDS,
    MODEL_TABLES,
    ON_CONFLICT,
    CHECKPOINTS,
    checkpoint_file,
    db_init_marker,
    db_name,
    log_file,
    memory_samples_file,
    mode_name,
    part_glob,
    results_file,
)

from smdt.io.readers import discover
from smdt.ingest.plan import plan_directories
from smdt.ingest.pipeline import run_pipeline, PipelineConfig
from smdt.standardizers import TwitterV1Standardizer
from smdt.store.standard_db import StandardDB

log = logging.getLogger("benchmark")


def setup_logging(log_path: Path) -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    fh = logging.FileHandler(log_path)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    root.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
    root.addHandler(sh)


def already_done_checkpoints(results_path: Path) -> set[int]:
    done = set()
    if results_path.exists():
        with results_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    done.add(json.loads(line)["checkpoint"])
    return done


def append_result(results_path: Path, result: dict) -> None:
    with results_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(result) + "\n")


_this_process = psutil.Process()
_memory_write_lock = threading.Lock()


def total_rss_kb() -> int | None:
    """RSS in KB summed across this process AND all of its live child
    processes, recursively.

    A self-only /proc/self/status read (the previous implementation) is
    correct for num_workers=1, where the one benchmark process does all the
    standardizing/buffering/flushing itself. But num_workers>1 dispatches
    each file to a separate worker process via ProcessPoolExecutor (see
    pipeline.py's _run_parallel) -- the actual memory-heavy work happens
    inside those workers, each in its own address space, and the main
    process just dispatches futures and aggregates small per-file counters.
    A self-only reading therefore only measures an idle dispatcher and
    made parallel ingestion look like it used less memory, when the real
    total (main + all workers) was invisible to the measurement entirely."""
    try:
        total = _this_process.memory_info().rss
        for child in _this_process.children(recursive=True):
            try:
                total += child.memory_info().rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return total // 1024  # bytes -> KB
    except Exception:
        return None


def append_memory_sample(memory_path: Path, *, stage_peak: dict | None = None, **fields) -> None:
    rss_kb = total_rss_kb()
    if rss_kb is None:
        return
    with _memory_write_lock:
        if stage_peak is not None:
            stage_peak["value"] = max(stage_peak.get("value", 0), rss_kb)
        with memory_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps({"timestamp": time.time(), "rss_kb": rss_kb, **fields}) + "\n")


class PeriodicMemorySampler:
    """Samples total_rss_kb() on a wall-clock timer in a background thread,
    independent of pipeline progress events -- so num_workers=1 and >1 get
    directly comparable, continuous memory time series. Event-driven
    sampling alone (at flush/file_end) only fires in the main process when
    a worker's whole file completes, which is both too coarse and (before
    total_rss_kb()) blind to concurrent worker memory."""

    def __init__(self, memory_path: Path, checkpoint: int, t0: float, stage_peak: dict, interval: float):
        self._memory_path = memory_path
        self._checkpoint = checkpoint
        self._t0 = t0
        self._stage_peak = stage_peak
        self._interval = interval
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join()

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            append_memory_sample(
                self._memory_path,
                stage_peak=self._stage_peak,
                checkpoint=self._checkpoint,
                event="periodic",
                elapsed_since_stage_start=time.perf_counter() - self._t0,
            )


def collect_db_metrics(db: StandardDB) -> dict:
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_database_size(current_database())")
            db_size_bytes = cur.fetchone()[0]

            table_sizes, row_counts = {}, {}
            for t in MODEL_TABLES:
                cur.execute("SELECT pg_total_relation_size(%s)", (t,))
                table_sizes[t] = cur.fetchone()[0]
                cur.execute(f"SELECT count(*) FROM {t}")  # noqa: S608 - fixed internal table list
                row_counts[t] = cur.fetchone()[0]

            cur.execute(
                "SELECT hypertable_name, count(*) FROM timescaledb_information.chunks "
                "GROUP BY hypertable_name"
            )
            chunk_counts = {r[0]: r[1] for r in cur.fetchall()}
        conn.commit()
    finally:
        conn.close()
    return {
        "db_size_bytes": db_size_bytes,
        "table_sizes_bytes": table_sizes,
        "row_counts": row_counts,
        "chunk_counts": chunk_counts,
    }


def run_checkpoint(
    checkpoint: int,
    is_first_ever: bool,
    *,
    num_workers: int,
    db_name_: str,
    checkpoint_path: Path,
    marker_path: Path,
    memory_path: Path,
) -> dict:
    pattern = part_glob(checkpoint)

    log.info(
        "=== Checkpoint %s: starting (num_workers=%d, initialize=%s) ===",
        f"{checkpoint:,}", num_workers, is_first_ever,
    )

    discover()
    # IMPORTANT: prepare_slices.py creates every checkpoint's files up
    # front, so an unfiltered plan_directories([str(DATA_DIR)]) would see
    # every part file from the very first stage and run_pipeline would
    # ingest all of them in one call (the pipeline checkpoint file only
    # prevents *reprocessing* an already-completed file, it doesn't limit
    # which not-yet-completed files a single call consumes). Restrict
    # `include` to exactly this checkpoint's files so each call advances by
    # exactly one checkpoint's worth of records.
    plan = plan_directories([str(DATA_DIR)], include=[pattern])
    log.info(
        "Plan matched %d file(s) for pattern %s: %s",
        len(plan.files), pattern, [Path(fp.path).name for fp in plan.files],
    )

    db = StandardDB(db_name_, initialize=is_first_ever)
    if is_first_ever:
        marker_path.write_text(f"initialized at {time.time()}\n")

    flush_events: list[dict] = []
    file_end_events: list[dict] = []
    done_info: dict = {}
    t0 = time.perf_counter()
    stage_peak: dict = {}  # max total_rss_kb() seen this stage, across all sample sources

    def progress(event, info):
        if event == "flush":
            flush_events.append(dict(info))
            append_memory_sample(
                memory_path,
                stage_peak=stage_peak,
                checkpoint=checkpoint,
                event=event,
                elapsed_since_stage_start=time.perf_counter() - t0,
                model=info.get("model"),
                flush_count=info.get("count"),
            )
        elif event == "file_end":
            file_end_events.append(dict(info))
            append_memory_sample(
                memory_path,
                stage_peak=stage_peak,
                checkpoint=checkpoint,
                event=event,
                elapsed_since_stage_start=time.perf_counter() - t0,
                records_in_file=info.get("records"),
            )
        elif event == "flush_error":
            log.error("flush_error: %s", info)
        elif event == "done":
            done_info.update(info)

    load_before = os.getloadavg()
    append_memory_sample(
        memory_path, stage_peak=stage_peak, checkpoint=checkpoint,
        event="stage_start", elapsed_since_stage_start=0.0,
    )

    # Wall-clock periodic sampling runs alongside the event-driven samples
    # above -- it's what actually captures worker-process memory under
    # num_workers>1, where flush/file_end only fire once per whole file.
    sampler = PeriodicMemorySampler(
        memory_path, checkpoint, t0, stage_peak, MEMORY_SAMPLE_INTERVAL_SECONDS
    )
    sampler.start()
    try:
        run_pipeline(
            plan,
            db,
            TwitterV1Standardizer(),
            config=PipelineConfig(
                batch_size=BATCH_SIZE,
                chunk_size=CHUNK_SIZE,
                on_conflict=ON_CONFLICT,
                checkpoint_file=str(checkpoint_path),
                reset_checkpoint=is_first_ever,
                progress=progress,
                num_workers=num_workers,
            ),
        )
    finally:
        sampler.stop()

    elapsed = time.perf_counter() - t0
    load_after = os.getloadavg()
    # Self-only high-water mark since process start -- kept for continuity,
    # but under num_workers>1 this reflects only the (mostly idle) dispatcher
    # process, NOT worker memory. Use peak_total_rss_kb_this_stage instead.
    peak_rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    append_memory_sample(
        memory_path, stage_peak=stage_peak, checkpoint=checkpoint,
        event="stage_end", elapsed_since_stage_start=elapsed,
    )

    records_this_stage = done_info.get("records", 0)
    db_metrics = collect_db_metrics(db)

    result = {
        "checkpoint": checkpoint,
        "num_workers": num_workers,
        "timestamp": time.time(),
        "records_this_stage": records_this_stage,
        "elapsed_seconds": elapsed,
        "records_per_second_this_stage": (
            records_this_stage / elapsed if elapsed > 0 else None
        ),
        "load_avg_before": list(load_before),
        "load_avg_after": list(load_after),
        # High-water mark since this *process* started, not just this
        # stage -- meaningful because the whole benchmark runs as one
        # continuous process; see the driver's module docstring. Self-only:
        # under num_workers>1 this does NOT include worker-process memory.
        "peak_rss_kb_since_process_start": peak_rss_kb,
        # Correct total-memory peak for THIS stage: self + all live child
        # (worker) processes, from the periodic + event-driven samples
        # written to memory_path during this stage. Comparable across
        # num_workers values, unlike the self-only field above.
        "peak_total_rss_kb_this_stage": stage_peak.get("value"),
        "pipeline_done_counters": done_info,
        "flush_events": flush_events,
        "file_end_events": file_end_events,
        **db_metrics,
    }

    log.info(
        "Checkpoint %s done: %d records in %.1fs (%.1f rec/s). DB size=%.1f MB. Chunks=%s",
        f"{checkpoint:,}",
        records_this_stage,
        elapsed,
        result["records_per_second_this_stage"] or 0.0,
        db_metrics["db_size_bytes"] / 1e6,
        db_metrics["chunk_counts"],
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--num-workers", type=int, default=1,
        help="1 = single-threaded baseline (default). >1 = parallel ingestion.",
    )
    args = parser.parse_args()
    num_workers = args.num_workers

    db_name_ = db_name(num_workers)
    checkpoint_path = checkpoint_file(num_workers)
    marker_path = db_init_marker(num_workers)
    results_path = results_file(num_workers)
    memory_path = memory_samples_file(num_workers)
    log_path = log_file(num_workers)

    setup_logging(log_path)
    log.info(
        "Benchmark starting. mode=%s DB=%s num_workers=%d Checkpoints=%s",
        mode_name(num_workers), db_name_, num_workers, CHECKPOINTS,
    )

    done = already_done_checkpoints(results_path)
    if done:
        log.info("Already-completed checkpoints from a prior run: %s", sorted(done))

    remaining = [c for c in sorted(CHECKPOINTS) if c not in done]
    if not remaining:
        log.info("All checkpoints already completed. Nothing to do.")
        return

    for checkpoint in remaining:
        is_first_ever = not marker_path.exists()
        try:
            result = run_checkpoint(
                checkpoint,
                is_first_ever,
                num_workers=num_workers,
                db_name_=db_name_,
                checkpoint_path=checkpoint_path,
                marker_path=marker_path,
                memory_path=memory_path,
            )
        except Exception:
            log.exception("Checkpoint %s failed -- stopping.", f"{checkpoint:,}")
            raise
        append_result(results_path, result)

    log.info("Benchmark complete. Results: %s", results_path)


if __name__ == "__main__":
    main()
