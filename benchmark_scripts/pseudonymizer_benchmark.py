"""Full-scale pseudonymization benchmark: single-threaded vs. parallel,
same rigor and structure as run_ingestion_benchmark.py.

Runs the REAL, COMPLETE pseudonymization (all 8 tables, no time_window
restriction -- full dataset, not a sample) against the finished
single-threaded ingestion benchmark DB, once per mode (num_workers=1 and
num_workers=8), each writing to its own real, permanent destination
database. Per-table timing is appended to that mode's results file as each
table finishes, so an interrupted run leaves usable partial results and can
be resumed without redoing already-completed tables.

This is the one canonical pseudonymization benchmark script -- it replaces
the earlier pseudonymizer_profile.py (bounded sample, used to find the
fetch/transform/flush bottleneck) and pseudonymizer_parallel_verify.py
(bounded speedup check); both were exploratory steps whose findings are
already folded into this design and documented in
pseudonymizer_benchamrk.md.

Safe to interrupt and re-run:
  - Already-completed tables (recorded in that mode's results file) are
    skipped.
  - A table that was only partially copied when interrupted gets its
    destination table truncated before being re-copied from scratch (there's
    no row-level resume within a table -- see Pseudonymizer._copy_table).
  - The hashing pepper is persisted per mode and reused on resume, so
    already-flushed rows and newly-processed rows hash consistently.

Usage (run inside a `screen` session -- this covers the full dataset,
expect hours, not minutes):
    cd /cta/users/anajafi/SMDT/benchmark_scripts
    ../.venv/bin/python pseudonymizer_benchmark.py --num-workers 1
    ../.venv/bin/python pseudonymizer_benchmark.py --num-workers 8

Requires DB_USER / DB_PASSWORD (and optionally DB_HOST / DB_PORT) in the
environment, same as any other StandardDB usage.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

from config import (
    LOGS_DIR,
    PSEUDO_TABLES,
    db_name,
    mode_name,
    pseudo_dst_db_name,
    pseudo_log_file,
    pseudo_pepper_file,
    pseudo_results_file,
)

from smdt.config import DBConfig
from smdt.pseudonymizer.pseudonymizer import Pseudonymizer, PseudonymizeConfig

log = logging.getLogger("pseudonymizer_benchmark")

SRC_DB = db_name(1)  # single-threaded final ingestion benchmark DB -- the
                      # clean one, not parallel8 (which has extra duplicate
                      # posts from its file-scoped dedup)
CHUNK_ROWS = 50_000
TRANSFORM_CHUNK_SIZE = 1_000


def setup_logging(log_path: Path) -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    # force=True: smdt.pseudonymizer.pseudonymizer sets up its own root
    # logger (stdout-only) at import time -- without force=True here, our
    # FileHandler never actually attaches.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        force=True,
        handlers=[logging.FileHandler(log_path), logging.StreamHandler(sys.stdout)],
    )


def already_done_tables(results_path: Path) -> set[str]:
    done = set()
    if results_path.exists():
        with results_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                r = json.loads(line)
                if r.get("status") == "done":
                    done.add(r["table"])
    return done


def append_result(results_path: Path, **fields) -> None:
    with results_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps({"timestamp": time.time(), **fields}, default=str) + "\n")


def get_or_create_pepper(pepper_path: Path) -> bytes:
    if pepper_path.exists():
        return pepper_path.read_bytes()
    import os

    pepper = os.urandom(32)
    pepper_path.write_bytes(pepper)
    return pepper


def truncate_table(dst: "Pseudonymizer", table: str) -> None:
    """Clear out any partial data from an interrupted prior attempt at this
    table before re-copying it from scratch. No-op cost on an empty table."""
    conn = dst.dst.connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {table}")
        conn.commit()
    finally:
        conn.close()


def collect_dst_metrics(dst_db_name: str) -> dict:
    cfg = DBConfig()
    import psycopg

    conn = psycopg.connect(
        dbname=dst_db_name, user=cfg.user, password=cfg.password, host=cfg.host, port=cfg.port
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_database_size(current_database())")
            db_size_bytes = cur.fetchone()[0]
            row_counts = {}
            for t in PSEUDO_TABLES:
                cur.execute(f"SELECT count(*) FROM {t}")  # noqa: S608 - fixed internal table list
                row_counts[t] = cur.fetchone()[0]
    finally:
        conn.close()
    return {"db_size_bytes": db_size_bytes, "row_counts": row_counts}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--num-workers", type=int, default=1,
        help="1 = single-threaded baseline (default). >1 = parallel transform.",
    )
    args = parser.parse_args()
    num_workers = args.num_workers

    dst_db = pseudo_dst_db_name(num_workers)
    results_path = pseudo_results_file(num_workers)
    log_path = pseudo_log_file(num_workers)
    pepper_path = pseudo_pepper_file(num_workers)

    setup_logging(log_path)
    log.info(
        "Pseudonymization benchmark starting. mode=%s src=%s dst=%s num_workers=%d tables=%s",
        mode_name(num_workers), SRC_DB, dst_db, num_workers, PSEUDO_TABLES,
    )

    done = already_done_tables(results_path)
    if done:
        log.info("Already-completed tables from a prior run: %s", sorted(done))

    pepper = get_or_create_pepper(pepper_path)

    cfg = PseudonymizeConfig(
        src_db_name=SRC_DB,
        dst_db_name=dst_db,
        pepper=pepper,
        chunk_rows=CHUNK_ROWS,
        transform_chunk_size=TRANSFORM_CHUNK_SIZE,
        ask_reinit=False,
        num_workers=num_workers,
        time_window=None,  # full, complete dataset -- no scoping
    )
    p = Pseudonymizer(cfg)
    # _ensure_prepared(), not prepare_destination(): the latter unconditionally
    # re-applies the schema, but TimescaleDB's add_dimension() isn't
    # idempotent -- it errors if the destination was already initialized by
    # an earlier (interrupted) attempt. _ensure_prepared() checks first and
    # only applies schema to a genuinely fresh DB (same as Pseudonymizer.run()
    # does internally). Found by the smoke test's resume case, not by luck.
    p._ensure_prepared()

    for table in PSEUDO_TABLES:
        if table in done:
            log.info("Skipping %s (already completed)", table)
            continue

        log.info("=== %s ===", table)
        truncate_table(p, table)  # safe no-op if this table never started

        t0 = time.perf_counter()
        try:
            n = p._copy_table(table)
        except Exception:
            log.exception("Table %s failed -- not marking done, safe to resume", table)
            raise
        elapsed = time.perf_counter() - t0
        rps = n / elapsed if elapsed > 0 else 0.0

        log.info("Finished %s: %d rows in %.1fs (%.1f rows/s)", table, n, elapsed, rps)
        append_result(
            results_path, table=table, num_workers=num_workers, status="done",
            rows=n, elapsed_s=elapsed, rows_per_sec=rps,
        )

    metrics = collect_dst_metrics(dst_db)
    log.info(
        "Pseudonymization benchmark complete. DB size=%.1f MB. Row counts=%s",
        metrics["db_size_bytes"] / 1e6, metrics["row_counts"],
    )
    append_result(results_path, event="summary", num_workers=num_workers, **metrics)


if __name__ == "__main__":
    main()
