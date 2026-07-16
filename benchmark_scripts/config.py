"""Shared configuration for the ingestion scalability benchmark.

See run_ingestion_benchmark.py for the actual driver.
"""
from pathlib import Path

from smdt.store.models import (
    Accounts,
    Posts,
    Entities,
    Actions,
    Communities,
    AccountEnrichments,
    PostEnrichments,
)

# ---- Source dataset (real, not synthetic -- see conversation/paper notes) ----
DATASET_DIR = "/cta/DATASTORE/Secim2023/TwitterTracking"
# Only the main daily files, e.g. stream_collection_2023-01-01.jsons.gz --
# excludes the *_turkishbertweet_*/*_wordfrequencies* derivative sidecar files.
DATASET_INCLUDE = ["*stream_collection_2023-0[12]-*.jsons.gz"]
DATASET_EXCLUDE = ["*turkishbertweet*", "*wordfrequencies*"]

# ---- Benchmark scale points (cumulative input records read from file) ----
CHECKPOINTS = [1_000, 10_000, 100_000, 1_000_000, 10_000_000]

# ---- Parallel ingestion comparison ----
# Each checkpoint's incremental delta is sliced into multiple files (see
# prepare_slices.py) capped at this many records each, so num_workers > 1
# actually has more than one file to distribute across workers. This is
# independent of num_workers itself -- the same sliced files are reused for
# both the single-threaded and parallel runs; only run_pipeline's
# PipelineConfig.num_workers differs between them.
FILE_CAP_RECORDS = 12_500
PARALLEL_NUM_WORKERS = 8

# ---- Paths ----
BENCHMARK_ROOT = Path(__file__).resolve().parent
DATA_DIR = BENCHMARK_ROOT / "data"
LOGS_DIR = BENCHMARK_ROOT / "logs"

# NOTE: deliberately NOT turkish_election2023 (that's real prior research
# data on this shared Postgres instance) -- this is a dedicated benchmark DB.
DB_NAME_BASE = "benchmark_v1_election_2023"


def mode_name(num_workers: int) -> str:
    """'single' for num_workers=1, else 'parallelN' -- used to keep the two
    comparison runs' DB/log/checkpoint files completely separate."""
    return "single" if num_workers <= 1 else f"parallel{num_workers}"


def db_name(num_workers: int) -> str:
    return f"{DB_NAME_BASE}_{mode_name(num_workers)}"


def checkpoint_file(num_workers: int) -> Path:
    return LOGS_DIR / f"pipeline_checkpoint_{mode_name(num_workers)}.txt"


def db_init_marker(num_workers: int) -> Path:
    return LOGS_DIR / f"db_initialized_{mode_name(num_workers)}.marker"


def results_file(num_workers: int) -> Path:
    return LOGS_DIR / f"results_{mode_name(num_workers)}.jsonl"


def log_file(num_workers: int) -> Path:
    return LOGS_DIR / f"benchmark_{mode_name(num_workers)}.log"


def memory_samples_file(num_workers: int) -> Path:
    return LOGS_DIR / f"memory_samples_{mode_name(num_workers)}.jsonl"


def query_results_file(num_workers: int) -> Path:
    return LOGS_DIR / f"query_benchmark_{mode_name(num_workers)}.jsonl"


# ---- Pseudonymization benchmark ----
# See pseudonymizer_benchmark.py. Full, complete run (no time_window) against
# each mode's real destination DB, mirroring the ingestion benchmark's
# single-vs-parallel comparison structure.
PSEUDO_TABLES = [
    "communities", "accounts", "posts", "entities", "actions",
    "post_enrichments", "account_enrichments", "dataset_meta",
]


def pseudo_dst_db_name(num_workers: int) -> str:
    return f"{DB_NAME_BASE}_pseudo_{mode_name(num_workers)}"


def pseudo_results_file(num_workers: int) -> Path:
    return LOGS_DIR / f"pseudonymizer_benchmark_{mode_name(num_workers)}.jsonl"


def pseudo_log_file(num_workers: int) -> Path:
    return LOGS_DIR / f"pseudonymizer_benchmark_{mode_name(num_workers)}.log"


def pseudo_pepper_file(num_workers: int) -> Path:
    """Persisted so a resumed run keeps hashing consistently with rows it
    already flushed -- a fresh random pepper on resume would make the same
    account_id/username hash differently across the same destination DB.
    Never commit this file (it's a secret salt, same handling as any other
    credential in this repo)."""
    return LOGS_DIR / f"pseudonymizer_pepper_{mode_name(num_workers)}.bin"


# ---- Memory sampling ----
# Wall-clock interval for the background total-RSS sampler in
# run_ingestion_benchmark.py -- independent of pipeline progress events so
# num_workers=1 and num_workers>1 get comparable, continuous time series
# (event-driven sampling alone only fires once per file completion under
# parallel dispatch, which is too coarse and main-process-only).
MEMORY_SAMPLE_INTERVAL_SECONDS = 2.0

# ---- Query-speed / indexing-strategy benchmark ----
# See query_benchmark.py. Runs against whatever data volume already exists in
# that mode's DB (by default the final state of the ingestion benchmark) --
# not a latency-vs-scale curve, since the ingestion benchmark is one
# continuously-growing DB without preserved per-checkpoint snapshots.
QUERY_REPEATS = 20  # timed repetitions per (query, sampled-param) pair -- warm-cache
QUERY_PARAM_SAMPLES = 5  # distinct real parameter values sampled per query

# ---- Pipeline config (mirrors standardizer_scripts/main_election2023_v1.py,
# the production ingestion script for this exact dataset) ----
BATCH_SIZE = 100_000
CHUNK_SIZE = 100_000
ALL_MODELS = (
    Accounts,
    Posts,
    Entities,
    Actions,
    Communities,
    AccountEnrichments,
    PostEnrichments,
)
ON_CONFLICT = {m: "DO NOTHING" for m in ALL_MODELS}

MODEL_TABLES = [
    "accounts",
    "posts",
    "entities",
    "actions",
    "communities",
    "account_enrichments",
    "post_enrichments",
]


def part_path(checkpoint: int, file_index: int) -> Path:
    """Path of one (of possibly several) pre-sliced files for this checkpoint's
    incremental delta -- see prepare_slices.py, which caps each file at
    FILE_CAP_RECORDS so num_workers > 1 has multiple files to distribute."""
    return DATA_DIR / f"part_{checkpoint:010d}_{file_index:04d}.jsonl.gz"


def part_glob(checkpoint: int) -> str:
    """fnmatch-style pattern (for plan_directories' `include`) matching every
    sliced file belonging to this checkpoint's delta, regardless of how many
    there are."""
    return f"*part_{checkpoint:010d}_*.jsonl.gz"
