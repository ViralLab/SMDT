"""One-time data preparation for the ingestion benchmark.

Slices the real Jan-Feb 2023 Turkish election Twitter dataset into
checkpoint-aligned chunks so run_ingestion_benchmark.py can feed
run_pipeline exactly the incremental records needed to reach each
checkpoint (1K, 10K, 100K, 1M, 10M cumulative records), in order, into
one continuously-growing database.

Each checkpoint's incremental delta is split into multiple files capped at
FILE_CAP_RECORDS records each (not one big file per checkpoint) -- this is
what lets the num_workers > 1 comparison actually parallelize: file-level
parallelism needs more than one file to distribute across workers. The same
sliced files are reused for both the single-threaded and parallel runs;
only PipelineConfig.num_workers differs between them.

This is pure line-slicing (no JSON parsing, no standardization) -- it just
copies raw JSONL lines from the real daily files into new gzip files cut at
exact cumulative-record boundaries. It is NOT part of the timed ingestion
benchmark; run it once before run_ingestion_benchmark.py.

Safe to re-run: if the expected file set already fully exists, does nothing.
If it's partial, regenerates everything from scratch so boundaries stay
consistent.

Usage:
    cd /cta/users/anajafi/SMDT/benchmark_scripts
    ../.venv/bin/python prepare_slices.py
"""
from __future__ import annotations

import gzip
import re
import time
from pathlib import Path

from config import CHECKPOINTS, DATA_DIR, DATASET_DIR, FILE_CAP_RECORDS, part_path

FILENAME_RE = re.compile(r"^stream_collection_(\d{4}-\d{2}-\d{2})\.jsons\.gz$")


def source_files() -> list[Path]:
    """Real daily files for Jan-Feb 2023, in chronological order.

    Matches only the main dated files (excludes the *_turkishbertweet_*/
    *_wordfrequencies* sidecar files, which don't match FILENAME_RE at all).
    """
    files = []
    for p in Path(DATASET_DIR).iterdir():
        m = FILENAME_RE.match(p.name)
        if m and m.group(1).startswith(("2023-01", "2023-02")):
            files.append(p)
    return sorted(files)


def _expected_file_counts(checkpoints_sorted: list[int]) -> dict[int, int]:
    """How many capped files each checkpoint's delta should produce."""
    counts = {}
    lower = 0
    for c in checkpoints_sorted:
        delta = c - lower
        counts[c] = max(1, -(-delta // FILE_CAP_RECORDS))  # ceil division
        lower = c
    return counts


def _all_expected_paths(checkpoints_sorted: list[int]) -> list[Path]:
    counts = _expected_file_counts(checkpoints_sorted)
    return [
        part_path(c, i) for c in checkpoints_sorted for i in range(counts[c])
    ]


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    checkpoints_sorted = sorted(CHECKPOINTS)
    expected = _all_expected_paths(checkpoints_sorted)

    existing = [p for p in expected if p.exists()]
    if len(existing) == len(expected):
        print(f"All {len(expected)} slice files already exist, nothing to do.")
        return
    if existing:
        print(
            f"{len(existing)}/{len(expected)} slice files already exist but not "
            "all -- regenerating everything from scratch to keep boundaries "
            "consistent (also removes any stale files from an older FILE_CAP_RECORDS)."
        )
    for p in DATA_DIR.glob("part_*.jsonl.gz"):
        p.unlink()

    files = source_files()
    if not files:
        raise RuntimeError(f"No source files found under {DATASET_DIR}")
    print(f"Found {len(files)} source files ({files[0].name} .. {files[-1].name})")
    print(f"File cap: {FILE_CAP_RECORDS:,} records/file")

    max_target = checkpoints_sorted[-1]
    ckpt_idx = 0
    lower_bound = 0
    cumulative = 0
    file_index = 0
    records_in_current_file = 0

    def open_new_file():
        p = part_path(checkpoints_sorted[ckpt_idx], file_index)
        return gzip.open(p, "wt", encoding="utf-8")

    t0 = time.time()
    out_f = open_new_file()
    files_written_this_checkpoint = 1
    try:
        for fp in files:
            if cumulative >= max_target:
                break
            with gzip.open(fp, "rt", encoding="utf-8") as in_f:
                for line in in_f:
                    if not line.strip():
                        continue
                    cumulative += 1
                    records_in_current_file += 1
                    out_f.write(line if line.endswith("\n") else line + "\n")

                    target = checkpoints_sorted[ckpt_idx]
                    reached_checkpoint = cumulative == target
                    reached_file_cap = records_in_current_file >= FILE_CAP_RECORDS

                    if reached_checkpoint:
                        out_f.close()
                        print(
                            f"  checkpoint {target:,}: {files_written_this_checkpoint} "
                            f"file(s), records {lower_bound + 1:,}..{target:,}"
                        )
                        lower_bound = target
                        ckpt_idx += 1
                        file_index = 0
                        records_in_current_file = 0
                        if ckpt_idx >= len(checkpoints_sorted):
                            break
                        files_written_this_checkpoint = 1
                        out_f = open_new_file()
                    elif reached_file_cap:
                        out_f.close()
                        file_index += 1
                        records_in_current_file = 0
                        files_written_this_checkpoint += 1
                        out_f = open_new_file()

                if cumulative >= max_target:
                    break
    finally:
        if not out_f.closed:
            out_f.close()

    elapsed = time.time() - t0
    print(f"Done in {elapsed:.1f}s. Total records sliced: {cumulative:,}")
    if cumulative < max_target:
        print(
            f"WARNING: dataset only contained {cumulative:,} records, "
            f"less than the largest checkpoint ({max_target:,})."
        )


if __name__ == "__main__":
    main()
