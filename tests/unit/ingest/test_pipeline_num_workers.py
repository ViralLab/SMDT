"""Regression test: PipelineConfig(num_workers=1) (explicit) must behave
identically to the default (num_workers unspecified) -- num_workers=1 is
the single-threaded path, completely unchanged by adding parallel support.

Real ProcessPoolExecutor-based (num_workers > 1) behavior needs a real,
picklable DB connection (a MagicMock isn't picklable across a process
boundary), so that path is covered separately in the integration tests.
"""

from unittest.mock import MagicMock

from smdt.ingest.pipeline import PipelineConfig, run_pipeline
from smdt.ingest.plan import FilePlan, Plan


class _CountingStandardizer:
    name = "dummy"
    platform = "dummy_platform"

    def standardize(self, input_record):
        return []


def _run(base_dir, num_workers_kwarg):
    base_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = base_dir / "records.jsonl"
    jsonl_path.write_text('{"id": 1}\n{"id": 2}\n{"id": 3}\n', encoding="utf-8")

    plan = Plan(
        files=[
            FilePlan(
                path=str(jsonl_path),
                size=jsonl_path.stat().st_size,
                mtime=jsonl_path.stat().st_mtime,
                reader_name="jsonl",
                is_archive=False,
            )
        ]
    )

    db = MagicMock()
    cur = db.connect.return_value.cursor.return_value.__enter__.return_value
    cur.fetchone.return_value = None  # forces _upsert_dataset_meta's INSERT branch

    events = []
    kwargs = {} if num_workers_kwarg is None else {"num_workers": num_workers_kwarg}
    run_pipeline(
        plan,
        db,
        _CountingStandardizer(),
        config=PipelineConfig(
            progress=lambda event, info: events.append((event, info)), **kwargs
        ),
    )
    return next(info for event, info in events if event == "done")


def test_pipeline_config_defaults_to_single_worker():
    assert PipelineConfig().num_workers == 1


def test_num_workers_1_matches_default_behavior(tmp_path):
    default_result = _run(tmp_path / "a", None)
    explicit_result = _run(tmp_path / "b", 1)
    # 'elapsed' will always differ between two separate timed runs.
    default_result.pop("elapsed")
    explicit_result.pop("elapsed")
    assert default_result == explicit_result
    assert default_result["records"] == 3
    assert default_result["record_errors"] == 0
