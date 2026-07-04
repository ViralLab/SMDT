"""Regression test for run_pipeline's handling of standardize() returning None.

Standardizer.standardize()'s documented contract is "0..N models" -- an empty
iterable is how a standardizer signals "nothing to emit for this record."
run_pipeline's main loop used to treat a `None` return as fatal (print the
raw record to stdout, then raise ValueError), inconsistent with the (unused)
worker path's handling of the same case, which just counts it and moves on.
"""

from unittest.mock import MagicMock

from smdt.ingest.pipeline import PipelineConfig, run_pipeline
from smdt.ingest.plan import FilePlan, Plan
from smdt.standardizers.base import SourceInfo


class _ReturnsNoneStandardizer:
    name = "dummy"
    platform = "dummy_platform"

    def standardize(self, input_record):
        return None


def test_standardize_returning_none_does_not_raise(tmp_path, capsys):
    jsonl_path = tmp_path / "records.jsonl"
    jsonl_path.write_text('{"id": 1}\n{"id": 2}\n', encoding="utf-8")

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
    run_pipeline(
        plan,
        db,
        _ReturnsNoneStandardizer(),
        config=PipelineConfig(progress=lambda event, info: events.append((event, info))),
    )

    done_info = next(info for event, info in events if event == "done")
    assert done_info["records"] == 2
    # No record_errors -- a None return is not treated as an error.
    assert done_info["record_errors"] == 0

    # No debug dump of raw record data to stdout.
    assert capsys.readouterr().out == ""
