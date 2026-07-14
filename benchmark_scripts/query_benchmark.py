"""Query-speed / indexing-strategy benchmark for the paper.

Runs a representative, domain-realistic query workload (point lookups,
composite-index range scans, hypertable chunk-exclusion aggregations,
space-partitioned entity/action queries, and one deliberately UNindexed
query for contrast) against an already-populated ingestion-benchmark
database, and records warm-cache wall-clock latency (repeated runs,
percentiles) plus each query's actual EXPLAIN (ANALYZE, BUFFERS) plan --
scan type (index/bitmap/seq), which index (if any), and shared buffer
hit vs. disk-read block counts.

Scope note: this measures query performance at whatever data volume already
exists in the target database (by default the FINAL state of the
single-threaded ingestion benchmark) -- NOT a latency-vs-scale curve. The
ingestion benchmark is one continuously-growing database with no preserved
per-checkpoint snapshots, so a single full-scale measurement is what's
available without re-running ingestion with snapshots at each checkpoint.
Run again with --num-workers 8 once/if the parallel benchmark exists, for a
second comparison point (same query set, different DB).

Cache scope: this script only ever measures WARM-cache latency (repeated
runs against whatever is already resident in shared_buffers / OS page
cache), plus each query's real shared_hit vs. shared_read block counts from
EXPLAIN BUFFERS. It deliberately does NOT drop OS caches or restart
Postgres to force a cold start -- this is a shared Postgres instance other
work may depend on, and that kind of disruption needs an explicit,
separately-coordinated action, not something baked into an unattended
script.

Enrichment/spatial queries: `post_enrichments`/`account_enrichments` are
empty in an ingestion-only benchmark DB (the standardizer used here never
emits enrichment rows -- that's a separate "enrichment throughput" stage),
and Twitter posts are rarely geotagged, so `location` may be entirely NULL.
Those queries are skipped with a clear reason (not run against empty data)
rather than reported as meaningless near-zero-row timings.

Usage:
    cd /cta/users/anajafi/SMDT/benchmark_scripts
    ../.venv/bin/python query_benchmark.py --num-workers 1
    ../.venv/bin/python query_benchmark.py --num-workers 1 --repeats 30 --param-samples 8

Requires DB_USER / DB_PASSWORD (and optionally DB_HOST / DB_PORT) in the
environment, same as any other StandardDB usage.
"""
from __future__ import annotations

import argparse
import json
import logging
import statistics
import sys
import time
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from config import (
    LOGS_DIR,
    QUERY_PARAM_SAMPLES,
    QUERY_REPEATS,
    db_name,
    mode_name,
    query_results_file,
)

from smdt.store.standard_db import StandardDB

log = logging.getLogger("query_benchmark")


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


# ---------------------------------------------------------------------------
# Representative query set. Each targets a specific real index (or, for
# unindexed_text_search, deliberately none) defined in
# src/smdt/store/schemas/standard_schema.sql -- see that file for the index
# list this workload is validating.
# ---------------------------------------------------------------------------

QUERIES: List[Dict[str, Any]] = [
    {
        "name": "point_lookup_account",
        "category": "point_lookup",
        "index_under_test": "accounts_acct_created_uk (account_id, created_at)",
        "sql": "SELECT * FROM accounts WHERE account_id = %(account_id)s LIMIT 1",
        "params_needed": ["account_id"],
    },
    {
        "name": "point_lookup_post",
        "category": "point_lookup",
        "index_under_test": "posts_post_id_idx (post_id)",
        "sql": "SELECT * FROM posts WHERE post_id = %(post_id)s",
        "params_needed": ["post_id"],
    },
    {
        "name": "account_timeline",
        "category": "composite_index_range",
        "index_under_test": "posts_acct_time_idx (account_id, created_at DESC)",
        "sql": (
            "SELECT * FROM posts WHERE account_id = %(account_id)s "
            "ORDER BY created_at DESC LIMIT 100"
        ),
        "params_needed": ["account_id"],
    },
    {
        "name": "conversation_thread",
        "category": "composite_index_range",
        "index_under_test": "posts_convo_idx (conversation_id)",
        "sql": (
            "SELECT * FROM posts WHERE conversation_id = %(conversation_id)s "
            "ORDER BY created_at"
        ),
        "params_needed": ["conversation_id"],
    },
    {
        "name": "time_range_count",
        "category": "chunk_exclusion_aggregation",
        "index_under_test": "hypertable chunk exclusion + posts_created_at_brin",
        "sql": "SELECT count(*) FROM posts WHERE created_at >= %(start)s AND created_at < %(end)s",
        "params_needed": ["time_window"],
    },
    {
        "name": "posts_per_day",
        "category": "chunk_exclusion_aggregation",
        "index_under_test": "hypertable chunk exclusion + posts_created_at_brin",
        "sql": (
            "SELECT date_trunc('day', created_at) AS day, count(*) "
            "FROM posts WHERE created_at >= %(start)s AND created_at < %(end)s "
            "GROUP BY 1 ORDER BY 1"
        ),
        "params_needed": ["time_window"],
    },
    {
        "name": "top_hashtags",
        "category": "space_partition_aggregation",
        "index_under_test": "entities space partition (entity_type) + entities_acct_type_time_idx",
        "sql": (
            "SELECT body, count(*) AS n FROM entities "
            "WHERE entity_type = 'HASHTAG' AND created_at >= %(start)s AND created_at < %(end)s "
            "GROUP BY body ORDER BY n DESC LIMIT 20"
        ),
        "params_needed": ["time_window"],
    },
    {
        "name": "account_hashtags",
        "category": "composite_index_range",
        "index_under_test": "entities_acct_type_time_idx (account_id, entity_type, created_at DESC)",
        "sql": (
            "SELECT * FROM entities WHERE account_id = %(account_id)s "
            "AND entity_type = 'HASHTAG' ORDER BY created_at DESC LIMIT 50"
        ),
        "params_needed": ["account_id"],
    },
    {
        "name": "who_shared_account",
        "category": "space_partition_range",
        "index_under_test": "actions space partition (action_type) + actions_target_time_idx",
        "sql": (
            "SELECT * FROM actions WHERE action_type = 'SHARE' "
            "AND target_account_id = %(account_id)s "
            "AND created_at >= %(start)s AND created_at < %(end)s"
        ),
        "params_needed": ["account_id", "time_window"],
    },
    {
        "name": "originator_actions",
        "category": "composite_index_range",
        "index_under_test": "actions_origin_time_idx (originator_account_id, created_at DESC)",
        "sql": (
            "SELECT * FROM actions WHERE originator_account_id = %(account_id)s "
            "ORDER BY created_at DESC LIMIT 100"
        ),
        "params_needed": ["account_id"],
    },
    {
        "name": "unindexed_text_search",
        "category": "unindexed_contrast",
        "index_under_test": "none (deliberate) -- posts.body has no text index",
        "sql": "SELECT count(*) FROM posts WHERE body ILIKE %(pattern)s",
        "params_needed": ["text_pattern"],
    },
]

# Data-dependent extras: only run if the target DB actually has this data.
# An ingestion-only benchmark DB has empty enrichment tables and likely no
# geotagged posts -- see module docstring.
OPTIONAL_QUERIES: List[Dict[str, Any]] = [
    {
        "name": "spatial_nearby",
        "category": "spatial_gist",
        "index_under_test": "posts_location_gix (GIST on location)",
        "sql": (
            "SELECT * FROM posts WHERE location IS NOT NULL "
            "AND ST_DWithin(location, ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326), %(radius)s) "
            "LIMIT 50"
        ),
        "params_needed": ["location_sample"],
        "requires_data": "location",
    },
    {
        "name": "enrichment_join",
        "category": "enrichment_join",
        "index_under_test": "post_enrichments (model_id, post_id) unique + posts_acct_time_idx",
        "sql": (
            "SELECT p.post_id, pe.body FROM posts p "
            "JOIN post_enrichments pe ON pe.post_id = p.post_id "
            "WHERE p.account_id = %(account_id)s LIMIT 50"
        ),
        "params_needed": ["account_id"],
        "requires_data": "enrichment",
    },
]


def percentile(sorted_vals: Sequence[float], p: float) -> float:
    """Linear-interpolation percentile, no external dependency required."""
    k = (len(sorted_vals) - 1) * (p / 100)
    f, c = int(k), min(int(k) + 1, len(sorted_vals) - 1)
    if f == c:
        return sorted_vals[f]
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)


def compute_stats(latencies_s: Sequence[float]) -> Dict[str, float]:
    ms = sorted(x * 1000 for x in latencies_s)
    return {
        "n": len(ms),
        "min_ms": ms[0],
        "p50_ms": percentile(ms, 50),
        "p95_ms": percentile(ms, 95),
        "max_ms": ms[-1],
        "mean_ms": sum(ms) / len(ms),
        "stddev_ms": statistics.stdev(ms) if len(ms) > 1 else 0.0,
    }


def time_query(cur, sql: str, params: Dict[str, Any], repeats: int) -> List[float]:
    latencies = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        cur.execute(sql, params)
        cur.fetchall()
        latencies.append(time.perf_counter() - t0)
    return latencies


def explain_query(cur, sql: str, params: Dict[str, Any]) -> Dict[str, Any]:
    cur.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, params)
    plan_json = cur.fetchone()[0][0]
    root = plan_json["Plan"]

    acc = {"shared_hit": 0, "shared_read": 0, "scan_nodes": []}

    def walk(node: Dict[str, Any]) -> None:
        acc["shared_hit"] += node.get("Shared Hit Blocks", 0)
        acc["shared_read"] += node.get("Shared Read Blocks", 0)
        acc["scan_nodes"].append(
            {
                "node_type": node.get("Node Type"),
                "relation": node.get("Relation Name"),
                "index_name": node.get("Index Name"),
            }
        )
        for child in node.get("Plans", []) or []:
            walk(child)

    walk(root)
    return {
        "planning_time_ms": plan_json.get("Planning Time"),
        "execution_time_ms": plan_json.get("Execution Time"),
        "shared_hit_blocks": acc["shared_hit"],
        "shared_read_blocks": acc["shared_read"],
        "scan_nodes": acc["scan_nodes"],
    }


def _sample_with_escalation(cur, sql_template: str, params: Tuple[Any, ...]) -> List[Tuple[Any, ...]]:
    """`sql_template` must contain one `{pct}` placeholder for the TABLESAMPLE
    percentage. Escalates 1% -> 10% -> 100% until it finds any rows.

    A fixed 1% sample is fine at 10M rows in the common case, but a
    selective WHERE clause (e.g. `conversation_id IS NOT NULL`) or a small
    table can otherwise make even a fairly generous fixed sample come back
    completely empty -- this only pays the cost of a wider (or full) scan
    when the narrow one actually failed to find anything.
    """
    rows: List[Tuple[Any, ...]] = []
    for pct in (1, 10, 100):
        cur.execute(sql_template.format(pct=pct), params)
        rows = cur.fetchall()
        if rows:
            return rows
    return rows


def sample_params(cur, n_samples: int) -> Dict[str, Any]:
    """One-time setup: pull real, representative parameter values out of the
    target DB itself (not hardcoded/synthetic IDs, which likely wouldn't
    exist and would make every timed query a trivial zero-row lookup).

    Not part of the timed benchmark results -- these queries run once,
    logged separately.
    """
    log.info("Sampling representative parameters (one-time setup)...")

    sampled: Dict[str, Any] = {}

    # account_ids: half "power users" (most posts), half "typical" (TABLESAMPLE)
    # -- a plain random sample would mostly land on low-activity accounts
    # (power-law distribution), understating how the index performs on the
    # accounts that actually drive most query volume in practice.
    n_top = max(1, n_samples // 2)
    n_typical = n_samples - n_top
    cur.execute(
        "SELECT account_id FROM posts GROUP BY account_id ORDER BY count(*) DESC LIMIT %s",
        (n_top,),
    )
    top_ids = [r[0] for r in cur.fetchall()]
    typical_rows = _sample_with_escalation(
        cur,
        "SELECT DISTINCT account_id FROM posts TABLESAMPLE SYSTEM ({pct}) LIMIT %s",
        (n_typical * 3,),
    )
    typical_ids = [r[0] for r in typical_rows if r[0] not in top_ids][:n_typical]
    account_ids = (top_ids + typical_ids)[:n_samples]
    sampled["account_ids"] = account_ids or top_ids or typical_ids

    # post_ids: TABLESAMPLE keeps this cheap regardless of table size (a
    # `ORDER BY random()` at 10M+ rows would itself be a slow full scan+sort).
    post_id_rows = _sample_with_escalation(
        cur, "SELECT post_id FROM posts TABLESAMPLE SYSTEM ({pct}) LIMIT %s", (n_samples,)
    )
    sampled["post_ids"] = [r[0] for r in post_id_rows]

    # conversation_ids that are actual threads (>1 post), found by checking
    # TABLESAMPLE candidates against posts_convo_idx (cheap per-candidate
    # lookup) rather than a full GROUP BY HAVING over the whole table.
    conv_candidate_rows = _sample_with_escalation(
        cur,
        "SELECT DISTINCT conversation_id FROM posts TABLESAMPLE SYSTEM ({pct}) "
        "WHERE conversation_id IS NOT NULL LIMIT %s",
        (n_samples * 5,),
    )
    conv_ids = []
    for (cid,) in conv_candidate_rows:
        cur.execute("SELECT count(*) FROM posts WHERE conversation_id = %s", (cid,))
        if cur.fetchone()[0] > 1:
            conv_ids.append(cid)
        if len(conv_ids) >= n_samples:
            break
    sampled["conversation_ids"] = conv_ids

    # Time window: a ~3-day slice starting at the MEDIAN of a real,
    # density-weighted sample of created_at values -- NOT a naive
    # min+(max-min)*0.4 interpolation. posts.created_at is the tweet's
    # original post date, not collection date, so retweets/quotes of old
    # content stretch min() back years (this dataset: min is 2008, but 99%+
    # of rows are packed into a ~19-day window in early 2023). A linear
    # interpolation between min and max lands in that sparse historical
    # tail, not the real data, and silently produces near-empty (and
    # therefore misleadingly fast) time-filtered query results. Sampling
    # actual rows and taking the median is robust to that skew since the
    # overwhelming majority of sampled rows come from the dense window.
    cur.execute("SELECT max(created_at) FROM posts")
    (hi,) = cur.fetchone()
    time_rows = _sample_with_escalation(
        cur, "SELECT created_at FROM posts TABLESAMPLE SYSTEM ({pct})", ()
    )
    times = sorted(r[0] for r in time_rows)
    start = times[len(times) // 2]
    end = min(start + timedelta(days=3), hi)
    sampled["time_window"] = (start, end)

    # A real word (>=6 letters) pulled from an actual post body, for the
    # deliberately-unindexed ILIKE contrast query.
    body_rows = _sample_with_escalation(
        cur,
        "SELECT body FROM posts TABLESAMPLE SYSTEM ({pct}) "
        "WHERE body IS NOT NULL AND length(body) > 20 LIMIT 50",
        (),
    )
    pattern = None
    for (body,) in body_rows:
        for w in body.split():
            w = w.strip(".,!?:;\"'()[]{}#@")
            if len(w) >= 6 and w.isalpha():
                pattern = f"%{w}%"
                break
        if pattern:
            break
    sampled["text_pattern"] = pattern

    # Data-dependent extras: check presence before ever building a param set.
    cur.execute("SELECT EXISTS (SELECT 1 FROM posts WHERE location IS NOT NULL)")
    sampled["has_location"] = cur.fetchone()[0]
    if sampled["has_location"]:
        cur.execute("SELECT ST_X(location), ST_Y(location) FROM posts WHERE location IS NOT NULL LIMIT 1")
        lon, lat = cur.fetchone()
        sampled["location_sample"] = (lon, lat)

    cur.execute("SELECT EXISTS (SELECT 1 FROM post_enrichments)")
    sampled["has_enrichment"] = cur.fetchone()[0]

    log.info(
        "Sampled: %d account_ids, %d post_ids, %d conversation_ids, "
        "time_window=%s..%s, text_pattern=%s, has_location=%s, has_enrichment=%s",
        len(sampled["account_ids"]), len(sampled["post_ids"]), len(sampled["conversation_ids"]),
        start, end, pattern, sampled["has_location"], sampled["has_enrichment"],
    )
    return sampled


def build_param_sets(query: Dict[str, Any], sampled: Dict[str, Any]) -> List[Dict[str, Any]]:
    keys = query["params_needed"]
    start, end = sampled["time_window"]

    if keys == ["account_id"]:
        return [{"account_id": v} for v in sampled["account_ids"]]
    if keys == ["post_id"]:
        return [{"post_id": v} for v in sampled["post_ids"]]
    if keys == ["conversation_id"]:
        return [{"conversation_id": v} for v in sampled["conversation_ids"]]
    if keys == ["time_window"]:
        return [{"start": start, "end": end}]
    if keys == ["account_id", "time_window"]:
        return [{"account_id": v, "start": start, "end": end} for v in sampled["account_ids"]]
    if keys == ["text_pattern"]:
        return [{"pattern": sampled["text_pattern"]}] if sampled.get("text_pattern") else []
    if keys == ["location_sample"]:
        if not sampled.get("has_location"):
            return []
        lon, lat = sampled["location_sample"]
        return [{"lon": lon, "lat": lat, "radius": 0.5}]  # degrees -- see module docstring
    raise ValueError(f"Unhandled params_needed combination for {query['name']!r}: {keys}")


def run_all(cur, sampled: Dict[str, Any], repeats: int) -> List[Dict[str, Any]]:
    results = []
    for query in QUERIES + OPTIONAL_QUERIES:
        requires_data = query.get("requires_data")
        if requires_data == "location" and not sampled.get("has_location"):
            results.append(_skipped(query, "no posts with non-null location in this DB"))
            continue
        if requires_data == "enrichment" and not sampled.get("has_enrichment"):
            results.append(_skipped(
                query,
                "post_enrichments is empty -- this is an ingestion-only benchmark DB; "
                "run after the enrichment-throughput stage populates it",
            ))
            continue

        param_sets = build_param_sets(query, sampled)
        if not param_sets:
            results.append(_skipped(query, "no sample parameter values available"))
            continue

        all_latencies: List[float] = []
        per_param = []
        for params in param_sets:
            lat = time_query(cur, query["sql"], params, repeats)
            all_latencies.extend(lat)
            per_param.append({"params": _jsonable(params), "stats": compute_stats(lat)})

        explain = explain_query(cur, query["sql"], param_sets[0])
        pooled = compute_stats(all_latencies)
        results.append({
            "name": query["name"],
            "category": query["category"],
            "index_under_test": query["index_under_test"],
            "sql": query["sql"],
            "skipped": False,
            "n_param_sets": len(param_sets),
            "repeats_per_param_set": repeats,
            "pooled_stats": pooled,
            "per_param": per_param,
            "explain": explain,
        })
        log.info(
            "%-24s pooled p50=%7.2fms p95=%7.2fms (n=%d)  scan=%s",
            query["name"], pooled["p50_ms"], pooled["p95_ms"], pooled["n"],
            [n["node_type"] for n in explain["scan_nodes"] if "Scan" in (n["node_type"] or "")],
        )
    return results


def _skipped(query: Dict[str, Any], reason: str) -> Dict[str, Any]:
    log.info("%-24s SKIPPED: %s", query["name"], reason)
    return {
        "name": query["name"],
        "category": query["category"],
        "index_under_test": query["index_under_test"],
        "sql": query["sql"],
        "skipped": True,
        "skip_reason": reason,
    }


def _jsonable(params: Dict[str, Any]) -> Dict[str, Any]:
    return {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in params.items()}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--num-workers", type=int, default=1,
        help="Which ingestion-benchmark DB to query against: 1=single (default), N=parallelN.",
    )
    parser.add_argument(
        "--repeats", type=int, default=QUERY_REPEATS,
        help=f"Timed (warm-cache) repetitions per (query, sampled-param) pair. Default {QUERY_REPEATS}.",
    )
    parser.add_argument(
        "--param-samples", type=int, default=QUERY_PARAM_SAMPLES,
        help=f"Distinct real parameter values sampled per identity-keyed query. Default {QUERY_PARAM_SAMPLES}.",
    )
    args = parser.parse_args()

    db_name_ = db_name(args.num_workers)
    log_path = LOGS_DIR / f"query_benchmark_{mode_name(args.num_workers)}.log"
    setup_logging(log_path)
    log.info(
        "Query benchmark starting. mode=%s DB=%s repeats=%d param_samples=%d",
        mode_name(args.num_workers), db_name_, args.repeats, args.param_samples,
    )

    db = StandardDB(db_name_)  # initialize=False (default) -- read-only use, never touches schema
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            sampled = sample_params(cur, args.param_samples)
            results = run_all(cur, sampled, args.repeats)
        conn.rollback()  # belt-and-suspenders: this script only ever SELECTs/EXPLAINs
    finally:
        conn.close()

    out_path = query_results_file(args.num_workers)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        for r in results:
            f.write(json.dumps(r, default=str) + "\n")
    log.info("Wrote %d query results to %s", len(results), out_path)


if __name__ == "__main__":
    main()
