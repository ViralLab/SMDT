"""
Generate a single paper-ready summary figure for the SMDT scalability benchmark.

Three panels:
  A. Ingestion throughput: single vs parallel at each checkpoint
  B. Query latency by access pattern (p50 bar + p95 error bar)
  C. Pseudonymization speedup per table

Output: benchmark_summary.pdf  (vector, paper-ready)
"""

import json
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
from pathlib import Path

# ---------------------------------------------------------------------------
# Data -- extracted from the benchmark logs
# ---------------------------------------------------------------------------

# Ingestion throughput (records/sec) at each cumulative checkpoint
CHECKPOINTS = [1_000, 10_000, 100_000, 1_000_000, 10_000_000]
SINGLE_THROUGHPUT = [69.0, 340.0, 364.4, 370.3, 355.8]
PARALLEL_THROUGHPUT = [65.4, 338.9, 1615.9, 2021.1, 2091.5]

# Peak total RSS (MB) -- main process + all workers
SINGLE_MEMORY_MB = [54.7, 120.8, 331.0, 423.4, 461.7]
PARALLEL_MEMORY_MB = [326.3, 388.6, 1088.6, 1295.6, 1375.8]

# Query performance: p50 and p95 (ms), sorted by p50 descending
QUERY_NAMES = [
    "Unindexed text search",
    "Top hashtags\n(space-partitioned)",
    "Posts per day\n(chunk exclusion)",
    "Time-range count\n(chunk exclusion)",
    "Spatial nearby\n(GIST)",
    "Hashtag-posts join\n(two-table)",
    "Conversation thread",
    "Account timeline",
    "Point lookup (post)",
    "Account hashtags\n(composite index)",
    "Originator actions",
    "Point lookup (account)",
    "Who shared account\n(space partition)",
]
QUERY_P50 = [2623.2, 582.5, 273.3, 132.3, 70.6, 15.8, 14.1, 13.5, 13.2, 5.3, 3.3, 3.1, 0.33]
QUERY_P95 = [2674.4, 810.6, 458.7, 191.9, 93.5, 279.8, 86.1, 80.9, 56.3, 41.5, 45.2, 11.8, 154.7]

# Pseudonymization: rows/sec per table, single vs parallel
PSEUDO_TABLES = ["entities\n(64.0M)", "actions\n(10.5M)", "accounts\n(815K)", "posts\n(10.7M)"]
PSEUDO_SINGLE = [6492, 8995, 3055, 533]
PSEUDO_PARALLEL = [12037, 8205, 8606, 3430]
PSEUDO_SPEEDUP = [p / s for p, s in zip(PSEUDO_PARALLEL, PSEUDO_SINGLE)]

# ---------------------------------------------------------------------------
# Style setup -- clean, publication-ready
# ---------------------------------------------------------------------------
plt.rcParams.update({
    "font.family": "sans-serif",
    "font.sans-serif": ["DejaVu Sans", "Arial", "Helvetica"],
    "font.size": 8,
    "axes.titlesize": 9,
    "axes.labelsize": 8,
    "xtick.labelsize": 7,
    "ytick.labelsize": 7,
    "legend.fontsize": 7,
    "figure.dpi": 150,
    "savefig.dpi": 300,
    "savefig.bbox": "tight",
    "axes.spines.top": False,
    "axes.spines.right": False,
})

COLOR_SINGLE = "#2166AC"   # blue
COLOR_PARALLEL = "#B2182B"  # red
COLOR_BAR = "#4393C3"
COLOR_P95 = "#D6604D"
GRAY = "#999999"

# ---------------------------------------------------------------------------
# Figure layout -- 3 panels side by side
# ---------------------------------------------------------------------------
fig, axes = plt.subplots(1, 3, figsize=(10, 4.0))
(ax_a, ax_b, ax_c) = axes

# ---- Panel A: Ingestion throughput ----
x = np.arange(len(CHECKPOINTS))
width = 0.35
bars1 = ax_a.bar(x - width / 2, SINGLE_THROUGHPUT, width,
                  color=COLOR_SINGLE, edgecolor="white", linewidth=0.5, label="Single")
bars2 = ax_a.bar(x + width / 2, PARALLEL_THROUGHPUT, width,
                  color=COLOR_PARALLEL, edgecolor="white", linewidth=0.5, label="8 workers")

# Annotate speedup on parallel bars for 100K+
for i in range(2, len(CHECKPOINTS)):
    sp = PARALLEL_THROUGHPUT[i] / SINGLE_THROUGHPUT[i]
    ax_a.text(x[i] + width / 2, PARALLEL_THROUGHPUT[i] + 60,
              f"{sp:.1f}x", ha="center", va="bottom", fontsize=6.5,
              color=COLOR_PARALLEL, fontweight="bold")

ax_a.set_xticks(x)
ax_a.set_xticklabels(["1K", "10K", "100K", "1M", "10M"])
ax_a.set_ylabel("Records per second")
ax_a.set_xlabel("Cumulative records ingested")
ax_a.legend(frameon=False, loc="upper left")
ax_a.set_ylim(0, 2500)
ax_a.text(-0.15, 1.04, "A", transform=ax_a.transAxes, fontsize=10, fontweight="bold", va="bottom")

# ---- Panel B: Query latency ----
y_pos = range(len(QUERY_NAMES))
ax_b.barh(y_pos, QUERY_P50, color=COLOR_BAR, edgecolor="white", linewidth=0.3)
# p95 as overlaid tick marks, always drawn and brought to front
for i in y_pos:
    ax_b.plot(QUERY_P95[i], i, marker="|", color=COLOR_P95, markersize=8,
              markeredgewidth=1.5, zorder=10)

ax_b.set_yticks(y_pos)
ax_b.set_yticklabels(QUERY_NAMES, fontsize=6.5)
ax_b.set_xlabel("Latency (ms)")
ax_b.set_xscale("log")
ax_b.set_xlim(0.15, 5000)
ax_b.axvline(x=10, color=GRAY, linewidth=0.5, linestyle="--", alpha=0.5)
ax_b.axvline(x=1000, color=GRAY, linewidth=0.5, linestyle="--", alpha=0.5)

# Legend for p50/p95 + join
from matplotlib.lines import Line2D
legend_elements = [
    plt.Rectangle((0, 0), 1, 1, fc=COLOR_BAR, ec="white", linewidth=0.3, label="p50"),
    Line2D([0], [0], marker="|", color=COLOR_P95, markersize=8, markeredgewidth=1.5,
           linestyle="None", label="p95"),
]
ax_b.legend(handles=legend_elements, frameon=True, fancybox=False,
           edgecolor="#AAAAAA", facecolor="white", framealpha=0.8,
           loc="upper right", fontsize=6.5)
ax_b.text(-0.15, 1.04, "B", transform=ax_b.transAxes, fontsize=10, fontweight="bold", va="bottom")

# ---- Panel C: Pseudonymization speedup ----
y_pos_c = range(len(PSEUDO_TABLES))
bars_c = ax_c.barh(y_pos_c, PSEUDO_SPEEDUP, color=COLOR_BAR, edgecolor="white", linewidth=0.5)

# Add a subtle annotation for the action table being slower
# Show 0.9x in a different color directly
for i, sp in enumerate(PSEUDO_SPEEDUP):
    color = COLOR_PARALLEL if sp < 1.0 else "#333333"
    ax_c.text(sp + 0.08, i, f"{sp:.1f}x", va="center", fontsize=7, fontweight="bold", color=color)
    ax_c.text(sp + 0.08, i - 0.22,
              f"({PSEUDO_SINGLE[i]:,}  {PSEUDO_PARALLEL[i]:,} r/s)",
              va="center", fontsize=5.5, color=GRAY)

ax_c.set_yticks(y_pos_c)
ax_c.set_yticklabels(PSEUDO_TABLES, fontsize=7)
ax_c.set_xlabel("Speedup (parallel / single)")
ax_c.axvline(x=1.0, color=GRAY, linewidth=0.8, linestyle="-", alpha=0.6)
ax_c.set_xlim(0, max(PSEUDO_SPEEDUP) * 1.25)
ax_c.text(-0.15, 1.04, "C", transform=ax_c.transAxes, fontsize=10, fontweight="bold", va="bottom")


# ---------------------------------------------------------------------------
# Final layout and save
# ---------------------------------------------------------------------------
fig.tight_layout(pad=1.5, w_pad=1.0)

outpath = Path(__file__).resolve().parent / "benchmark_summary.pdf"
fig.savefig(outpath, format="pdf", dpi=300, bbox_inches="tight")
fig.savefig(outpath.with_suffix(".png"), format="png", dpi=300, bbox_inches="tight")
print(f"Saved to {outpath} and {outpath.with_suffix('.png')}")
