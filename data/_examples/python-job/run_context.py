"""Per-run state (counts, lineage, checks) and the timed-stage decorator."""

from __future__ import annotations

import logging
import resource
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from common import Config, log
from storage import Store

# ---------------------------------------------------------------------------
# Run context: counts, lineage and the run manifest
# ---------------------------------------------------------------------------


@dataclass
class RunContext:
    """State collected during one run."""

    config: Config
    lake: Store
    run_id: str = field(default_factory=lambda: datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                        + "-" + uuid.uuid4().hex[:6])
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    stages: dict[str, dict[str, Any]] = field(default_factory=dict)
    lineage: list[dict[str, str]] = field(default_factory=list)
    checks: list[dict[str, Any]] = field(default_factory=list)

    def add_lineage(self, source: str, target: str, transformation: str) -> None:
        """Record a source-to-target edge."""
        self.lineage.append({"source": source, "target": target, "transformation": transformation})


def peak_memory_mb() -> float:
    """Peak resident memory of this process so far, in MB (Linux reports KB)."""
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)


def stage_records(stats: dict) -> int:
    """Count the records a stage handled, from its per-dataset counts."""
    total = 0
    for counts in stats.values():
        if isinstance(counts, dict):
            for key in ("read", "lines", "lines_copied", "written"):
                if isinstance(counts.get(key), int):
                    total += counts[key]
                    break
    return total


def timed_stage(name: str) -> Callable[[Callable[[RunContext], dict]], Callable[[RunContext], dict]]:
    """Log start/end, duration, throughput, memory and counts for a stage function."""

    def decorator(func: Callable[[RunContext], dict]) -> Callable[[RunContext], dict]:
        def wrapper(ctx: RunContext) -> dict:
            log(logging.INFO, "stage_start", stage=name, run_id=ctx.run_id)
            start = time.perf_counter()
            stats = func(ctx)
            duration = round(time.perf_counter() - start, 2)
            records = stage_records(stats)
            per_min = round(records / duration * 60) if duration else None
            ctx.stages[name] = {"duration_s": duration, "records": records, "records_per_min": per_min,
                                "peak_rss_mb": peak_memory_mb(), "datasets": stats}
            log(logging.INFO, "stage_end", stage=name, run_id=ctx.run_id, duration_s=duration, records=records,
                records_per_min=per_min, peak_rss_mb=ctx.stages[name]["peak_rss_mb"])
            return stats

        return wrapper

    return decorator
