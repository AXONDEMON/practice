"""ACME Performance & Talent Management batch ETL job.

Moves data through a medallion data lake:

* Bronze: raw copies of the PostgreSQL HR tables and the LMS learning_activity
  JSONL files, plus ingestion metadata.
* Silver: typed, validated and deduplicated tables. Rows that fail a rule are
  written to quarantine with the rule id and reason.
* Gold: one table per business question, built from Silver.
* Serve: Silver and Gold are also published to PostgreSQL schemas ``silver``
  and ``gold`` for SQL tools (``SERVE_TO_POSTGRES``; on by default locally).

Every run writes a manifest with record counts, reconciliation checks and
lineage edges to ``_meta/runs/{run_id}.json``.

The same code runs locally (``IS_LOCAL=true``, filesystem lake) and on EKS
(``IS_LOCAL=false``, S3 lake, Aurora PostgreSQL with SSL). ``job.py`` is the
entry point; the stages live in sibling modules of this folder:

* ``settings.py``    constants and the Silver schema contracts
* ``common.py``      errors, JSON logging, configuration, retries
* ``storage.py``     local / S3 lake storage and Parquet helpers
* ``run_context.py`` per-run counts, lineage and stage timing
* ``bronze.py``, ``silver.py``, ``gold.py``  the medallion layers
* ``serve.py``       publishes Silver/Gold to PostgreSQL schemas for SQL users
* ``self_tests.py``  the built-in tests (``--self-test``)

All ``*.py`` files in this folder are shipped to the cluster's ConfigMap.

Usage::

    python job.py [--stage all|bronze|silver|gold|serve]
    python job.py --self-test
    python job.py --seed-source [--stage ...]
"""

from __future__ import annotations

import sys

sys.dont_write_bytecode = True  # keep the job folder free of __pycache__

import argparse
import logging
import os
from datetime import datetime, timezone
from typing import Iterable

from common import LOGGER, PipelineError, explain_aws_error, load_config, log, setup_logging
from storage import open_store, write_json
from run_context import RunContext
from bronze import ingest_into_bronze, seed_source
from silver import transform_to_silver
from gold import aggregate_to_gold
from serve import publish_to_postgres
from self_tests import run_self_tests

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def write_manifest(ctx: RunContext, status: str, error: str | None) -> None:
    """Write the run manifest (counts, checks, lineage) to ``_meta/runs``."""
    ended = datetime.now(timezone.utc)
    manifest = {
        "run_id": ctx.run_id,
        "status": status,
        "error": error,
        "started_at": ctx.started_at.isoformat(),
        "ended_at": ended.isoformat(),
        "duration_s": round((ended - ctx.started_at).total_seconds(), 2),
        "config": {"is_local": ctx.config.is_local, "lake_root": ctx.config.lake_root,
                   "lms_source": ctx.config.lms_source, "hr_seed_source": ctx.config.hr_seed_source,
                   "as_of_date": ctx.config.as_of_date,
                   "max_reject_pct": ctx.config.max_reject_pct, "serve_postgres": ctx.config.serve_postgres},
        "stages": ctx.stages,
        "checks": ctx.checks,
        "lineage": ctx.lineage,
    }
    try:
        write_json(ctx.lake, f"_meta/runs/{ctx.run_id}.json", manifest)
        ctx.lake.delete_prefix(f"_staging/{ctx.run_id}")
    except Exception as exc:  # noqa: BLE001 - the run result is already logged
        log(logging.ERROR, "manifest_write_failed", run_id=ctx.run_id, error=f"{type(exc).__name__}: {exc}",
            hint=explain_aws_error(exc), manifest=manifest)


def main(argv: Iterable[str] | None = None) -> int:
    """Run the requested ETL stages and return a process exit code."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--stage", choices=["all", "bronze", "silver", "gold", "serve"], default="all")
    parser.add_argument("--self-test", action="store_true", help="run the built-in tests instead of the pipeline")
    parser.add_argument("--seed-source", action="store_true",
                        help="first load the HR sample into PostgreSQL if its tables are missing (cloud demo)")
    args = parser.parse_args(list(argv) if argv is not None else None)

    setup_logging(os.environ.get("LOG_LEVEL") or "INFO")
    if args.self_test:
        return run_self_tests()
    try:
        config = load_config()
    except (PipelineError, ValueError) as exc:
        log(logging.ERROR, "config_error", error=str(exc))
        return 2

    ctx = RunContext(config=config, lake=open_store(config.lake_root))
    log(logging.INFO, "job_start", run_id=ctx.run_id, stage=args.stage, is_local=config.is_local,
        lake_root=config.lake_root, as_of_date=config.as_of_date)

    status, error = "SUCCESS", None
    try:
        if args.seed_source:
            seed_source(config)
        if args.stage in ("all", "bronze"):
            ingest_into_bronze(ctx)
        if args.stage in ("all", "silver"):
            transform_to_silver(ctx)
        if args.stage in ("all", "gold"):
            aggregate_to_gold(ctx)
            if not all(c["passed"] for c in ctx.checks):
                status, error = "FAILED", "reconciliation checks failed"
        # Only data that passed every reconciliation check is published to PostgreSQL.
        if status == "SUCCESS" and (args.stage == "serve" or (args.stage in ("all", "gold") and config.serve_postgres)):
            publish_to_postgres(ctx)
    except PipelineError as exc:
        status, error = "FAILED", str(exc)
        log(logging.ERROR, "pipeline_error", run_id=ctx.run_id, error=error)
    except Exception as exc:  # noqa: BLE001 - logged with traceback, reported in manifest
        status, error = "FAILED", f"{type(exc).__name__}: {exc}"
        hint = explain_aws_error(exc)
        if hint:
            error = f"{error}. Hint: {hint}"
        LOGGER.exception("unexpected_error", extra={"fields": {"run_id": ctx.run_id, "hint": hint}})
    finally:
        write_manifest(ctx, status, error)

    log(logging.INFO if status == "SUCCESS" else logging.ERROR, "job_end", run_id=ctx.run_id, status=status,
        duration_s=round((datetime.now(timezone.utc) - ctx.started_at).total_seconds(), 2),
        manifest=f"_meta/runs/{ctx.run_id}.json")
    return 0 if status == "SUCCESS" else 1


if __name__ == "__main__":
    sys.exit(main())
