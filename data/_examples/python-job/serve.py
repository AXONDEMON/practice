"""Serving layer: publish Silver, Gold and quarantine tables to PostgreSQL for SQL users.

The lake (Parquet) stays the system of record. This step copies the latest
Silver, Gold and quarantine tables into their own schemas (``silver``,
``gold``, ``quarantine``) so they can be queried with SQL tools such as pgAdmin
and the insights notebook. The source tables in ``public`` are never touched.

All tables are replaced in one transaction: readers see either the previous
run's tables or this run's, never a mix. Row counts are verified in PostgreSQL
before the transaction commits.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from settings import SILVER_CONTRACTS
from common import PipelineError, log
from storage import Store, read_json, read_table
from run_context import RunContext, timed_stage
from bronze import connect_postgres
from gold import GOLD_SOURCES

# Lake prefix -> PostgreSQL schema.
SERVE_SCHEMAS = {"silver": "silver", "gold": "gold", "quarantine": "quarantine"}
# Quarantine tables exist only when a run rejected rows.
QUARANTINE_TABLES = ["learning_activity", "learning_activity_duplicates"]


def pg_type(series: pd.Series) -> str:
    """Map a pandas column to a PostgreSQL column type."""
    dtype = series.dtype
    if pd.api.types.is_bool_dtype(dtype):
        return "boolean"
    if pd.api.types.is_integer_dtype(dtype):
        return "bigint"
    if pd.api.types.is_float_dtype(dtype):
        return "double precision"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        if getattr(dtype, "tz", None) is not None:
            return "timestamptz"
        values = series.dropna()
        # Calendar dates (all midnight, e.g. hire_date) become ``date``; *_ts columns stay timestamps.
        if not str(series.name).endswith("_ts") and (values == values.dt.normalize()).all():
            return "date"
        return "timestamp"
    return "text"


def pg_rows(df: pd.DataFrame, types: list[str]) -> list[tuple]:
    """Convert a DataFrame to plain Python rows, with ``None`` for missing values."""
    columns = []
    for (_, series), col_type in zip(df.items(), types):
        missing = series.isna().tolist()
        if col_type == "text":
            values = series.astype(str).tolist()
        elif col_type == "date":
            values = [v.date() if not m else None for v, m in zip(series.dt.to_pydatetime().tolist(), missing)]
        elif col_type in ("timestamp", "timestamptz"):
            values = series.dt.to_pydatetime().tolist()
        else:
            values = series.tolist()
        columns.append([None if m else v for v, m in zip(values, missing)])
    return list(zip(*columns))


def publish_tables(conn: Any, tables: dict[str, dict[str, pd.DataFrame | None]], run_id: str) -> dict:
    """Replace ``{schema: {table: df}}`` in PostgreSQL in one transaction and verify row counts.

    A ``None`` frame drops the table, so a stale copy does not outlive the lake data.
    """
    from psycopg import sql

    stats = {}
    with conn.transaction(), conn.cursor() as cur:
        for schema, frames in tables.items():
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
            for name, df in frames.items():
                target = sql.Identifier(schema, name)
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(target))
                if df is None:
                    stats[f"{schema}.{name}"] = {"written": 0, "dropped": True}
                    continue
                types = [pg_type(df[c]) for c in df.columns]
                cur.execute(sql.SQL("CREATE TABLE {} ({})").format(target, sql.SQL(", ").join(
                    sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(t)) for c, t in zip(df.columns, types))))
                copy = sql.SQL("COPY {} ({}) FROM STDIN").format(
                    target, sql.SQL(", ").join(sql.Identifier(c) for c in df.columns))
                with cur.copy(copy) as writer:
                    for row in pg_rows(df, types):
                        writer.write_row(row)
                cur.execute(sql.SQL("COMMENT ON TABLE {} IS {}").format(
                    target, sql.Literal(f"Published from lake {schema}/{name} by run {run_id}")))
                cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(target))
                loaded = cur.fetchone()[0]
                if loaded != len(df):
                    raise PipelineError(f"{schema}.{name}: {loaded} rows in PostgreSQL, {len(df)} in the lake")
                stats[f"{schema}.{name}"] = {"written": loaded}
    return stats


def read_lake_tables(lake: Store) -> dict[str, dict[str, pd.DataFrame | None]]:
    """Read the latest Silver, Gold and quarantine tables from the lake (``None`` = no quarantine rows)."""
    names = {
        "silver": [c["target"] for c in SILVER_CONTRACTS.values()],
        "gold": list(GOLD_SOURCES),
    }
    tables: dict[str, dict[str, pd.DataFrame | None]] = {
        SERVE_SCHEMAS[layer]: {name: read_table(lake, f"{layer}/{name}") for name in table_names}
        for layer, table_names in names.items()}
    tables[SERVE_SCHEMAS["quarantine"]] = {
        name: read_table(lake, f"quarantine/{name}")
        if any(k.endswith(".parquet") for k in lake.list(f"quarantine/{name}")) else None
        for name in QUARANTINE_TABLES}
    return tables


def check_latest_gold_run(lake: Store) -> str:
    """Return the id of the newest run that built Gold, if it passed every reconciliation check.

    Gold files are written before reconciliation, so serving on its own
    (``--stage serve``) must not publish Gold from a run whose checks failed.
    """
    for key in sorted((k for k in lake.list("_meta/runs") if k.endswith(".json")), reverse=True):
        manifest = read_json(lake, key, {})
        if "gold" not in manifest.get("stages", {}):
            continue
        failed = [c["check"] for c in manifest.get("checks", []) if not c["passed"]]
        if manifest.get("status") != "SUCCESS" or failed or not manifest.get("checks"):
            raise PipelineError(f"Latest Gold run {manifest.get('run_id')} did not pass reconciliation "
                                f"({len(failed)} failed checks); not publishing")
        return manifest["run_id"]
    raise PipelineError("No completed Gold run found; run the gold stage first")


@timed_stage("serve")
def publish_to_postgres(ctx: RunContext) -> dict:
    """Publish the lake's Silver, Gold and quarantine tables to PostgreSQL schemas of the same name."""
    if "gold" not in ctx.stages:  # serving on its own: Gold came from an earlier run
        log(logging.INFO, "serve_gold_verified", run_id=ctx.run_id, gold_run_id=check_latest_gold_run(ctx.lake))
    tables = read_lake_tables(ctx.lake)
    with connect_postgres(ctx.config) as conn:
        stats = publish_tables(conn, tables, ctx.run_id)
    for key in stats:
        layer, name = key.split(".", 1)
        ctx.add_lineage(f"{layer}/{name}", f"postgres:{key}", "publish (replace)")
    log(logging.INFO, "serve_published", run_id=ctx.run_id, tables=sum(not s.get("dropped") for s in stats.values()),
        rows=sum(s["written"] for s in stats.values()))
    return stats
