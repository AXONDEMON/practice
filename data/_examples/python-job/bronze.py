"""Bronze layer: raw PostgreSQL snapshots and LMS JSONL copies, verified on write."""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from settings import HR_TABLES
from common import Config, PipelineError, log, retry
from storage import Store, open_store, read_json, read_many, replace_prefix, to_parquet_bytes, write_json
from run_context import RunContext, timed_stage

# ---------------------------------------------------------------------------
# Bronze
# ---------------------------------------------------------------------------


def connect_postgres(config: Config) -> Any:
    """Open a PostgreSQL connection, retrying transient failures."""
    import psycopg

    def _connect() -> Any:
        return psycopg.connect(
            host=config.postgres_host,
            port=config.postgres_port,
            dbname=config.postgres_name,
            user=config.postgres_user,
            password=config.postgres_pass,
            sslmode="prefer" if config.is_local else "require",
            connect_timeout=10,
        )

    def _is_transient(exc: Exception) -> bool:
        message = str(exc).lower()
        return not any(s in message for s in ("authentication failed", "does not exist", "no password supplied"))

    try:
        return retry(_connect, "postgres_connect", is_transient=_is_transient)
    except psycopg.OperationalError as exc:
        raise PipelineError(
            f"Cannot connect to PostgreSQL {config.postgres_user}@{config.postgres_host}:"
            f"{config.postgres_port}/{config.postgres_name}: {str(exc).strip().splitlines()[-1]}"
        ) from exc


def extract_table(conn: Any, table: str, order_by: list[str], batch_size: int) -> pd.DataFrame:
    """Read a whole table in batches through a server-side cursor.

    Values are kept as text so Bronze is an exact copy of the source.
    """
    from psycopg import sql

    query = sql.SQL("SELECT * FROM {} ORDER BY {}").format(
        sql.Identifier(table), sql.SQL(", ").join(sql.Identifier(c) for c in order_by)
    )
    rows: list[tuple] = []
    columns: list[str] = []
    with conn.cursor(name=f"bronze_{table}") as cur:
        cur.execute(query)
        while batch := cur.fetchmany(batch_size):
            columns = columns or [d.name for d in cur.description]
            rows.extend(tuple(None if v is None else str(v) for v in row) for row in batch)
        columns = columns or [d.name for d in cur.description]
    return pd.DataFrame(rows, columns=columns, dtype="object")


def seed_source(config: Config) -> dict:
    """Load the HR sample data into PostgreSQL if its tables do not exist yet.

    This is a demo bootstrap for the cloud, where Aurora is only reachable
    from inside the VPC: it does there what ``load.sh`` does locally. It
    reads ``schema.sql`` and ``{table}.csv`` from ``HR_SEED_SOURCE_PATH`` and
    does nothing when the source tables are already present.
    """
    source = open_store(config.hr_seed_source)
    with connect_postgres(config) as conn:
        if conn.execute("SELECT to_regclass('public.employees')").fetchone()[0] is not None:
            log(logging.INFO, "seed_skipped", reason="source tables already exist")
            return {"seeded": False}
        conn.execute(source.read_bytes("schema.sql").decode())
        counts = {}
        with conn.cursor() as cur:
            for table in HR_TABLES:
                with cur.copy(f"COPY {table} FROM STDIN WITH (FORMAT csv, HEADER true)") as copy:
                    copy.write(source.read_bytes(f"{table}.csv"))
                counts[table] = cur.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        conn.commit()
    log(logging.INFO, "seed_done", source=config.hr_seed_source, rows=counts)
    return {"seeded": True, "rows": counts}


def sha256_hex(data: bytes) -> str:
    """Return the SHA-256 of ``data`` as hex."""
    return hashlib.sha256(data).hexdigest()


def verify_bronze_objects(lake: Store, expected: dict[str, bytes]) -> int:
    """Read freshly written Bronze objects back and compare size and SHA-256 with the source bytes.

    Raises:
        PipelineError: if any stored object differs from what was sent.
    """
    keys = list(expected)
    mismatched = [key for key, stored in zip(keys, read_many(lake, keys))
                  if len(stored) != len(expected[key]) or sha256_hex(stored) != sha256_hex(expected[key])]
    if mismatched:
        raise PipelineError(f"Bronze verification failed for {len(mismatched)} object(s): {mismatched[:5]}")
    return len(keys)


def write_hr_snapshot(lake: Store, ctx: RunContext, table: str, df: pd.DataFrame, ingest_ts: datetime) -> bool:
    """Write an immutable Bronze snapshot of an HR table if its content changed.

    Snapshots are never overwritten: each change lands in its own
    ``ingest_date=YYYY-MM-DD/run_id=...`` folder. A rerun with identical
    source content writes nothing, so reruns stay idempotent.

    Returns:
        Whether a new snapshot was written.
    """
    manifest_key = "bronze/hr/_manifest.json"
    manifest: dict[str, dict] = read_json(lake, manifest_key, {})
    digest = sha256_hex(pd.util.hash_pandas_object(df, index=False).values.tobytes())
    if manifest.get(table, {}).get("content_sha256") == digest:
        return False
    prefix = f"bronze/hr/{table}/ingest_date={ingest_ts.date().isoformat()}/run_id={ctx.run_id}"
    snapshot = df.assign(_ingest_ts=ingest_ts.isoformat(), _run_id=ctx.run_id,
                         _source_system="postgres", _source_object=f"public.{table}")
    data = to_parquet_bytes(snapshot)
    replace_prefix(lake, prefix, {"part-0000.parquet": data}, ctx.run_id)
    verify_bronze_objects(lake, {f"{prefix}/part-0000.parquet": data})
    manifest[table] = {"content_sha256": digest, "rows": len(df), "path": prefix, "run_id": ctx.run_id}
    write_json(lake, manifest_key, manifest)
    return True


def ingest_lms_files(lake: Store, source: Store, ctx: RunContext, ingest_ts: datetime) -> dict:
    """Copy new or changed LMS files byte-for-byte into Bronze, then verify them.

    A file whose content changed at the source does not overwrite history:
    the previous Bronze copy is first preserved under
    ``bronze/lms/_superseded/run_id=.../``.
    """
    manifest_key = "bronze/lms/_manifest.json"
    manifest: dict[str, dict] = read_json(lake, manifest_key, {})
    keys = [k for k in source.list("") if k.endswith(".jsonl")]
    if not keys:
        raise PipelineError("No LMS files found at the source")

    written: dict[str, bytes] = {}
    skipped = empty = superseded = lines_copied = 0
    for key, data in zip(keys, read_many(source, keys)):
        if not re.search(r"year=\d{4}/month=\d{2}/day=\d{2}/", key):
            log(logging.WARNING, "lms_unexpected_partition_path", file=key)
        digest = sha256_hex(data)
        if manifest.get(key, {}).get("sha256") == digest:
            skipped += 1
            continue
        target = f"bronze/lms/learning_activity/{key}"
        if key in manifest and lake.exists(target):
            archive = f"bronze/lms/_superseded/run_id={ctx.run_id}/{key}"
            lake.write_bytes(archive, lake.read_bytes(target))
            superseded += 1
            log(logging.WARNING, "lms_file_changed_at_source", file=key, previous_version=archive)
        lines = sum(1 for line in data.decode("utf-8", errors="replace").splitlines() if line.strip())
        if lines == 0:
            empty += 1
            log(logging.WARNING, "lms_empty_file", file=key)
        lake.write_bytes(target, data)
        written[target] = data
        lines_copied += lines
        manifest[key] = {"sha256": digest, "bytes": len(data), "lines": lines,
                         "run_id": ctx.run_id, "ingested_at": ingest_ts.isoformat()}
    verified = verify_bronze_objects(lake, written)
    write_json(lake, manifest_key, manifest)
    return {
        "files_seen": len(keys),
        "files_copied": len(written),
        "files_verified": verified,
        "files_skipped": skipped,
        "files_superseded": superseded,
        "files_empty": empty,
        "lines_copied": lines_copied,
        "files_total": len(manifest),
        "lines_total": sum(entry["lines"] for entry in manifest.values()),
    }


@timed_stage("bronze")
def ingest_into_bronze(ctx: RunContext) -> dict:
    """Copy the HR tables and new LMS files into the Bronze layer."""
    config, lake = ctx.config, ctx.lake
    ingest_ts = datetime.now(timezone.utc)
    stats: dict[str, Any] = {}

    # HR tables: immutable snapshots, written only when the content changed.
    with connect_postgres(config) as conn:
        for table, pk in HR_TABLES.items():
            df = extract_table(conn, table, pk, config.batch_size)
            if df.empty:
                raise PipelineError(f"Source table {table} returned zero rows")
            written = write_hr_snapshot(lake, ctx, table, df, ingest_ts)
            stats[table] = {"read": len(df), "snapshot_written": written}
            ctx.add_lineage(f"postgres.public.{table}", f"bronze/hr/{table}", "copy as text + ingest metadata")
            log(logging.INFO, "bronze_table_written" if written else "bronze_table_unchanged",
                table=table, rows=len(df))

    # LMS files: byte-for-byte, incremental, verified; changed files keep their history.
    try:
        stats["learning_activity"] = ingest_lms_files(lake, open_store(config.lms_source), ctx, ingest_ts)
    except PipelineError as exc:
        raise PipelineError(f"{exc} ({config.lms_source})") from exc
    ctx.add_lineage(config.lms_source, "bronze/lms/learning_activity", "byte-for-byte copy (incremental)")
    log(logging.INFO, "bronze_lms_done", **stats["learning_activity"])
    write_json(lake, "_meta/latest/bronze_stats.json", {"run_id": ctx.run_id, "datasets": stats})
    return stats
