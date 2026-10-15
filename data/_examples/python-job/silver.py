"""Silver layer: contract validation, quarantine, dedup and typed tables."""

from __future__ import annotations

import io
import json
import logging
from datetime import date

import numpy as np
import pandas as pd

from settings import (
    HR_TABLES, JOB_LEVEL_RANK, LMS_KNOWN_FIELDS, QUIZ_EVENT_TYPES, QUIZ_PASS_MARK, SILVER_CONTRACTS,
)
from common import PipelineError, log
from storage import Store, read_many, replace_prefix, to_parquet_bytes, write_json, write_table
from run_context import RunContext, timed_stage

# ---------------------------------------------------------------------------
# Silver: schema validation
# ---------------------------------------------------------------------------


def _is_blank(series: pd.Series) -> pd.Series:
    """Return True where a value is null or an empty string."""
    text = series.astype("string").str.strip()
    return (series.isna() | text.eq("").fillna(False)).astype(bool)


def _cast(series: pd.Series, col_type: str) -> pd.Series:
    """Cast raw values to the contract type; invalid values become null."""
    if col_type == "int":
        number = pd.to_numeric(series, errors="coerce")
        return number.where(number % 1 == 0).astype("Int64")
    if col_type == "decimal":
        return pd.to_numeric(series, errors="coerce").astype("float64")
    if col_type == "date":
        return pd.to_datetime(series, format="%Y-%m-%d", errors="coerce")
    if col_type == "timestamp":
        return pd.to_datetime(series, format="%Y-%m-%dT%H:%M:%S", errors="coerce")
    return series.astype("string").str.strip()


def validate(df: pd.DataFrame, dataset: str,
             keys: dict[str, set]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Apply a Silver contract to raw rows.

    Each row is checked column by column (required, type, domain, range,
    pattern, foreign key), then for duplicate keys. A rejected row keeps
    the first rule it failed.

    Args:
        df: Raw rows; every column may be text.
        dataset: Key into ``SILVER_CONTRACTS``.
        keys: Valid key values of already-processed parent tables, by
            ``"{silver_table}.{column}"``.

    Returns:
        ``(valid, rejected, duplicates)``: typed valid rows with renamed
        columns, rejected raw rows with ``_rule_id`` and ``_reason``, and
        the copies dropped under the ``dedup`` policy, each with the
        location of the copy that was kept (``kept_source_file``,
        ``kept_line_no``) so the drop can be audited.
    """
    contract = SILVER_CONTRACTS[dataset]
    columns: dict[str, dict] = contract["columns"]
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise PipelineError(f"{dataset}: missing required columns {missing}")

    rule = pd.Series(pd.NA, index=df.index, dtype="object")
    reason = pd.Series(pd.NA, index=df.index, dtype="object")

    def reject(mask: pd.Series, rule_id: str, message: str | pd.Series) -> None:
        new = mask.fillna(False).astype(bool) & rule.isna()
        rule[new] = f"{dataset}.{rule_id}"
        reason[new] = message[new] if isinstance(message, pd.Series) else message

    typed: dict[str, pd.Series] = {}
    for col, spec in columns.items():
        raw = df[col]
        blank = _is_blank(raw)
        value = _cast(raw.where(~blank), spec["type"])
        typed[col] = value
        if spec.get("required"):
            reject(blank, f"{col}.REQUIRED", f"{col} is null or empty")
        reject(~blank & value.isna(), f"{col}.TYPE",
               f"{col} is not a valid {spec['type']}: " + raw.astype("string").fillna(""))
        present = value.notna()
        if "domain" in spec:
            reject(present & ~value.isin(spec["domain"]), f"{col}.DOMAIN",
                   f"{col} not in {spec['domain']}: " + raw.astype("string").fillna(""))
        if "min" in spec:
            reject(present & (value < spec["min"]), f"{col}.RANGE", f"{col} below {spec['min']}")
        if "max" in spec:
            reject(present & (value > spec["max"]), f"{col}.RANGE", f"{col} above {spec['max']}")
        if "pattern" in spec:
            reject(present & ~value.str.match(spec["pattern"]).fillna(False), f"{col}.PATTERN",
                   f"{col} does not match {spec['pattern']}")
        if "fk" in spec:
            parent = keys["{}.{}".format(*spec["fk"])]
            reject(present & ~value.isin(parent), f"{col}.FK",
                   f"{col} not found in {spec['fk'][0]}: " + raw.astype("string").fillna(""))

    out = pd.DataFrame(typed, index=df.index)
    for extra in [c for c in df.columns if c.startswith("_") or c.startswith("dq_")]:
        out[extra] = df[extra]

    duplicates = pd.DataFrame()
    for key_cols, label in [(contract["pk"], "PK"), (contract.get("unique"), "UNIQUE")]:
        if not key_cols:
            continue
        # Only rows that passed every check compete, so a valid copy is never
        # dropped in favour of a rejected one.
        passed = out[rule.isna()]
        duplicate = pd.Series(False, index=out.index)
        duplicate[passed.index] = passed.duplicated(subset=key_cols, keep="first")
        if contract.get("pk_policy") == "dedup" and label == "PK":
            location = [c for c in ("_source_file", "_line_no") if c in out.columns]
            kept = passed[~duplicate[passed.index]][key_cols + location]
            duplicates = out.loc[duplicate, list(columns) + location].merge(
                kept.rename(columns={c: f"kept{c}" for c in location}), on=key_cols, how="left")
            duplicates.insert(0, "_rule_id", f"{dataset}.{'_'.join(key_cols)}.DUPLICATE_DROPPED")
            duplicates.insert(1, "_reason", f"duplicate {key_cols}; first valid copy kept")
            out = out[~duplicate]
            rule, reason = rule[~duplicate], reason[~duplicate]
            df = df[~duplicate]
        else:
            reject(duplicate, f"{'_'.join(key_cols)}.{label}_DUPLICATE", f"duplicate {key_cols}")

    valid = out[rule.isna()].rename(columns={c: s["rename"] for c, s in columns.items() if "rename" in s})
    rejected = df[rule.notna()].copy()
    rejected["_rule_id"] = rule[rule.notna()]
    rejected["_reason"] = reason[rule.notna()]
    return valid.reset_index(drop=True), rejected.reset_index(drop=True), duplicates.reset_index(drop=True)


def quarantine_frame(rejected: pd.DataFrame, run_id: str) -> pd.DataFrame:
    """Shape rejected rows for the quarantine area (raw row kept as JSON)."""
    if rejected.empty:
        return pd.DataFrame(columns=["_rule_id", "_reason", "_raw", "_run_id"])
    if "_raw" not in rejected.columns:
        data_cols = [c for c in rejected.columns if not c.startswith("_")]
        rejected = rejected.assign(_raw=rejected[data_cols].apply(
            lambda r: json.dumps({k: (None if pd.isna(v) else v) for k, v in r.items()}, default=str), axis=1))
    keep = [c for c in ["_rule_id", "_reason", "_raw", "_source_file", "_line_no"] if c in rejected.columns]
    out = rejected[keep].copy()
    out["_run_id"] = run_id
    return out


def check_reject_rate(dataset: str, read: int, rejected: int, max_pct: float) -> float:
    """Stop the stage if too many rows were rejected."""
    pct = round(100.0 * rejected / read, 3) if read else 0.0
    if pct > max_pct:
        raise PipelineError(f"{dataset}: {pct}% of rows rejected (limit {max_pct}%); Silver not updated")
    return pct


# ---------------------------------------------------------------------------
# Silver: transforms
# ---------------------------------------------------------------------------


def latest_hr_snapshot(lake: Store, table: str) -> pd.DataFrame:
    """Read the most recent Bronze snapshot of an HR table."""
    files = [k for k in lake.list(f"bronze/hr/{table}") if k.endswith(".parquet")]
    if not files:
        raise PipelineError(f"No Bronze snapshot for {table}; run the bronze stage first")
    # Folders sort by ingest date, then by run id (which starts with a timestamp).
    latest = max(k.rsplit("/", 1)[0] for k in files)
    keys = [k for k in files if k.rsplit("/", 1)[0] == latest]
    return pd.concat([pd.read_parquet(io.BytesIO(d)) for d in read_many(lake, keys)], ignore_index=True)


def parse_lms_files(lake: Store) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    """Parse all Bronze LMS files.

    Returns:
        ``(rows, malformed, lines)``: parsed events with ``_source_file`` and
        ``_line_no``, unparseable lines for quarantine, and the line count.
    """
    keys = [k for k in lake.list("bronze/lms/learning_activity") if k.endswith(".jsonl")]
    records: list[dict] = []
    malformed: list[dict] = []
    lines = 0
    unknown: dict[str, int] = {}
    for key, data in zip(keys, read_many(lake, keys)):
        source_file = key.removeprefix("bronze/lms/learning_activity/")
        for line_no, line in enumerate(data.decode("utf-8", errors="replace").splitlines(), 1):
            if not line.strip():
                continue
            lines += 1
            try:
                record = json.loads(line)
                if not isinstance(record, dict):
                    raise ValueError("line is not a JSON object")
            except ValueError as exc:
                malformed.append({"_rule_id": "learning_activity.JSON_PARSE", "_reason": f"malformed JSON: {exc}",
                                  "_raw": line, "_source_file": source_file, "_line_no": line_no})
                continue
            for name in record.keys() - LMS_KNOWN_FIELDS:
                unknown[name] = unknown.get(name, 0) + 1
            record["_source_file"] = source_file
            record["_line_no"] = line_no
            records.append(record)
    if unknown:
        log(logging.WARNING, "lms_unknown_fields_dropped", fields=unknown)

    rows = pd.DataFrame(records).reindex(columns=sorted(LMS_KNOWN_FIELDS) + ["_source_file", "_line_no"])
    rows = rows.astype({c: "object" for c in LMS_KNOWN_FIELDS})
    malformed_df = pd.DataFrame(malformed, columns=["_rule_id", "_reason", "_raw", "_source_file", "_line_no"])
    return rows, malformed_df, lines


def transform_hr(lake: Store, ctx: RunContext, keys: dict[str, set]) -> tuple[dict, dict, dict]:
    """Validate and conform every HR table, parents first."""
    silver: dict[str, pd.DataFrame] = {}
    quarantine: dict[str, pd.DataFrame] = {}
    stats: dict[str, dict] = {}
    for table in HR_TABLES:
        raw = latest_hr_snapshot(lake, table)
        contract = SILVER_CONTRACTS[table]
        valid, rejected, _ = validate(raw, table, keys)
        pct = check_reject_rate(table, len(raw), len(rejected), ctx.config.max_reject_pct)
        target = contract["target"]
        for col in contract["pk"]:
            keys[f"{target}.{col}"] = set(valid[col].dropna().tolist())
        silver[target] = valid
        quarantine[table] = quarantine_frame(rejected, ctx.run_id)
        stats[table] = {"read": len(raw), "quarantined": len(rejected), "reject_pct": pct, "written": len(valid)}
        ctx.add_lineage(f"bronze/hr/{table}", f"silver/{target}", "validate, cast, rename")
    return silver, quarantine, stats


def enrich_hr(silver: dict[str, pd.DataFrame], as_of: date) -> dict[str, int]:
    """Add derived columns and warning flags to the HR Silver tables."""
    employees = silver["dim_employee"]
    employees["email"] = employees["email"].str.lower()
    employees["full_name"] = employees["first_name"] + " " + employees["last_name"]
    employees["job_level_rank"] = employees["job_level"].map(JOB_LEVEL_RANK).astype("Int64")
    employees["tenure_years"] = ((pd.Timestamp(as_of) - employees["hire_date"]).dt.days / 365.25).round(1)

    hire = employees.set_index("employee_id")["hire_date"]
    reviews = silver["fact_performance_review"]
    year = reviews["review_period"].str[:4].astype(int)
    first_half = reviews["review_period"].str.endswith("H1")
    reviews["period_start"] = pd.to_datetime(year.astype(str) + np.where(first_half, "-01-01", "-07-01"))
    reviews["period_end"] = pd.to_datetime(year.astype(str) + np.where(first_half, "-06-30", "-12-31"))
    reviews["dq_review_before_hire"] = (reviews["submitted_date"] < reviews["employee_id"].map(hire)).astype(bool)

    assessments = silver["fact_competency_assessment"]
    assessments["gap"] = (assessments["required_level"] - assessments["proficiency_level"]).astype("Int64")
    assessments["dq_assessed_before_hire"] = (
        assessments["assessed_date"] < assessments["employee_id"].map(hire)).astype(bool)

    warnings = {
        "dq_review_before_hire": int(reviews["dq_review_before_hire"].sum()),
        "dq_assessed_before_hire": int(assessments["dq_assessed_before_hire"].sum()),
    }
    for flag, count in warnings.items():
        if count:
            log(logging.WARNING, "dq_warning", flag=flag, rows=count)
    return warnings


def transform_lms(lake: Store, ctx: RunContext,
                  keys: dict[str, set]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    """Parse, validate, deduplicate and flag the LMS learning events.

    Returns:
        ``(events, quarantine, duplicates, stats)``; ``duplicates`` is the
        audit trail of dropped copies.
    """
    rows, malformed, lines = parse_lms_files(lake)

    rows["dq_schema_drift"] = (rows["text"].isna() & rows["notes"].notna()).astype(bool)
    rows["event_text"] = rows["text"].where(rows["text"].notna(), rows["notes"])
    valid, rejected, duplicates = validate(rows, "learning_activity", keys)
    duplicates["_run_id"] = ctx.run_id

    # Scores belong to quiz events only.
    stray_score = valid["score"].notna() & ~valid["event_type"].isin(QUIZ_EVENT_TYPES)
    if stray_score.any():
        bad = rows.loc[rows["event_id"].isin(valid.loc[stray_score, "event_id"])].copy()
        bad["_rule_id"] = "learning_activity.score.CONSISTENCY"
        bad["_reason"] = "score set on a non-quiz event"
        rejected = pd.concat([rejected, bad], ignore_index=True)
        valid = valid[~stray_score].reset_index(drop=True)

    valid["score"] = valid["score"].round().astype("Int64")
    valid["event_date"] = valid["event_ts"].dt.normalize()
    partition_date = pd.to_datetime(
        valid["_source_file"].str.extract(r"year=(\d{4})/month=(\d{2})/day=(\d{2})").fillna("")
        .agg("-".join, axis=1),
        errors="coerce")
    valid["dq_partition_mismatch"] = (partition_date != valid["event_date"]).astype(bool)
    valid["dq_quiz_passed_low_score"] = (
        (valid["event_type"] == "quiz_passed") & (valid["score"] < QUIZ_PASS_MARK)).fillna(False).astype(bool)

    quarantine = pd.concat([malformed, quarantine_frame(rejected, ctx.run_id)], ignore_index=True)
    quarantine["_run_id"] = ctx.run_id
    parsed = len(rows)
    pct = check_reject_rate("learning_activity", lines, len(quarantine), ctx.config.max_reject_pct)

    warnings = {flag: int(valid[flag].sum())
                for flag in ["dq_schema_drift", "dq_partition_mismatch", "dq_quiz_passed_low_score"]}
    for flag, count in warnings.items():
        if count:
            log(logging.WARNING, "dq_warning", flag=flag, rows=count)

    stats = {
        "lines": lines,
        "parsed": parsed,
        "malformed": len(malformed),
        "quarantined": len(quarantine),
        "quarantined_by_rule": quarantine["_rule_id"].value_counts().to_dict(),
        "deduplicated": len(duplicates),
        "reject_pct": pct,
        "written": len(valid),
        "warnings": warnings,
    }
    ctx.add_lineage("bronze/lms/learning_activity", "silver/fact_learning_event",
                    "parse JSON, coalesce text/notes, validate, dedup on event_id")
    ctx.add_lineage("bronze/lms/learning_activity", "quarantine/learning_activity_duplicates",
                    "dropped duplicate copies (audit)")
    return valid, quarantine, duplicates, stats


@timed_stage("silver")
def transform_to_silver(ctx: RunContext) -> dict:
    """Build the Silver layer from the latest Bronze data."""
    lake, run_id = ctx.lake, ctx.run_id
    keys: dict[str, set] = {}

    silver, quarantine, stats = transform_hr(lake, ctx, keys)
    hr_warnings = enrich_hr(silver, ctx.config.as_of_date)
    events, lms_quarantine, lms_duplicates, stats["learning_activity"] = transform_lms(lake, ctx, keys)
    quarantine["learning_activity"] = lms_quarantine
    quarantine["learning_activity_duplicates"] = lms_duplicates

    # All validation passed: publish Silver and the quarantine snapshot.
    for target, df in silver.items():
        write_table(lake, f"silver/{target}", df.assign(_run_id=run_id), run_id)
    events = events.assign(_run_id=run_id)
    parts = {
        f"event_year={y:04d}/event_month={m:02d}/part-0000.parquet": to_parquet_bytes(group)
        for (y, m), group in events.groupby([events["event_ts"].dt.year, events["event_ts"].dt.month])
    }
    replace_prefix(lake, "silver/fact_learning_event", parts, run_id)
    for dataset, df in quarantine.items():
        if df.empty:
            lake.delete_prefix(f"quarantine/{dataset}")
        else:
            write_table(lake, f"quarantine/{dataset}", df, run_id)

    for table, counts in stats.items():
        log(logging.INFO, "silver_dataset_written", dataset=table,
            **{k: v for k, v in counts.items() if not isinstance(v, dict)})
    stats["hr_warnings"] = hr_warnings
    write_json(lake, "_meta/latest/silver_stats.json", {"run_id": run_id, "datasets": stats})
    return stats
