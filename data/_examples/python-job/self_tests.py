"""Built-in self-tests, run with ``python job.py --self-test``."""

from __future__ import annotations

import contextlib
import io
import json
import logging
import os
import tempfile
import time
import uuid
from datetime import date, datetime, timezone
from typing import Any, Callable, Iterator

import pandas as pd

from settings import HR_TABLES, LMS_KNOWN_FIELDS, SILVER_CONTRACTS
from common import Config, LOGGER, PipelineError, explain_aws_error, load_config, log, retry
from storage import LocalStore, S3Store, read_table, replace_prefix, write_table
from run_context import RunContext
from bronze import connect_postgres, extract_table, ingest_lms_files, verify_bronze_objects, write_hr_snapshot
from silver import _cast, check_reject_rate, latest_hr_snapshot, transform_to_silver, validate
from gold import GOLD_GRAIN, aggregate_to_gold
from serve import check_latest_gold_run, pg_rows, pg_type, publish_tables

# ---------------------------------------------------------------------------
# Self-tests (python job.py --self-test)
# ---------------------------------------------------------------------------
#
# Unit tests for the validation rules, transforms and Gold metrics, an
# end-to-end smoke test on a small fixture with hand-computed expected values,
# and optional integration tests against PostgreSQL (when POSTGRES_PASS is set)
# (extract, and publishing to throwaway schemas), and S3 (when SELF_TEST_S3_URI
# is set, e.g. a LocalStack bucket).

SELF_TESTS: list[tuple[str, str, Callable[[], None]]] = []


def self_test(category: str) -> Callable[[Callable[[], None]], Callable[[], None]]:
    """Register a self-test function under a category."""

    def decorator(func: Callable[[], None]) -> Callable[[], None]:
        SELF_TESTS.append((category, func.__name__.removeprefix("test_"), func))
        return func

    return decorator


class SkipTest(Exception):
    """Raised by a self-test whose prerequisites are not available."""


def expect(actual: Any, expected: Any, what: str) -> None:
    """Fail the current test unless ``actual == expected``."""
    if isinstance(expected, float) and actual is not None and not pd.isna(actual):
        ok = abs(float(actual) - expected) < 1e-6
    else:
        ok = actual == expected
    if not ok:
        raise AssertionError(f"{what}: expected {expected!r}, got {actual!r}")


@contextlib.contextmanager
def patched_env(**values: str | None) -> Iterator[None]:
    """Temporarily set (or with ``None``, unset) environment variables."""
    saved = {k: os.environ.get(k) for k in values}
    try:
        for key, value in values.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        yield
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _rules(rejected: pd.DataFrame) -> dict[str, str]:
    """Map the first data column of each rejected row to its rule id."""
    return dict(zip(rejected.iloc[:, 0].astype(str), rejected["_rule_id"]))


def _lms_frame(rows: list[dict]) -> pd.DataFrame:
    """Build raw LMS rows the way parse_lms_files does."""
    df = pd.DataFrame(rows).reindex(columns=sorted(LMS_KNOWN_FIELDS) + ["_source_file", "_line_no"])
    df = df.astype({c: "object" for c in LMS_KNOWN_FIELDS})
    df["event_text"] = df["text"]
    return df


EMPLOYEE_KEYS = {"dim_employee.employee_id": {1, 3}}


def _event(event_id: str, employee_id: Any = "1", event_date: Any = "2026-09-01T10:00:00",
           event_type: str = "enrollment", score: Any = None) -> dict:
    """Build one raw LMS event; tests override only the field they want to break."""
    return {"event_id": event_id, "employee_id": employee_id, "event_date": event_date,
            "event_type": event_type, "course_name": "Course X", "text": "t", "score": score}


# --- Unit: casting and validation rules ------------------------------------


@self_test("unit")
def test_cast_types() -> None:
    """Invalid integers, dates and timestamps become null; valid values convert."""
    raw = pd.Series(["7", "7.5", "abc", None], dtype="object")
    expect(_cast(raw, "int").tolist()[:1], [7], "int cast")
    expect(_cast(raw, "int").isna().tolist(), [False, True, True, True], "non-integers become null")
    expect(_cast(pd.Series(["56.87"]), "decimal")[0], 56.87, "decimal cast")
    dates = _cast(pd.Series(["2025-08-31", "2026-13-45", "yesterday"]), "date")
    expect(dates.isna().tolist(), [False, True, True], "strict date parsing")
    stamps = _cast(pd.Series(["2026-02-10T16:56:00", "2026-02-10", "N/A"]), "timestamp")
    expect(stamps.isna().tolist(), [False, True, True], "strict timestamp parsing")


@self_test("unit")
def test_required_and_type_rules() -> None:
    """Null/empty values and unparseable types are rejected with the right rule."""
    raw = _lms_frame([_event("A"), _event("B", employee_id=None), _event("C", event_date="yesterday"),
                      _event("D", event_date=""), _event("E", event_date="2026-13-45"),
                      _event("F", employee_id="abc")])
    valid, rejected, _ = validate(raw, "learning_activity", EMPLOYEE_KEYS)
    expect(valid["event_id"].tolist(), ["A"], "only the clean row survives")
    expect(_rules(rejected[["event_id", "_rule_id"]]), {
        "B": "learning_activity.employee_id.REQUIRED",
        "C": "learning_activity.event_date.TYPE",
        "D": "learning_activity.event_date.REQUIRED",
        "E": "learning_activity.event_date.TYPE",
        "F": "learning_activity.employee_id.TYPE",
    }, "rule per rejected row")
    expect(str(valid["event_ts"].dtype).startswith("datetime64"), True, "event_ts is a timestamp")
    expect(str(valid["employee_id"].dtype), "Int64", "employee_id is an integer")


@self_test("unit")
def test_domain_range_fk_rules() -> None:
    """Unknown event types, out-of-range scores and unknown employees are rejected."""
    raw = _lms_frame([_event("A"), _event("B", event_type="lesson_viewed"),
                      _event("C", event_type="quiz_passed", score=150), _event("D", employee_id="9")])
    _, rejected, _ = validate(raw, "learning_activity", EMPLOYEE_KEYS)
    expect(_rules(rejected[["event_id", "_rule_id"]]), {
        "B": "learning_activity.event_type.DOMAIN",
        "C": "learning_activity.score.RANGE",
        "D": "learning_activity.employee_id.FK",
    }, "rule per rejected row")


@self_test("unit")
def test_first_failing_rule_wins() -> None:
    """A row with several problems is rejected once, with its first failing rule."""
    raw = _lms_frame([_event("A", employee_id=None, event_date="yesterday")])
    _, rejected, _ = validate(raw, "learning_activity", EMPLOYEE_KEYS)
    expect(rejected["_rule_id"].tolist(), ["learning_activity.employee_id.REQUIRED"], "one rule per row")


@self_test("unit")
def test_dedup_keeps_first_valid_copy() -> None:
    """Duplicates keep the first valid copy and are recorded in the audit trail."""
    raw = _lms_frame([_event("A", employee_id=None), _event("A"), _event("A", event_type="module_started"),
                      _event("B"), _event("B")])
    raw["_line_no"] = range(1, 6)
    valid, rejected, duplicates = validate(raw, "learning_activity", EMPLOYEE_KEYS)
    expect(sorted(valid["event_id"]), ["A", "B"], "one row per event_id")
    expect(len(duplicates), 2, "duplicates dropped")
    audit = duplicates.set_index("event_id")
    expect(int(audit.loc["A", "_line_no"]), 3, "dropped copy location recorded")
    expect(int(audit.loc["A", "kept_line_no"]), 2, "kept copy location recorded")
    expect(int(audit.loc["B", "kept_line_no"]), 4, "kept copy for the second key")
    expect(audit.loc["A", "event_type"], "module_started", "dropped copy content recorded")
    expect(len(rejected), 1, "the invalid copy is quarantined, not counted as a duplicate")
    expect(int(valid.loc[valid["event_id"] == "A", "_line_no"].iloc[0]), 2, "first valid copy kept")


@self_test("unit")
def test_hr_duplicate_keys_rejected() -> None:
    """HR duplicate keys are rejected (not silently dropped), as are bad patterns."""
    raw = pd.DataFrame({
        "review_id": ["1", "1", "2", "3"],
        "employee_id": ["1", "1", "1", "3"],
        "review_period": ["2024-H2", "2024-H2", "2024-H2", "2025-H3"],
        "rating": ["3", "3", "4", "6"],
        "manager_comment": [None, None, "ok", None],
        "submitted_date": ["2025-01-01"] * 4,
    }, dtype="object")
    valid, rejected, _ = validate(raw, "performance_reviews", EMPLOYEE_KEYS)
    expect(valid["review_id"].tolist(), [1], "only the first review survives")
    expect(sorted(rejected["_rule_id"]), sorted([
        "performance_reviews.review_id.PK_DUPLICATE",
        "performance_reviews.employee_id_review_period.UNIQUE_DUPLICATE",
        "performance_reviews.review_period.PATTERN",
    ]), "PK, unique and pattern rules")


@self_test("unit")
def test_missing_column_is_fatal() -> None:
    """A missing contract column stops validation with a clear error."""
    try:
        validate(pd.DataFrame({"team_id": ["1"], "name": ["A"]}), "teams", {})
    except PipelineError as exc:
        expect("department" in str(exc), True, "error names the missing column")
        return
    raise AssertionError("missing column did not raise PipelineError")


@self_test("unit")
def test_reject_rate_threshold() -> None:
    """The rejection limit allows exactly the threshold and stops above it."""
    expect(check_reject_rate("x", 1000, 50, 5.0), 5.0, "exactly at the limit passes")
    try:
        check_reject_rate("x", 1000, 51, 5.0)
    except PipelineError:
        return
    raise AssertionError("reject rate above limit did not raise")


@self_test("unit")
def test_retry_backoff() -> None:
    """Transient errors are retried; non-transient errors fail on the first attempt."""
    calls = {"n": 0}

    def flaky() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectionError("temporary")
        return "ok"

    expect(retry(flaky, "flaky", base_delay=0), "ok", "succeeds after transient failures")
    expect(calls["n"], 3, "attempts made")

    calls["n"] = 0

    def denied() -> None:
        calls["n"] += 1
        raise PermissionError("authentication failed")

    try:
        retry(denied, "denied", base_delay=0, is_transient=lambda e: "authentication" not in str(e))
    except PermissionError:
        expect(calls["n"], 1, "non-transient error is not retried")
        return
    raise AssertionError("non-transient error was swallowed")


@self_test("unit")
def test_config_from_env() -> None:
    """Cloud mode lists missing settings and derives S3 defaults from the bucket."""
    with patched_env(IS_LOCAL="false", POSTGRES_NAME=None, POSTGRES_USER=None, POSTGRES_PASS=None,
                     AWS_S3_BUCKET=None, LAKE_ROOT=None, LMS_SOURCE_PATH=None):
        try:
            load_config()
            raise AssertionError("cloud mode without settings did not raise")
        except PipelineError as exc:
            expect("POSTGRES_PASS" in str(exc), True, "missing variables are listed")
    with patched_env(IS_LOCAL="false", POSTGRES_NAME="db", POSTGRES_USER="u", POSTGRES_PASS="p",
                     AWS_S3_BUCKET="bucket", LAKE_ROOT=None, LMS_SOURCE_PATH=None, AS_OF_DATE="2026-01-31"):
        config = load_config()
        expect(config.lake_root, "s3://bucket/lake", "cloud lake defaults to the bucket")
        expect(config.lms_source, "s3://bucket/landing/learning_activity", "cloud LMS source")
        expect(config.as_of_date, date(2026, 1, 31), "AS_OF_DATE is honoured")


@self_test("unit")
def test_store_replace_is_idempotent() -> None:
    """Replacing a table removes old files and leaves sibling prefixes alone."""
    with tempfile.TemporaryDirectory() as tmp:
        store = LocalStore(tmp)
        replace_prefix(store, "silver/t", {"a.parquet": b"1", "b.parquet": b"2"}, "run1")
        replace_prefix(store, "silver/t", {"a.parquet": b"3"}, "run2")
        expect(store.list("silver/t"), ["silver/t/a.parquet"], "old files removed")
        expect(store.read_bytes("silver/t/a.parquet"), b"3", "new content written")
        replace_prefix(store, "silver/t2", {"x": b"0"}, "run3")
        expect(store.list("silver/t"), ["silver/t/a.parquet"], "sibling prefix untouched")


# --- End-to-end smoke test on a fixture ------------------------------------

FIXTURE_HR = {
    "teams": [["1", "Team A", "Engineering"], ["2", "Team B", "Sales"]],
    "competencies": [["1", "Python", "Technical"], ["2", "Coaching", "Leadership"]],
    "employees": [
        ["1", "Ann", "Lee", "Ann.Lee@x.example", "1", "Manager", "2020-01-01"],
        ["2", "Bob", "Ray", "bob.ray@x.example", "1", "IC1", "2026-06-01"],
        ["3", "Cy", "Doe", "cy.doe@x.example", "2", "Director", "2019-05-05"],
    ],
    "projects": [["1", "P1", "1", "on-time", "80.00"], ["2", "P2", "1", "delayed", "60.00"],
                 ["3", "P3", "2", "on-time", "90.00"]],
    "performance_reviews": [
        ["1", "1", "2024-H2", "5", "great", "2025-01-15"], ["2", "1", "2025-H1", "4", None, "2025-08-01"],
        ["3", "2", "2024-H2", "4", None, "2025-01-15"], ["4", "2", "2025-H1", "2", None, "2025-08-01"],
        ["5", "3", "2024-H2", "3", None, "2025-01-15"], ["6", "3", "2025-H1", "4", None, "2025-08-01"],
    ],
    "competency_assessments": [
        ["1", "1", "1", "2", "4", "2025-10-01"], ["2", "1", "2", "3", "3", "2025-10-01"],
        ["3", "2", "1", "3", "4", "2026-07-01"], ["4", "3", "2", "5", "4", "2025-10-01"],
    ],
    "talent_review": [["1", "2025-H1", "High", "Ready Now", "High"], ["2", "2025-H1", "Medium", "Not Ready", "Low"],
                      ["3", "2025-H1", "High", "Ready Now", "Low"]],
}
FIXTURE_COLUMNS = {table: list(SILVER_CONTRACTS[table]["columns"]) for table in HR_TABLES}
FIXTURE_LMS = "\n".join([
    json.dumps(_event("E1", "1", event_type="enrollment")),
    json.dumps(_event("E2", "1", event_type="course_completed")),
    json.dumps(_event("E3", "1", event_type="quiz_passed", score=50)),
    json.dumps(_event("E3", "1", event_type="quiz_passed", score=50)),
    json.dumps(_event("E4", None)),
    json.dumps(_event("E5", "1", event_date="yesterday")),
    json.dumps({**{k: v for k, v in _event("E6", "3").items() if k != "text"}, "notes": "drifted"}),
    json.dumps(_event("E7", "9")),
    '{"event_id": "E8", "employee_id"',
    "",
    json.dumps(_event("E9", "3", event_type="module_started", score=70)),
]) + "\n"


def _fixture_context(tmp: str) -> RunContext:
    """Write the fixture into a Bronze layer under ``tmp`` and return a context."""
    config = Config(is_local=True, postgres_host="", postgres_port=5432, postgres_name="", postgres_user="",
                    postgres_pass="", lake_root=tmp, lms_source="", hr_seed_source="", as_of_date=date(2026, 10, 14),
                    max_reject_pct=100.0, batch_size=100, log_level="WARNING")
    lake = LocalStore(tmp)
    for table, rows in FIXTURE_HR.items():
        df = pd.DataFrame(rows, columns=FIXTURE_COLUMNS[table], dtype="object")
        write_table(lake, f"bronze/hr/{table}/ingest_date=2026-10-14", df, "fixture")
    lake.write_bytes("bronze/lms/learning_activity/year=2026/month=09/day=01/events-000.jsonl", FIXTURE_LMS.encode())
    return RunContext(config=config, lake=lake)


@self_test("e2e")
def test_silver_on_fixture() -> None:
    """Silver on the fixture: exact counts, rules, flags, partitions and quarantine."""
    with tempfile.TemporaryDirectory() as tmp:
        ctx = _fixture_context(tmp)
        stats = transform_to_silver(ctx)
        lms = stats["learning_activity"]
        stage = ctx.stages["silver"]
        expect(stage["records"], 10 + sum(len(rows) for rows in FIXTURE_HR.values()), "records counted")
        expect(stage["records_per_min"] > 0 and stage["peak_rss_mb"] > 0, True, "throughput and memory logged")
        expect((lms["lines"], lms["parsed"], lms["malformed"]), (10, 9, 1), "lines / parsed / malformed")
        expect((lms["written"], lms["quarantined"], lms["deduplicated"]), (4, 5, 1), "written / quarantined / dedup")
        expect(lms["quarantined_by_rule"], {
            "learning_activity.JSON_PARSE": 1,
            "learning_activity.employee_id.REQUIRED": 1,
            "learning_activity.event_date.TYPE": 1,
            "learning_activity.employee_id.FK": 1,
            "learning_activity.score.CONSISTENCY": 1,
        }, "quarantine by rule")
        expect(lms["warnings"]["dq_schema_drift"], 1, "notes->text drift flagged")
        audit = read_table(ctx.lake, "quarantine/learning_activity_duplicates")
        expect(audit[["event_id", "_line_no", "kept_line_no"]].values.tolist(), [["E3", 4, 3]],
               "duplicate audit trail")
        expect(lms["warnings"]["dq_quiz_passed_low_score"], 1, "low quiz_passed score flagged")
        expect(stats["hr_warnings"]["dq_review_before_hire"], 2, "reviews before hire flagged")

        employees = read_table(ctx.lake, "silver/dim_employee").set_index("employee_id")
        expect(employees.loc[1, "email"], "ann.lee@x.example", "email lowercased")
        expect(int(employees.loc[1, "job_level_rank"]), 5, "job level rank")
        expect(float(employees.loc[2, "tenure_years"]), 0.4, "tenure in years")
        events = read_table(ctx.lake, "silver/fact_learning_event").set_index("event_id")
        expect(events.loc["E6", "event_text"], "drifted", "text coalesced from notes")
        expect(ctx.lake.list("silver/fact_learning_event"),
               ["silver/fact_learning_event/event_year=2026/event_month=09/part-0000.parquet"], "partition layout")
        quarantine = read_table(ctx.lake, "quarantine/learning_activity")
        expect(set(quarantine.columns) >= {"_rule_id", "_reason", "_raw", "_source_file", "_line_no", "_run_id"},
               True, "quarantine keeps raw row and location")


@self_test("e2e")
def test_gold_on_fixture() -> None:
    """Gold on the fixture: hand-computed metrics, risk scores and reconciliation."""
    with tempfile.TemporaryDirectory() as tmp:
        ctx = _fixture_context(tmp)
        transform_to_silver(ctx)
        stats = aggregate_to_gold(ctx)
        expect(all(c["passed"] for c in ctx.checks), True,
               "reconciliation: " + str([c for c in ctx.checks if not c["passed"]]))
        gold = {name: read_table(ctx.lake, f"gold/{name}").set_index(GOLD_GRAIN[name][0])
                for name in ["employee_skill_gaps", "attrition_risk", "promotion_candidates",
                             "employee_learning_summary", "team_performance_outcomes"]}
        expect(stats["employee_performance_history"]["written"], 6, "one row per employee per period")

        gaps = gold["employee_skill_gaps"]["gap_severity"].to_dict()
        expect(gaps, {1: "critical", 2: "none", 3: "moderate", 4: "none"}, "gap severity")

        risk = gold["attrition_risk"]
        expect(risk["rating_trend"].to_dict(), {1: "declining", 2: "declining", 3: "improving"}, "rating trend")
        # 1: HR High 40 + drop of 1 15 = 55; 2: drop of 2 25 + rating 2 15 + no learning 10 + new hire 10 = 60
        expect(risk["computed_risk_score"].to_dict(), {1: 55, 2: 60, 3: 0}, "risk score")
        expect(risk["risk_band"].to_dict(), {1: "Medium", 2: "High", 3: "Low"}, "risk band")

        candidates = gold["promotion_candidates"]
        expect(sorted(candidates.index), [1, 3], "High potential + Ready Now")
        expect(candidates["is_recommended"].to_dict(), {1: False, 3: True}, "critical gap blocks recommendation")

        learning = gold["employee_learning_summary"]
        expect(learning["total_events"].to_dict(), {1: 3, 2: 0, 3: 1}, "events per employee")
        expect(learning.loc[1, "completion_rate"], 1.0, "completion rate")
        expect(pd.isna(learning.loc[2, "completion_rate"]), True, "no enrollments -> no completion rate")
        expect(learning.loc[1, "avg_quiz_score"], 50.0, "average quiz score")

        team = gold["team_performance_outcomes"]
        expect(team.loc[1, ["headcount", "project_count"]].tolist(), [2, 2], "team 1 counts")
        expect(team.loc[1, ["avg_rating_latest", "pct_high_performers", "on_time_pct", "avg_outcome_score"]]
               .astype(float).tolist(), [3.0, 50.0, 50.0, 70.0], "team 1 metrics")
        expect(team.loc[2, ["avg_rating_latest", "on_time_pct", "avg_outcome_score"]].astype(float).tolist(),
               [4.0, 100.0, 90.0], "team 2 metrics")


@self_test("e2e")
def test_rerun_is_idempotent() -> None:
    """Rerunning Silver and Gold produces the same files and row counts."""
    with tempfile.TemporaryDirectory() as tmp:
        ctx = _fixture_context(tmp)

        def snapshot() -> dict[str, int]:
            return {k: len(pd.read_parquet(io.BytesIO(ctx.lake.read_bytes(k))))
                    for k in ctx.lake.list("") if k.endswith(".parquet") and not k.startswith("bronze/")}

        transform_to_silver(ctx)
        aggregate_to_gold(ctx)
        first = snapshot()
        transform_to_silver(ctx)
        aggregate_to_gold(ctx)
        expect(snapshot(), first, "same files and row counts after a rerun")


@self_test("unit")
def test_bronze_lms_verified_incremental_immutable() -> None:
    """LMS files are verified after copy, skipped when unchanged, and history is kept on change."""
    with tempfile.TemporaryDirectory() as tmp, tempfile.TemporaryDirectory() as src:
        ctx = _fixture_context(tmp)
        source = LocalStore(src)
        source.write_bytes("year=2026/month=09/day=01/events-000.jsonl", b'{"a": 1}\n')
        source.write_bytes("year=2026/month=09/day=02/events-000.jsonl", b'{"b": 2}\n')
        ts = datetime(2026, 10, 14, tzinfo=timezone.utc)
        first = ingest_lms_files(ctx.lake, source, ctx, ts)
        expect((first["files_copied"], first["files_verified"], first["lines_copied"]), (2, 2, 2), "first copy")
        second = ingest_lms_files(ctx.lake, source, ctx, ts)
        expect((second["files_copied"], second["files_skipped"]), (0, 2), "unchanged files skipped")
        source.write_bytes("year=2026/month=09/day=01/events-000.jsonl", b'{"a": 1}\n{"a": 2}\n')
        third = ingest_lms_files(ctx.lake, source, ctx, ts)
        expect((third["files_copied"], third["files_superseded"]), (1, 1), "changed file re-copied")
        current = "bronze/lms/learning_activity/year=2026/month=09/day=01/events-000.jsonl"
        expect(ctx.lake.read_bytes(current), b'{"a": 1}\n{"a": 2}\n', "Bronze holds the new version")
        old = [k for k in ctx.lake.list("bronze/lms/_superseded") if k.endswith("events-000.jsonl")]
        expect(len(old), 1, "previous version preserved")
        expect(ctx.lake.read_bytes(old[0]), b'{"a": 1}\n', "previous version unchanged")


@self_test("unit")
def test_bronze_verification_detects_corruption() -> None:
    """Verification fails when a stored object differs from the bytes that were sent."""
    with tempfile.TemporaryDirectory() as tmp:
        store = LocalStore(tmp)
        store.write_bytes("bronze/x.jsonl", b"stored")
        expect(verify_bronze_objects(store, {"bronze/x.jsonl": b"stored"}), 1, "matching object verifies")
        try:
            verify_bronze_objects(store, {"bronze/x.jsonl": b"sent"})
        except PipelineError:
            return
        raise AssertionError("corrupted object was not detected")


@self_test("unit")
def test_bronze_hr_snapshots_immutable() -> None:
    """HR snapshots are never overwritten, are skipped when unchanged, and the newest one is read."""
    with tempfile.TemporaryDirectory() as tmp:
        ctx = _fixture_context(tmp)
        before = len([k for k in ctx.lake.list("bronze/hr/teams") if k.endswith(".parquet")])
        df = pd.DataFrame({"team_id": ["1"], "name": ["A"], "department": ["X"]}, dtype="object")
        ts = datetime(2026, 10, 15, tzinfo=timezone.utc)
        # Fixed run ids (they start with a timestamp): a clock-based id would make the order depend on today's date.
        ctx.run_id = "20261015T000000Z-aaaaaa"
        expect(write_hr_snapshot(ctx.lake, ctx, "teams", df, ts), True, "first snapshot written")
        expect(write_hr_snapshot(ctx.lake, ctx, "teams", df, ts), False, "unchanged content skipped")
        changed = df.assign(name=["B"])
        ctx.run_id = "20261015T000001Z-zzzzzz"
        expect(write_hr_snapshot(ctx.lake, ctx, "teams", changed, ts), True, "changed content written")
        after = len([k for k in ctx.lake.list("bronze/hr/teams") if k.endswith(".parquet")])
        expect(after - before, 2, "earlier snapshots kept")
        expect(latest_hr_snapshot(ctx.lake, "teams")["name"].tolist(), ["B"], "newest snapshot is read")


@self_test("unit")
def test_aws_error_hints() -> None:
    """S3 credential and permission errors get an actionable hint, including when wrapped."""
    from botocore.exceptions import ClientError, NoCredentialsError

    expect("instance metadata" in (explain_aws_error(NoCredentialsError()) or ""), True, "no credentials")
    denied = ClientError({"Error": {"Code": "AccessDenied", "Message": "x"}}, "PutObject")
    expect("eks_node_lake" in (explain_aws_error(denied) or ""), True, "access denied")
    try:
        try:
            raise denied
        except ClientError as exc:
            raise PipelineError("write failed") from exc
    except PipelineError as wrapped:
        expect(explain_aws_error(wrapped) is not None, True, "hint found through the cause chain")
    expect(explain_aws_error(ValueError("other")), None, "other errors get no hint")


# --- Integration (optional) ------------------------------------------------


@self_test("integration")
def test_postgres_extract() -> None:
    """Integration: every HR table extracts with rows, contract columns and unique keys."""
    if not os.environ.get("POSTGRES_PASS"):
        raise SkipTest("POSTGRES_PASS not set")
    config = load_config()
    with connect_postgres(config) as conn:
        for table in HR_TABLES:
            df = extract_table(conn, table, HR_TABLES[table], batch_size=500)
            expect(len(df) > 0, True, f"{table} has rows")
            missing = set(SILVER_CONTRACTS[table]["columns"]) - set(df.columns)
            expect(missing, set(), f"{table} has every contract column")
            expect(df[HR_TABLES[table]].duplicated().sum(), 0, f"{table} extract has unique keys")


@self_test("integration")
def test_s3_round_trip() -> None:
    """Integration: write, replace, list, read and delete against a real S3 bucket."""
    uri = os.environ.get("SELF_TEST_S3_URI")
    if not uri:
        raise SkipTest("SELF_TEST_S3_URI not set")
    prefix = f"self-test-{uuid.uuid4().hex[:8]}"
    store = S3Store(uri)
    try:
        frame = pd.DataFrame({"a": [1, 2, 3]})
        write_table(store, f"{prefix}/t", frame, "selftest")
        write_table(store, f"{prefix}/t", frame.head(2), "selftest")
        expect(store.list(f"{prefix}/t"), [f"{prefix}/t/part-0000.parquet"], "one file after replace")
        expect(len(read_table(store, f"{prefix}/t")), 2, "replaced content read back")
    finally:
        store.delete_prefix(prefix)
        store.delete_prefix("_staging/selftest")


@self_test("unit")
def test_serve_type_mapping() -> None:
    """Lake columns map to PostgreSQL types, and missing values become NULL."""
    df = pd.DataFrame({
        "employee_id": pd.array([1, None], dtype="Int64"),
        "score": [85.5, float("nan")],
        "is_recommended": [True, False],
        "hire_date": pd.to_datetime(["2024-04-06", None]),
        "event_ts": pd.to_datetime(["2026-09-01 10:00:00", "2026-09-01 00:00:00"]),
        "name": ["Ann", None],
    })
    types = [pg_type(df[c]) for c in df.columns]
    expect(types, ["bigint", "double precision", "boolean", "date", "timestamp", "text"], "column types")
    rows = pg_rows(df, types)
    expect(rows[0], (1, 85.5, True, date(2024, 4, 6), datetime(2026, 9, 1, 10, 0), "Ann"), "first row values")
    expect(rows[1][0], None, "missing Int64 is NULL")
    expect(rows[1][1], None, "NaN is NULL")
    expect(rows[1][3], None, "missing date is NULL")
    expect(rows[1][5], None, "missing text is NULL, not 'None'")


@self_test("unit")
def test_serve_refuses_unverified_gold() -> None:
    """Serving on its own publishes only Gold from a run that passed every reconciliation check."""
    with tempfile.TemporaryDirectory() as tmp:
        store = LocalStore(tmp)
        try:
            check_latest_gold_run(store)
            raise AssertionError("serving with no Gold run did not fail")
        except PipelineError:
            pass
        good = {"run_id": "20261015T000000Z-aaaaaa", "status": "SUCCESS", "stages": {"gold": {}},
                "checks": [{"check": "c", "passed": True}]}
        store.write_bytes("_meta/runs/20261015T000000Z-aaaaaa.json", json.dumps(good).encode())
        expect(check_latest_gold_run(store), "20261015T000000Z-aaaaaa", "passing Gold run is accepted")
        serve_only = {"run_id": "20261015T000001Z-bbbbbb", "status": "SUCCESS", "stages": {"serve": {}}, "checks": []}
        store.write_bytes("_meta/runs/20261015T000001Z-bbbbbb.json", json.dumps(serve_only).encode())
        expect(check_latest_gold_run(store), "20261015T000000Z-aaaaaa", "runs without Gold are skipped")
        bad = {"run_id": "20261015T000002Z-cccccc", "status": "FAILED", "stages": {"gold": {}},
               "checks": [{"check": "c", "passed": False}]}
        store.write_bytes("_meta/runs/20261015T000002Z-cccccc.json", json.dumps(bad).encode())
        try:
            check_latest_gold_run(store)
            raise AssertionError("Gold from a failed run was accepted")
        except PipelineError as exc:
            expect("did not pass reconciliation" in str(exc), True, "failed Gold run is refused")


@self_test("integration")
def test_serve_publish_is_atomic() -> None:
    """Integration: publish to PostgreSQL, read back, and a failed publish keeps the previous tables."""
    if not os.environ.get("POSTGRES_PASS"):
        raise SkipTest("POSTGRES_PASS not set")
    schema = f"selftest_{uuid.uuid4().hex[:8]}"
    with connect_postgres(load_config()) as conn:
        try:
            frame = pd.DataFrame({"id": pd.array([1, 2, 3], dtype="Int64"), "name": ["a", "b", None]})
            stats = publish_tables(conn, {schema: {"t": frame}}, "run-1")
            expect(stats, {f"{schema}.t": {"written": 3}}, "rows published")
            with conn.cursor() as cur:
                cur.execute(f'SELECT COUNT(*), COUNT(name) FROM "{schema}".t')
                expect(cur.fetchone(), (3, 2), "rows and NULLs read back")
            # Second publish: the first table is replaced, then the second fails (duplicate column).
            bad = pd.DataFrame([[1, 2]], columns=["x", "x"])
            try:
                publish_tables(conn, {schema: {"t": frame.head(1), "u": bad}}, "run-2")
                raise AssertionError("publishing an invalid table did not fail")
            except Exception as exc:  # noqa: BLE001 - any database error is the expected outcome
                if isinstance(exc, AssertionError):
                    raise
            with conn.cursor() as cur:
                cur.execute(f'SELECT COUNT(*) FROM "{schema}".t')
                expect(cur.fetchone()[0], 3, "failed publish rolled back; previous table intact")
            # A table whose lake data is gone (e.g. an empty quarantine) is dropped, not left stale.
            publish_tables(conn, {schema: {"t": None}}, "run-3")
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass(%s)", (f"{schema}.t",))
                expect(cur.fetchone()[0], None, "table dropped when the lake has no data")
        finally:
            conn.rollback()
            with conn.cursor() as cur:
                cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            conn.commit()


def run_self_tests() -> int:
    """Run every registered self-test and return a process exit code."""
    results = {"passed": 0, "failed": 0, "skipped": 0}
    started = time.perf_counter()
    for category, name, func in SELF_TESTS:
        start = time.perf_counter()
        level = LOGGER.level
        LOGGER.setLevel(logging.CRITICAL)  # keep pipeline logs from the fixture runs out of the report
        try:
            func()
            outcome, detail = "passed", None
        except SkipTest as exc:
            outcome, detail = "skipped", str(exc)
        except Exception as exc:  # noqa: BLE001 - reported as a test failure
            outcome, detail = "failed", f"{type(exc).__name__}: {exc}"
        finally:
            LOGGER.setLevel(level)
        results[outcome] += 1
        level = logging.ERROR if outcome == "failed" else logging.INFO
        log(level, "self_test", category=category, test=name, outcome=outcome,
            duration_s=round(time.perf_counter() - start, 3), **({"detail": detail} if detail else {}))
    log(logging.INFO if results["failed"] == 0 else logging.ERROR, "self_test_summary",
        total=len(SELF_TESTS), duration_s=round(time.perf_counter() - started, 2), **results)
    return 0 if results["failed"] == 0 else 1
