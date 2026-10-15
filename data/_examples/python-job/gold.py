"""Gold layer: one table per business question, plus reconciliation checks."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import numpy as np
import pandas as pd

from settings import (
    EVENT_TYPES, HR_TABLES, QUIZ_EVENT_TYPES, RISK_DROP_1_POINTS, RISK_DROP_2_POINTS, RISK_LABEL_POINTS,
    RISK_LOW_RATING_POINTS, RISK_NEW_HIRE_POINTS, RISK_NO_LEARNING_DAYS, RISK_NO_LEARNING_POINTS,
    SILVER_CONTRACTS,
)
from common import log
from storage import Store, read_json, read_table, write_table
from run_context import RunContext, timed_stage

# ---------------------------------------------------------------------------
# Gold
# ---------------------------------------------------------------------------


def build_gold(silver: dict[str, pd.DataFrame], as_of: date) -> dict[str, pd.DataFrame]:
    """Build one Gold table per business question from Silver tables."""
    teams = silver["dim_team"]
    employees = silver["dim_employee"].merge(teams, on="team_id", how="left")
    emp_cols = ["employee_id", "full_name", "team_id", "team_name", "department",
                "job_level", "job_level_rank", "hire_date", "tenure_years"]
    employees = employees[emp_cols]
    competencies = silver["dim_competency"]

    # Q1: ratings and review history.
    reviews = silver["fact_performance_review"].sort_values(["employee_id", "period_start"])
    reviews["rating"] = reviews["rating"].astype("float64")
    reviews["prev_rating"] = reviews.groupby("employee_id")["rating"].shift(1)
    reviews["rating_delta"] = reviews["rating"] - reviews["prev_rating"]
    performance_history = employees.merge(
        reviews[["employee_id", "review_id", "review_period", "period_start", "period_end", "rating",
                 "prev_rating", "rating_delta", "manager_comment", "submitted_date", "dq_review_before_hire"]],
        on="employee_id", how="inner")

    latest = reviews.drop_duplicates("employee_id", keep="last").set_index("employee_id")
    rating_summary = pd.DataFrame({
        "latest_review_period": latest["review_period"],
        "latest_rating": latest["rating"],
        "rating_delta": latest["rating_delta"],
        "avg_rating": reviews.groupby("employee_id")["rating"].mean().round(2),
    }).reset_index()
    rating_summary["rating_trend"] = np.select(
        [rating_summary["rating_delta"] > 0, rating_summary["rating_delta"] < 0, rating_summary["rating_delta"] == 0],
        ["improving", "declining", "stable"], default="insufficient_history")

    # Q2: skill gaps.
    assessments = silver["fact_competency_assessment"].copy()
    assessments["gap"] = assessments["gap"].astype("int64")
    skill_gaps = assessments.merge(competencies, on="competency_id", how="left").merge(
        employees, on="employee_id", how="left")
    skill_gaps["gap_severity"] = np.select(
        [skill_gaps["gap"] >= 2, skill_gaps["gap"] == 1], ["critical", "moderate"], default="none")
    skill_gaps = skill_gaps[emp_cols + ["assessment_id", "competency_id", "competency_name", "category",
                                        "proficiency_level", "required_level", "gap", "gap_severity",
                                        "assessed_date"]]
    critical_gaps = (skill_gaps[skill_gaps["gap_severity"] == "critical"]
                     .groupby("employee_id").size().rename("critical_gap_count"))

    # Q4: learning and development.
    events = silver["fact_learning_event"]
    counts = (events.groupby(["employee_id", "event_type"]).size().unstack(fill_value=0)
              .reindex(columns=EVENT_TYPES, fill_value=0).add_suffix("_count"))
    quiz = events[events["event_type"].isin(QUIZ_EVENT_TYPES)]
    completed = events[events["event_type"] == "course_completed"]
    learning = employees.merge(counts, left_on="employee_id", right_index=True, how="left")
    count_cols = list(counts.columns)
    learning[count_cols] = learning[count_cols].fillna(0).astype("int64")
    learning["total_events"] = learning[count_cols].sum(axis=1)
    learning = learning.merge(pd.DataFrame({
        "avg_quiz_score": quiz.groupby("employee_id")["score"].mean().astype("float64").round(1),
        "distinct_courses_completed": completed.groupby("employee_id")["course_name"].nunique(),
        "last_activity_date": events.groupby("employee_id")["event_date"].max(),
    }), left_on="employee_id", right_index=True, how="left")
    learning["distinct_courses_completed"] = learning["distinct_courses_completed"].fillna(0).astype("int64")
    enrolled = learning["enrollment_count"].replace(0, np.nan)
    learning["completion_rate"] = (learning["course_completed_count"] / enrolled).clip(upper=1.0).round(3)

    # Q3: promotion candidates (High potential and Ready Now).
    talent = silver["fact_talent_review"]
    candidates = (talent[(talent["potential_rating"] == "High") & (talent["promotion_readiness"] == "Ready Now")]
                  .merge(employees, on="employee_id", how="left")
                  .merge(rating_summary, on="employee_id", how="left")
                  .merge(critical_gaps, left_on="employee_id", right_index=True, how="left")
                  .merge(learning[["employee_id", "certification_earned_count", "course_completed_count"]],
                         on="employee_id", how="left"))
    candidates["critical_gap_count"] = candidates["critical_gap_count"].fillna(0).astype("int64")
    candidates["is_recommended"] = (candidates["latest_rating"] >= 4) & (candidates["critical_gap_count"] == 0)

    # Q5: attrition risk.
    attrition = (employees.merge(talent[["employee_id", "attrition_risk", "potential_rating"]],
                                 on="employee_id", how="left")
                 .merge(rating_summary, on="employee_id", how="left")
                 .merge(learning[["employee_id", "last_activity_date"]], on="employee_id", how="left")
                 .rename(columns={"attrition_risk": "hr_attrition_risk"}))
    attrition["days_since_last_learning"] = (pd.Timestamp(as_of) - attrition["last_activity_date"]).dt.days
    delta = attrition["rating_delta"]
    no_learning = attrition["days_since_last_learning"].isna() | (
        attrition["days_since_last_learning"] > RISK_NO_LEARNING_DAYS)
    score = (attrition["hr_attrition_risk"].map(RISK_LABEL_POINTS).fillna(0)
             + np.where(delta <= -2, RISK_DROP_2_POINTS, np.where(delta == -1, RISK_DROP_1_POINTS, 0))
             + np.where(attrition["latest_rating"] <= 2, RISK_LOW_RATING_POINTS, 0)
             + np.where(no_learning, RISK_NO_LEARNING_POINTS, 0)
             + np.where(attrition["tenure_years"] < 1, RISK_NEW_HIRE_POINTS, 0))
    attrition["computed_risk_score"] = score.clip(0, 100).astype("int64")
    attrition["risk_band"] = np.select(
        [attrition["computed_risk_score"] >= 60, attrition["computed_risk_score"] >= 30],
        ["High", "Medium"], default="Low")

    # Q6: team performance vs project outcomes.
    latest_period = reviews.loc[reviews["period_start"].idxmax(), "review_period"]
    latest_reviews = reviews[reviews["review_period"] == latest_period].merge(
        employees[["employee_id", "team_id"]], on="employee_id")
    projects = silver["fact_project"]
    team = teams.merge(
        employees.groupby("team_id").size().rename("headcount"), left_on="team_id", right_index=True, how="left"
    ).merge(pd.DataFrame({
        "avg_rating_latest": latest_reviews.groupby("team_id")["rating"].mean().round(2),
        "pct_high_performers": (latest_reviews.assign(high=latest_reviews["rating"] >= 4)
                                .groupby("team_id")["high"].mean() * 100).round(1),
    }), left_on="team_id", right_index=True, how="left").merge(pd.DataFrame({
        "project_count": projects.groupby("team_id").size(),
        "on_time_pct": (projects.assign(on_time=projects["outcome_status"] == "on-time")
                        .groupby("team_id")["on_time"].mean() * 100).round(1),
        "avg_outcome_score": projects.groupby("team_id")["outcome_score"].mean().round(2),
    }), left_on="team_id", right_index=True, how="left").merge(
        skill_gaps.assign(shortfall=skill_gaps["gap"].clip(lower=0)).groupby("team_id")["shortfall"]
        .mean().round(2).rename("avg_skill_gap"), left_on="team_id", right_index=True, how="left")
    team["headcount"] = team["headcount"].fillna(0).astype("int64")
    team["project_count"] = team["project_count"].fillna(0).astype("int64")
    team["latest_review_period"] = latest_period

    with_projects = team[team["project_count"] > 0]
    correlation = pd.DataFrame([
        {"metric_x": x, "metric_y": y, "n_teams": len(with_projects),
         "pearson_r": round(float(with_projects[x].corr(with_projects[y])), 3)}
        for x in ["avg_rating_latest", "pct_high_performers", "avg_skill_gap"]
        for y in ["avg_outcome_score", "on_time_pct"]
    ])

    # Q7: skill distribution.
    distribution = (skill_gaps.groupby(["competency_id", "competency_name", "category", "department",
                                        "proficiency_level"]).size().rename("employee_count").reset_index())
    group_cols = ["competency_id", "department"]
    distribution["pct_of_assessed"] = (100 * distribution["employee_count"]
                                       / distribution.groupby(group_cols)["employee_count"].transform("sum")).round(1)
    group_stats = skill_gaps.groupby(group_cols).agg(
        avg_gap=("gap", "mean"), pct_below_required=("gap", lambda g: 100 * (g > 0).mean())).round(2).reset_index()
    distribution = distribution.merge(group_stats, on=group_cols, how="left")

    return {
        "employee_performance_history": performance_history.reset_index(drop=True),
        "employee_skill_gaps": skill_gaps.reset_index(drop=True),
        "promotion_candidates": candidates.reset_index(drop=True),
        "employee_learning_summary": learning.reset_index(drop=True),
        "attrition_risk": attrition.reset_index(drop=True),
        "team_performance_outcomes": team.reset_index(drop=True),
        "team_outcome_correlation": correlation,
        "skill_distribution": distribution,
    }


GOLD_SOURCES = {
    "employee_performance_history": ["fact_performance_review", "dim_employee", "dim_team"],
    "employee_skill_gaps": ["fact_competency_assessment", "dim_competency", "dim_employee", "dim_team"],
    "promotion_candidates": ["fact_talent_review", "fact_performance_review", "fact_competency_assessment",
                             "fact_learning_event", "dim_employee"],
    "employee_learning_summary": ["fact_learning_event", "dim_employee", "dim_team"],
    "attrition_risk": ["fact_talent_review", "fact_performance_review", "fact_learning_event", "dim_employee"],
    "team_performance_outcomes": ["dim_team", "dim_employee", "fact_performance_review", "fact_project",
                                  "fact_competency_assessment"],
    "team_outcome_correlation": ["team_performance_outcomes"],
    "skill_distribution": ["fact_competency_assessment", "dim_competency", "dim_employee", "dim_team"],
}

GOLD_GRAIN = {
    "employee_performance_history": ["employee_id", "review_period"],
    "employee_skill_gaps": ["assessment_id"],
    "promotion_candidates": ["employee_id"],
    "employee_learning_summary": ["employee_id"],
    "attrition_risk": ["employee_id"],
    "team_performance_outcomes": ["team_id"],
    "team_outcome_correlation": ["metric_x", "metric_y"],
    "skill_distribution": ["competency_id", "department", "proficiency_level"],
}


@timed_stage("gold")
def aggregate_to_gold(ctx: RunContext) -> dict:
    """Build the Gold layer and run reconciliation checks."""
    lake = ctx.lake
    # Gold holds business columns only; lineage metadata (_ingest_ts, _run_id,
    # _source_file, ...) stays in Silver.
    silver = {}
    for target in [c["target"] for c in SILVER_CONTRACTS.values()]:
        df = read_table(lake, f"silver/{target}")
        silver[target] = df.drop(columns=[c for c in df.columns if c.startswith("_")])
    gold = build_gold(silver, ctx.config.as_of_date)

    stats = {}
    for name, df in gold.items():
        write_table(lake, f"gold/{name}", df.assign(_run_id=ctx.run_id), ctx.run_id)
        stats[name] = {"written": len(df)}
        for source in GOLD_SOURCES[name]:
            ctx.add_lineage(f"silver/{source}" if not source.startswith("team_") else f"gold/{source}",
                            f"gold/{name}", "join + aggregate")
        log(logging.INFO, "gold_table_written", table=name, rows=len(df))

    ctx.checks = reconcile(lake, silver, gold)
    return stats


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------


def reconcile(lake: Store, silver: dict[str, pd.DataFrame], gold: dict[str, pd.DataFrame]) -> list[dict]:
    """Check that row counts and aggregates agree across layers."""
    checks: list[dict] = []

    def check(name: str, expected: Any, actual: Any) -> None:
        passed = bool(expected == actual) if not isinstance(expected, float) else abs(expected - actual) < 1e-9
        checks.append({"check": name, "expected": expected, "actual": actual, "passed": passed})
        log(logging.INFO if passed else logging.ERROR, "reconciliation_check",
            check=name, expected=expected, actual=actual, passed=passed)

    bronze = read_json(lake, "_meta/latest/bronze_stats.json", {}).get("datasets", {})
    silver_stats = read_json(lake, "_meta/latest/silver_stats.json", {}).get("datasets", {})

    lms_b, lms_s = bronze.get("learning_activity", {}), silver_stats.get("learning_activity", {})
    if lms_b and lms_s:
        check("lms: bronze lines = silver lines read", lms_b["lines_total"], lms_s["lines"])
        check("lms: lines = parsed + malformed", lms_s["lines"], lms_s["parsed"] + lms_s["malformed"])
        check("lms: parsed = written + quarantined(non-parse) + deduplicated", lms_s["parsed"],
              lms_s["written"] + lms_s["quarantined"] - lms_s["malformed"] + lms_s["deduplicated"])
    if lms_s:
        audit = lake.list("quarantine/learning_activity_duplicates")
        audited = len(read_table(lake, "quarantine/learning_activity_duplicates")) if audit else 0
        check("lms: duplicate audit rows = deduplicated", lms_s["deduplicated"], audited)
    for table in HR_TABLES:
        if table in bronze and table in silver_stats:
            s = silver_stats[table]
            check(f"hr {table}: bronze = silver + quarantined", bronze[table]["read"],
                  s["written"] + s["quarantined"])

    check("silver events = fact_learning_event rows", lms_s.get("written"), len(silver["fact_learning_event"]))
    check("gold performance_history rows = silver reviews", len(silver["fact_performance_review"]),
          len(gold["employee_performance_history"]))
    check("gold skill_gaps rows = silver assessments", len(silver["fact_competency_assessment"]),
          len(gold["employee_skill_gaps"]))
    check("gold learning total_events = silver events", len(silver["fact_learning_event"]),
          int(gold["employee_learning_summary"]["total_events"].sum()))
    check("gold team headcount = silver employees", len(silver["dim_employee"]),
          int(gold["team_performance_outcomes"]["headcount"].sum()))
    check("gold team project_count = silver projects", len(silver["fact_project"]),
          int(gold["team_performance_outcomes"]["project_count"].sum()))
    check("gold skill_distribution count = silver assessments", len(silver["fact_competency_assessment"]),
          int(gold["skill_distribution"]["employee_count"].sum()))
    check("gold avg rating = silver avg rating",
          round(float(silver["fact_performance_review"]["rating"].astype("float64").mean()), 9),
          round(float(gold["employee_performance_history"]["rating"].mean()), 9))
    check("gold attrition rows = silver employees", len(silver["dim_employee"]), len(gold["attrition_risk"]))
    for name, grain in GOLD_GRAIN.items():
        check(f"gold {name}: unique on {grain}", 0, int(gold[name].duplicated(subset=grain).sum()))
    return checks
