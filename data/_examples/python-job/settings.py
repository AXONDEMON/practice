"""Constants and Silver schema contracts for the talent ETL job."""

from __future__ import annotations

from pathlib import Path
from typing import Any

# Local default source: <repo>/sampleData, three levels above this file in the
# repo (data/_examples/python-job/settings.py). On EKS the files run from /app
# and reads its sources from S3, so fall back to the job folder there.
_JOB_DIR = Path(__file__).resolve().parent
SAMPLE_DATA_DIR = (
    (_JOB_DIR.parents[2] if len(_JOB_DIR.parents) > 2 else _JOB_DIR)
    / "sampleData"
    / "data-engineering-sample-performance-scenario"
    / "data-engineering-performance-talent-management"
)

# HR tables in load order (parents before children), with their primary keys.
HR_TABLES: dict[str, list[str]] = {
    "teams": ["team_id"],
    "competencies": ["competency_id"],
    "employees": ["employee_id"],
    "projects": ["project_id"],
    "performance_reviews": ["review_id"],
    "competency_assessments": ["assessment_id"],
    "talent_review": ["employee_id"],
}

JOB_LEVEL_RANK = {
    "IC1": 1,
    "IC2": 2,
    "IC3": 3,
    "Senior IC": 4,
    "Manager": 5,
    "Senior Manager": 6,
    "Director": 7,
}

EVENT_TYPES = [
    "enrollment",
    "module_started",
    "module_completed",
    "quiz_attempted",
    "quiz_passed",
    "course_completed",
    "certification_earned",
]
QUIZ_EVENT_TYPES = ["quiz_attempted", "quiz_passed"]
LMS_KNOWN_FIELDS = {"event_id", "employee_id", "event_date", "event_type", "course_name", "text", "notes", "score"}
QUIZ_PASS_MARK = 60

# Attrition risk score weights (Gold attrition_risk).
RISK_LABEL_POINTS = {"High": 40, "Medium": 20, "Low": 0}
RISK_DROP_2_POINTS = 25
RISK_DROP_1_POINTS = 15
RISK_LOW_RATING_POINTS = 15
RISK_NO_LEARNING_POINTS = 10
RISK_NO_LEARNING_DAYS = 180
RISK_NEW_HIRE_POINTS = 10

# Silver schema contracts. Column spec keys:
#   type: int | decimal | str | date | timestamp
#   required, domain, min, max, pattern, rename, fk=(silver_table, column)
SILVER_CONTRACTS: dict[str, dict[str, Any]] = {
    "teams": {
        "target": "dim_team",
        "pk": ["team_id"],
        "columns": {
            "team_id": {"type": "int", "required": True},
            "name": {"type": "str", "required": True, "rename": "team_name"},
            "department": {"type": "str", "required": True},
        },
    },
    "competencies": {
        "target": "dim_competency",
        "pk": ["competency_id"],
        "columns": {
            "competency_id": {"type": "int", "required": True},
            "name": {"type": "str", "required": True, "rename": "competency_name"},
            "category": {"type": "str", "required": True, "domain": ["Technical", "Leadership", "Domain"]},
        },
    },
    "employees": {
        "target": "dim_employee",
        "pk": ["employee_id"],
        "columns": {
            "employee_id": {"type": "int", "required": True},
            "first_name": {"type": "str", "required": True},
            "last_name": {"type": "str", "required": True},
            "email": {"type": "str", "required": True, "pattern": r"^[^@\s]+@[^@\s]+\.[^@\s]+$"},
            "team_id": {"type": "int", "required": True, "fk": ("dim_team", "team_id")},
            "job_level": {"type": "str", "required": True, "domain": list(JOB_LEVEL_RANK)},
            "hire_date": {"type": "date", "required": True},
        },
    },
    "projects": {
        "target": "fact_project",
        "pk": ["project_id"],
        "columns": {
            "project_id": {"type": "int", "required": True},
            "name": {"type": "str", "required": True, "rename": "project_name"},
            "team_id": {"type": "int", "required": True, "fk": ("dim_team", "team_id")},
            "outcome_status": {
                "type": "str",
                "required": True,
                "domain": ["on-time", "delayed", "at-risk", "cancelled"],
            },
            "outcome_score": {"type": "decimal", "required": True, "min": 0, "max": 100},
        },
    },
    "performance_reviews": {
        "target": "fact_performance_review",
        "pk": ["review_id"],
        "unique": ["employee_id", "review_period"],
        "columns": {
            "review_id": {"type": "int", "required": True},
            "employee_id": {"type": "int", "required": True, "fk": ("dim_employee", "employee_id")},
            "review_period": {"type": "str", "required": True, "pattern": r"^\d{4}-H[12]$"},
            "rating": {"type": "int", "required": True, "min": 1, "max": 5},
            "manager_comment": {"type": "str", "required": False},
            "submitted_date": {"type": "date", "required": True},
        },
    },
    "competency_assessments": {
        "target": "fact_competency_assessment",
        "pk": ["assessment_id"],
        "columns": {
            "assessment_id": {"type": "int", "required": True},
            "employee_id": {"type": "int", "required": True, "fk": ("dim_employee", "employee_id")},
            "competency_id": {"type": "int", "required": True, "fk": ("dim_competency", "competency_id")},
            "proficiency_level": {"type": "int", "required": True, "min": 1, "max": 5},
            "required_level": {"type": "int", "required": True, "min": 1, "max": 5},
            "assessed_date": {"type": "date", "required": True},
        },
    },
    "talent_review": {
        "target": "fact_talent_review",
        "pk": ["employee_id"],
        "columns": {
            "employee_id": {"type": "int", "required": True, "fk": ("dim_employee", "employee_id")},
            "review_cycle": {"type": "str", "required": True, "pattern": r"^\d{4}-H[12]$"},
            "potential_rating": {"type": "str", "required": True, "domain": ["Low", "Medium", "High"]},
            "promotion_readiness": {
                "type": "str",
                "required": True,
                "domain": ["Ready Now", "1-2 Years", "Not Ready"],
            },
            "attrition_risk": {"type": "str", "required": True, "domain": ["Low", "Medium", "High"]},
        },
    },
    "learning_activity": {
        "target": "fact_learning_event",
        "pk": ["event_id"],
        "pk_policy": "dedup",
        "columns": {
            "event_id": {"type": "str", "required": True},
            "employee_id": {"type": "int", "required": True, "fk": ("dim_employee", "employee_id")},
            "event_date": {"type": "timestamp", "required": True, "rename": "event_ts"},
            "event_type": {"type": "str", "required": True, "domain": EVENT_TYPES},
            "course_name": {"type": "str", "required": True},
            "event_text": {"type": "str", "required": False},
            "score": {"type": "decimal", "required": False, "min": 0, "max": 100},
        },
    },
}
