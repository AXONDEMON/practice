"""Shared plumbing: errors, JSON logging, configuration, retries and AWS error hints."""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable

from settings import SAMPLE_DATA_DIR

LOGGER = logging.getLogger("talent-etl")
class PipelineError(Exception):
    """A fatal, dataset-level failure that stops the current stage."""


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


class JsonFormatter(logging.Formatter):
    """Format log records as one JSON object per line."""

    def format(self, record: logging.LogRecord) -> str:
        """Render ``record`` as a JSON object with its structured fields."""
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "event": record.getMessage(),
        }
        payload.update(getattr(record, "fields", {}))
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def setup_logging(level: str) -> None:
    """Send structured JSON logs to stdout."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    LOGGER.handlers[:] = [handler]
    LOGGER.setLevel(level.upper())
    LOGGER.propagate = False


def log(level: int, event: str, **fields: Any) -> None:
    """Log an event with structured fields."""
    LOGGER.log(level, event, extra={"fields": fields})


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Config:
    """Runtime settings, read from environment variables."""

    is_local: bool
    postgres_host: str
    postgres_port: int
    postgres_name: str
    postgres_user: str
    postgres_pass: str
    lake_root: str
    lms_source: str
    hr_seed_source: str
    as_of_date: date
    max_reject_pct: float
    batch_size: int
    log_level: str
    serve_postgres: bool = False


def load_config() -> Config:
    """Build the configuration from environment variables.

    Local defaults match the workshop's local PostgreSQL. In cloud mode
    (``IS_LOCAL=false``) the PostgreSQL settings and a lake location are
    required.
    """
    env = os.environ
    is_local = env.get("IS_LOCAL", "true").strip().lower() == "true"
    bucket = env.get("AWS_S3_BUCKET", "").strip()

    if is_local:
        default_lake = str(Path.home() / "talent-etl-lake")
        default_lms = str(SAMPLE_DATA_DIR / "learning_activity")
        default_seed = str(SAMPLE_DATA_DIR)
    else:
        default_lake = f"s3://{bucket}/lake" if bucket else ""
        default_lms = f"s3://{bucket}/landing/learning_activity" if bucket else ""
        default_seed = f"s3://{bucket}/landing/hr" if bucket else ""

    config = Config(
        is_local=is_local,
        postgres_host=env.get("POSTGRES_HOST") or "localhost",
        postgres_port=int(env.get("POSTGRES_PORT") or 5432),
        postgres_name=env.get("POSTGRES_NAME") or ("postgres" if is_local else ""),
        postgres_user=env.get("POSTGRES_USER") or ("postgres" if is_local else ""),
        postgres_pass=env.get("POSTGRES_PASS", ""),
        lake_root=env.get("LAKE_ROOT") or default_lake,
        lms_source=env.get("LMS_SOURCE_PATH") or default_lms,
        hr_seed_source=env.get("HR_SEED_SOURCE_PATH") or default_seed,
        as_of_date=date.fromisoformat(env["AS_OF_DATE"]) if env.get("AS_OF_DATE") else date.today(),
        max_reject_pct=float(env.get("DQ_MAX_REJECT_PCT") or 5),
        batch_size=int(env.get("BATCH_SIZE") or 5000),
        log_level=env.get("LOG_LEVEL") or "INFO",
        # Publish Silver/Gold to PostgreSQL schemas after Gold: on locally, off in the cloud unless set.
        serve_postgres=(env.get("SERVE_TO_POSTGRES") or str(is_local)).strip().lower() == "true",
    )

    missing = [
        name
        for name, value in [
            ("POSTGRES_NAME", config.postgres_name),
            ("POSTGRES_USER", config.postgres_user),
            ("POSTGRES_PASS", config.postgres_pass),
            ("LAKE_ROOT or AWS_S3_BUCKET", config.lake_root),
            ("LMS_SOURCE_PATH or AWS_S3_BUCKET", config.lms_source),
        ]
        if not value
    ]
    if missing:
        raise PipelineError(f"Missing required environment variables: {', '.join(missing)}")
    return config


def retry(func: Callable[[], Any], what: str, attempts: int = 5, base_delay: float = 1.0,
          is_transient: Callable[[Exception], bool] = lambda exc: True) -> Any:
    """Call ``func`` with exponential backoff on transient errors."""
    for attempt in range(1, attempts + 1):
        try:
            return func()
        except Exception as exc:  # noqa: BLE001 - re-raised after the last attempt
            if attempt == attempts or not is_transient(exc):
                raise
            delay = base_delay * 2 ** (attempt - 1)
            log(logging.WARNING, "retry", what=what, attempt=attempt, max_attempts=attempts,
                delay_s=delay, error=str(exc))
            time.sleep(delay)
    return None


def explain_aws_error(exc: BaseException | None) -> str | None:
    """Return an actionable hint if ``exc`` (or its cause) is an AWS credential or permission error."""
    seen = 0
    while exc is not None and seen < 10:
        name = type(exc).__name__
        code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", "")) if hasattr(exc, "response") else ""
        if name in ("NoCredentialsError", "PartialCredentialsError", "CredentialRetrievalError"):
            return ("No AWS credentials found. Locally: set AWS credentials or a profile. On EKS: the pod "
                    "must reach the node's instance metadata (helm pythonJob.hostNetwork=true when the "
                    "node's IMDS hop limit is 1).")
        if code in ("AccessDenied", "403", "AllAccessDisabled", "InvalidAccessKeyId", "ExpiredToken",
                    "SignatureDoesNotMatch"):
            return (f"AWS refused the request ({code}). Check the credentials or IAM role have S3 access "
                    "to the lake bucket (on EKS: infra/eks.tf aws_iam_role_policy.eks_node_lake).")
        exc, seen = exc.__cause__ or exc.__context__, seen + 1
    return None
