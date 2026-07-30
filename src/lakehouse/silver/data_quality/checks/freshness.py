"""Source-freshness check: flags a feed that has quietly stopped updating."""

from datetime import datetime, timedelta
from typing import Literal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse.silver.data_quality.checks.results import DQCheckResult


def check_freshness(
    df: DataFrame,
    timestamp_column: str,
    max_age: timedelta,
    severity: Literal["error", "warn"] = "warn",
) -> DQCheckResult:
    """Flags a DataFrame whose newest `timestamp_column` value is too old.

    Mirrors dbt source freshness -- catches a source/feed that has quietly
    stopped updating even though the pipeline itself runs green. Defaults to
    "warn" severity since a stale near-real-time snapshot (e.g. NWS alerts)
    is usually worth surfacing without blocking the whole pipeline run.

    Args:
        df (DataFrame): DataFrame to check (typically a Bronze snapshot).
        timestamp_column (str): Column holding a timestamp for each record.
        max_age (datetime.timedelta): Maximum allowed age of the newest record,
            measured against the current time.
        severity (Literal["error", "warn"]): Severity to attach to the result.

    Returns:
        DQCheckResult: Result named "freshness_{timestamp_column}".
            `failed_count` is 1 if stale (or the column has no rows/all
            nulls), 0 otherwise.
    """
    max_timestamp_row = df.agg(F.max(F.col(timestamp_column)).alias("_max_ts")).first()
    max_timestamp = max_timestamp_row["_max_ts"] if max_timestamp_row else None
    if max_timestamp is None:
        return DQCheckResult(
            check_name=f"freshness_{timestamp_column}",
            passed=False,
            failed_count=1,
            severity=severity,
        )
    age = datetime.now(max_timestamp.tzinfo) - max_timestamp
    stale = age > max_age
    return DQCheckResult(
        check_name=f"freshness_{timestamp_column}",
        passed=not stale,
        failed_count=1 if stale else 0,
        severity=severity,
    )
