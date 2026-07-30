"""Aggregate drift detection: flags row-count/sum swings a per-row check can't see."""

from typing import Literal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse.silver.data_quality.checks.results import DQCheckResult


def check_no_silent_drift(
    new_df: DataFrame,
    previous_df: DataFrame,
    numeric_columns: list[str],
    max_relative_change: float = 0.2,
    severity: Literal["error", "warn"] = "warn",
) -> list[DQCheckResult]:
    """Flags a new table version whose row count or numeric totals moved too sharply.

    Mirrors the "run on prod, compare test vs prod" idea from Stint's pipeline
    testing write-up, but instead of a separate pre-prod environment it
    compares the freshly transformed DataFrame against the Silver table's own
    current (pre-overwrite) contents -- see `BaseSilverTransformer.run()`.
    Catches changes that pass every per-row check (schema, nulls, ranges) yet
    silently reshape the dataset in aggregate: a join fan-out that quietly
    multiplies row count, an upstream feed that goes dark and starves half
    the rows, or a unit conversion bug that shifts a damage-total column by
    10x. None of those trip a per-row constraint, but all of them jump the
    aggregates checked here.

    Args:
        new_df (DataFrame): Freshly transformed DataFrame about to be written.
        previous_df (DataFrame): The table's previously persisted contents.
        numeric_columns (list[str]): Numeric columns to sum and compare in
            addition to row count (e.g. a damage-total column).
        max_relative_change (float): Maximum allowed |new - previous| /
            previous ratio before a metric is flagged. Defaults to 0.2 (20%).
        severity (Literal["error", "warn"]): Severity to attach to each
            result. Defaults to "warn" -- a large but legitimate refresh
            (e.g. a genuine catastrophe spike) shouldn't block promotion by
            default; pass "error" for tables where that risk is unacceptable.

    Returns:
        list[DQCheckResult]: One "no_silent_drift_row_count" result, plus one
            "no_silent_drift_sum_{column}" result per entry in
            `numeric_columns`. When `previous_df` has zero rows or a zero sum
            for a column, that metric's check passes automatically -- there is
            no prior baseline to compare against (e.g. the table's first run).
    """
    results = [
        _check_relative_change(
            metric_name="row_count",
            new_value=float(new_df.count()),
            previous_value=float(previous_df.count()),
            max_relative_change=max_relative_change,
            severity=severity,
        )
    ]
    for column in numeric_columns:
        new_sum_row = new_df.agg(F.sum(F.col(column)).alias("_sum")).first()
        previous_sum_row = previous_df.agg(F.sum(F.col(column)).alias("_sum")).first()
        # Reason: agg() over any DataFrame (including an empty one) always
        # returns exactly one row; None is not a real outcome here, just
        # DataFrame.first()'s general (possibly-empty-DataFrame) return type.
        assert new_sum_row is not None
        assert previous_sum_row is not None
        new_sum = new_sum_row["_sum"] or 0.0
        previous_sum = previous_sum_row["_sum"] or 0.0
        results.append(
            _check_relative_change(
                metric_name=f"sum_{column}",
                new_value=float(new_sum),
                previous_value=float(previous_sum),
                max_relative_change=max_relative_change,
                severity=severity,
            )
        )
    return results


def _check_relative_change(
    metric_name: str,
    new_value: float,
    previous_value: float,
    max_relative_change: float,
    severity: Literal["error", "warn"],
) -> DQCheckResult:
    """Flags a metric whose relative change from `previous_value` exceeds the threshold."""
    if previous_value == 0:
        passed = True
    else:
        relative_change = abs(new_value - previous_value) / abs(previous_value)
        passed = relative_change <= max_relative_change
    return DQCheckResult(
        check_name=f"no_silent_drift_{metric_name}",
        passed=passed,
        failed_count=0 if passed else 1,
        severity=severity,
    )
