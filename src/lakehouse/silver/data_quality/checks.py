"""Reusable data-quality checks for Silver- and Gold-layer transformers."""

from datetime import datetime, timedelta
from typing import Literal

from pydantic import BaseModel
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


class DQCheckResult(BaseModel):
    """Outcome of a single data-quality check.

    Attributes:
        check_name (str): Human-readable name of the check.
        passed (bool): Whether the check found zero failing records/columns.
        failed_count (int): Number of records (or unexpected columns) that failed.
        severity (Literal["error", "warn"]): "error" (default) means a failure
            must block promotion of the data; "warn" means it's logged but
            non-blocking. Mirrors dbt's `severity` config.
    """

    check_name: str
    passed: bool
    failed_count: int
    severity: Literal["error", "warn"] = "error"


class DQCheckFailure(Exception):
    """Raised when one or more "error"-severity DQCheckResults failed."""

    def __init__(self, failures: list[DQCheckResult]) -> None:
        """Builds an error message summarizing every failed check.

        Args:
            failures (list[DQCheckResult]): The failed, error-severity results.
        """
        self.failures = failures
        summary = ", ".join(
            f"{result.check_name} ({result.failed_count} failing)" for result in failures
        )
        super().__init__(f"Data-quality checks failed: {summary}")


def raise_on_failures(results: list[DQCheckResult]) -> None:
    """Raises DQCheckFailure if any "error"-severity check in `results` failed.

    "warn"-severity failures are ignored here; callers that want to surface
    them can inspect `results` directly.

    Args:
        results (list[DQCheckResult]): Results to inspect.

    Raises:
        DQCheckFailure: If any error-severity result has `passed=False`.
    """
    blocking_failures = [
        result for result in results if not result.passed and result.severity == "error"
    ]
    if blocking_failures:
        raise DQCheckFailure(blocking_failures)


def check_no_null_geography(
    df: DataFrame, geography_columns: list[str], severity: Literal["error", "warn"] = "error"
) -> DQCheckResult:
    """Flags records with a null value in any geography column.

    Args:
        df (DataFrame): DataFrame to check.
        geography_columns (list[str]): Columns that must not be null (e.g. state).
        severity (Literal["error", "warn"]): Severity to attach to the result.

    Returns:
        DQCheckResult: Result named "no_null_geography".
    """
    condition = F.lit(False)
    for column in geography_columns:
        condition = condition | F.col(column).isNull()
    failed_count = df.filter(condition).count()
    return DQCheckResult(
        check_name="no_null_geography",
        passed=failed_count == 0,
        failed_count=failed_count,
        severity=severity,
    )


def check_valid_dates(
    df: DataFrame, date_columns: list[str], severity: Literal["error", "warn"] = "error"
) -> DQCheckResult:
    """Flags records where any date column is null (e.g. failed to parse).

    Args:
        df (DataFrame): DataFrame to check.
        date_columns (list[str]): Columns expected to hold non-null parsed dates.
        severity (Literal["error", "warn"]): Severity to attach to the result.

    Returns:
        DQCheckResult: Result named "valid_dates".
    """
    condition = F.lit(False)
    for column in date_columns:
        condition = condition | F.col(column).isNull()
    failed_count = df.filter(condition).count()
    return DQCheckResult(
        check_name="valid_dates",
        passed=failed_count == 0,
        failed_count=failed_count,
        severity=severity,
    )


def check_no_duplicate_keys(
    df: DataFrame, key_columns: list[str], severity: Literal["error", "warn"] = "error"
) -> DQCheckResult:
    """Flags duplicate records sharing the same key column values.

    Args:
        df (DataFrame): DataFrame to check.
        key_columns (list[str]): Columns that should uniquely identify a record.
        severity (Literal["error", "warn"]): Severity to attach to the result.

    Returns:
        DQCheckResult: Result named "no_duplicate_keys".
    """
    total = df.count()
    distinct = df.select(*key_columns).distinct().count()
    failed_count = total - distinct
    return DQCheckResult(
        check_name="no_duplicate_keys",
        passed=failed_count == 0,
        failed_count=failed_count,
        severity=severity,
    )


def check_schema_drift(
    df: DataFrame, expected_columns: set[str], severity: Literal["error", "warn"] = "error"
) -> DQCheckResult:
    """Flags columns present in the DataFrame but outside the expected schema.

    Args:
        df (DataFrame): DataFrame to check.
        expected_columns (set[str]): Column names the downstream schema expects.
        severity (Literal["error", "warn"]): Severity to attach to the result.

    Returns:
        DQCheckResult: Result named "schema_drift"; `failed_count` is the number
            of unexpected columns found.
    """
    unexpected = set(df.columns) - expected_columns
    return DQCheckResult(
        check_name="schema_drift",
        passed=len(unexpected) == 0,
        failed_count=len(unexpected),
        severity=severity,
    )


def check_accepted_values(
    df: DataFrame,
    column: str,
    allowed_values: set[str],
    severity: Literal["error", "warn"] = "error",
) -> DQCheckResult:
    """Flags records whose column value falls outside a known set.

    Mirrors dbt's `accepted_values` generic test -- catches an upstream
    source silently introducing a new category (e.g. a new NOAA EVENT_TYPE
    or FEMA incidentType) that downstream logic doesn't account for. Nulls
    are not flagged here; pair with `check_no_null_geography`/`check_valid_dates`
    or a dedicated not-null check for that.

    Args:
        df (DataFrame): DataFrame to check.
        column (str): Column whose values must be in `allowed_values`.
        allowed_values (set[str]): The known/expected set of values.
        severity (Literal["error", "warn"]): Severity to attach to the result.

    Returns:
        DQCheckResult: Result named "accepted_values_{column}".
    """
    failed_count = df.filter(
        F.col(column).isNotNull() & ~F.col(column).isin(*allowed_values)
    ).count()
    return DQCheckResult(
        check_name=f"accepted_values_{column}",
        passed=failed_count == 0,
        failed_count=failed_count,
        severity=severity,
    )


def check_expression(
    df: DataFrame,
    expression: Column,
    check_name: str,
    severity: Literal["error", "warn"] = "error",
) -> DQCheckResult:
    """Flags records where a boolean expression evaluates to false.

    Mirrors dbt_utils.expression_is_true -- the general-purpose escape hatch
    for cross-column business-rule invariants (e.g. `days_to_declaration >= 0`,
    `total_damage_property_usd >= 0`) that don't fit the other named checks.
    A null expression result (e.g. from a null input column) counts as a
    failure, matching SQL's "unknown is not true" semantics.

    Args:
        df (DataFrame): DataFrame to check.
        expression (Column): Boolean column; rows where this is not true fail.
        check_name (str): Name to attach to the result (e.g.
            "days_to_declaration_non_negative").
        severity (Literal["error", "warn"]): Severity to attach to the result.

    Returns:
        DQCheckResult: Result named `check_name`.
    """
    failed_count = df.filter(~expression | expression.isNull()).count()
    return DQCheckResult(
        check_name=check_name,
        passed=failed_count == 0,
        failed_count=failed_count,
        severity=severity,
    )


def check_referential_integrity(
    fact_df: DataFrame,
    dim_df: DataFrame,
    fact_key: str,
    dim_key: str,
    severity: Literal["error", "warn"] = "error",
    check_name: str | None = None,
) -> DQCheckResult:
    """Flags fact rows whose foreign key has no matching dimension row.

    Mirrors dbt's `relationships` test. Catches a left join that silently
    dropped the match (e.g. a state code that didn't reconcile against
    `dim_geography_state`) instead of surfacing it as a `NULL`/orphaned key.
    A null `fact_key` is treated as a failure -- a fact row should always
    resolve to a real dimension row.

    Args:
        fact_df (DataFrame): Fact table to check.
        dim_df (DataFrame): Dimension table `fact_key` should resolve against.
        fact_key (str): Foreign-key column on `fact_df`.
        dim_key (str): Primary-key column on `dim_df`.
        severity (Literal["error", "warn"]): Severity to attach to the result.
        check_name (str | None): Override for the result's name. Defaults to
            "referential_integrity_{fact_key}"; pass an explicit name when
            checking the same `fact_key`/`dim_key` pair against more than one
            fact table, since the default alone wouldn't disambiguate them.

    Returns:
        DQCheckResult: Result named `check_name`, or
            "referential_integrity_{fact_key}" if not given.
    """
    valid_keys = dim_df.select(F.col(dim_key).alias("_valid_key")).distinct()
    orphaned = fact_df.join(valid_keys, fact_df[fact_key] == valid_keys["_valid_key"], "left_anti")
    failed_count = orphaned.count()
    return DQCheckResult(
        check_name=check_name or f"referential_integrity_{fact_key}",
        passed=failed_count == 0,
        failed_count=failed_count,
        severity=severity,
    )


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
