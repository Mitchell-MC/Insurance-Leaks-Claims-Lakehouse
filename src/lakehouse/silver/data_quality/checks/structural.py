"""Per-row structural checks: nulls, dates, duplicates, schema, and value/expression rules."""

from typing import Literal

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from lakehouse.silver.data_quality.checks.results import DQCheckResult


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
