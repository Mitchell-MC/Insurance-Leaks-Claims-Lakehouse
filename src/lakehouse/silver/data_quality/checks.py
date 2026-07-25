"""Reusable data-quality checks for Silver-layer transformers."""

from pydantic import BaseModel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DQCheckResult(BaseModel):
    """Outcome of a single data-quality check.

    Attributes:
        check_name (str): Human-readable name of the check.
        passed (bool): Whether the check found zero failing records/columns.
        failed_count (int): Number of records (or unexpected columns) that failed.
    """

    check_name: str
    passed: bool
    failed_count: int


def check_no_null_geography(df: DataFrame, geography_columns: list[str]) -> DQCheckResult:
    """Flags records with a null value in any geography column.

    Args:
        df (DataFrame): DataFrame to check.
        geography_columns (list[str]): Columns that must not be null (e.g. state).

    Returns:
        DQCheckResult: Result named "no_null_geography".
    """
    condition = F.lit(False)
    for column in geography_columns:
        condition = condition | F.col(column).isNull()
    failed_count = df.filter(condition).count()
    return DQCheckResult(
        check_name="no_null_geography", passed=failed_count == 0, failed_count=failed_count
    )


def check_valid_dates(df: DataFrame, date_columns: list[str]) -> DQCheckResult:
    """Flags records where any date column is null (e.g. failed to parse).

    Args:
        df (DataFrame): DataFrame to check.
        date_columns (list[str]): Columns expected to hold non-null parsed dates.

    Returns:
        DQCheckResult: Result named "valid_dates".
    """
    condition = F.lit(False)
    for column in date_columns:
        condition = condition | F.col(column).isNull()
    failed_count = df.filter(condition).count()
    return DQCheckResult(
        check_name="valid_dates", passed=failed_count == 0, failed_count=failed_count
    )


def check_no_duplicate_keys(df: DataFrame, key_columns: list[str]) -> DQCheckResult:
    """Flags duplicate records sharing the same key column values.

    Args:
        df (DataFrame): DataFrame to check.
        key_columns (list[str]): Columns that should uniquely identify a record.

    Returns:
        DQCheckResult: Result named "no_duplicate_keys".
    """
    total = df.count()
    distinct = df.select(*key_columns).distinct().count()
    failed_count = total - distinct
    return DQCheckResult(
        check_name="no_duplicate_keys", passed=failed_count == 0, failed_count=failed_count
    )


def check_schema_drift(df: DataFrame, expected_columns: set[str]) -> DQCheckResult:
    """Flags columns present in the DataFrame but outside the expected schema.

    Args:
        df (DataFrame): DataFrame to check.
        expected_columns (set[str]): Column names the downstream schema expects.

    Returns:
        DQCheckResult: Result named "schema_drift"; `failed_count` is the number
            of unexpected columns found.
    """
    unexpected = set(df.columns) - expected_columns
    return DQCheckResult(
        check_name="schema_drift", passed=len(unexpected) == 0, failed_count=len(unexpected)
    )
