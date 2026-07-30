"""Tests for per-row structural DQ checks: nulls, dates, duplicates, schema, values, expressions."""

from datetime import date

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

from lakehouse.silver.data_quality.checks import (
    check_accepted_values,
    check_expression,
    check_no_duplicate_keys,
    check_no_null_geography,
    check_schema_drift,
    check_valid_dates,
)


def test_check_no_null_geography_passes_when_no_nulls(spark: SparkSession) -> None:
    """check_no_null_geography passes when every geography column is populated."""
    df = spark.createDataFrame([Row(state="TX", county="Harris"), Row(state="FL", county="Dade")])

    result = check_no_null_geography(df, ["state", "county"])

    assert result.passed
    assert result.failed_count == 0


def test_check_no_null_geography_counts_null_rows(spark: SparkSession) -> None:
    """check_no_null_geography counts rows with a null in any listed column."""
    df = spark.createDataFrame([Row(state="TX", county="Harris"), Row(state=None, county="Dade")])

    result = check_no_null_geography(df, ["state", "county"])

    assert not result.passed
    assert result.failed_count == 1


def test_check_valid_dates_counts_null_dates(spark: SparkSession) -> None:
    """check_valid_dates counts rows where a date column failed to parse (is null)."""
    df = spark.createDataFrame([Row(event_date=date(2020, 1, 1)), Row(event_date=None)])

    result = check_valid_dates(df, ["event_date"])

    assert not result.passed
    assert result.failed_count == 1


def test_check_no_duplicate_keys_detects_duplicates(spark: SparkSession) -> None:
    """check_no_duplicate_keys counts rows beyond the distinct key count."""
    df = spark.createDataFrame([Row(id="a", value=1), Row(id="a", value=2), Row(id="b", value=3)])

    result = check_no_duplicate_keys(df, ["id"])

    assert not result.passed
    assert result.failed_count == 1


def test_check_schema_drift_flags_unexpected_columns(spark: SparkSession) -> None:
    """check_schema_drift flags columns outside the expected set."""
    df = spark.createDataFrame([Row(state="TX", unexpected_column="x")])

    result = check_schema_drift(df, expected_columns={"state"})

    assert not result.passed
    assert result.failed_count == 1


def test_check_accepted_values_passes_for_known_values(spark: SparkSession) -> None:
    """check_accepted_values passes when every value is in the allowed set."""
    df = spark.createDataFrame([Row(band="severe"), Row(band="minor")])

    result = check_accepted_values(df, "band", {"severe", "moderate", "minor"})

    assert result.passed
    assert result.failed_count == 0


def test_check_accepted_values_flags_unknown_values(spark: SparkSession) -> None:
    """check_accepted_values counts rows whose value isn't in the allowed set."""
    df = spark.createDataFrame([Row(band="severe"), Row(band="catastrophic")])

    result = check_accepted_values(df, "band", {"severe", "moderate", "minor"})

    assert not result.passed
    assert result.failed_count == 1
    assert result.check_name == "accepted_values_band"


def test_check_accepted_values_ignores_nulls(spark: SparkSession) -> None:
    """check_accepted_values does not flag nulls -- that's a separate not-null check's job."""
    df = spark.createDataFrame([Row(band="severe"), Row(band=None)])

    result = check_accepted_values(df, "band", {"severe"})

    assert result.passed


def test_check_expression_flags_false_rows(spark: SparkSession) -> None:
    """check_expression counts rows where the expression is false."""
    df = spark.createDataFrame([Row(amount=10.0), Row(amount=-5.0)])

    result = check_expression(df, F.col("amount") >= 0, "amount_non_negative")

    assert not result.passed
    assert result.failed_count == 1
    assert result.check_name == "amount_non_negative"


def test_check_expression_flags_null_as_failure(spark: SparkSession) -> None:
    """check_expression treats a null expression result as a failure (SQL "unknown" semantics)."""
    df = spark.createDataFrame([Row(amount=10.0), Row(amount=None)])

    result = check_expression(df, F.col("amount") >= 0, "amount_non_negative")

    assert not result.passed
    assert result.failed_count == 1
