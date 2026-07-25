"""Tests for reusable Silver-layer data-quality checks."""

from datetime import date

from pyspark.sql import Row, SparkSession

from lakehouse.silver.data_quality.checks import (
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
