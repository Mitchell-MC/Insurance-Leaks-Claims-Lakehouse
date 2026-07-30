"""Tests for the source-freshness check."""

from datetime import UTC, datetime, timedelta

from pyspark.sql import Row, SparkSession

from lakehouse.silver.data_quality.checks import check_freshness


def test_check_freshness_passes_for_recent_timestamp(spark: SparkSession) -> None:
    """check_freshness passes when the newest timestamp is within max_age."""
    recent = datetime.now(UTC) - timedelta(minutes=5)
    df = spark.createDataFrame([Row(loaded_at=recent)])

    result = check_freshness(df, "loaded_at", timedelta(hours=1))

    assert result.passed
    assert result.failed_count == 0


def test_check_freshness_flags_stale_timestamp(spark: SparkSession) -> None:
    """check_freshness fails when the newest timestamp is older than max_age."""
    stale = datetime.now(UTC) - timedelta(hours=2)
    df = spark.createDataFrame([Row(loaded_at=stale)])

    result = check_freshness(df, "loaded_at", timedelta(hours=1))

    assert not result.passed
    assert result.failed_count == 1
    assert result.check_name == "freshness_loaded_at"


def test_check_freshness_flags_empty_dataframe_as_stale(spark: SparkSession) -> None:
    """check_freshness fails when there are no rows to derive a max timestamp from."""
    df = spark.createDataFrame([], schema="loaded_at timestamp")

    result = check_freshness(df, "loaded_at", timedelta(hours=1))

    assert not result.passed
    assert result.failed_count == 1


def test_check_freshness_defaults_to_warn_severity(spark: SparkSession) -> None:
    """check_freshness defaults to "warn" severity, unlike the other checks."""
    df = spark.createDataFrame([Row(loaded_at=datetime.now(UTC))])

    result = check_freshness(df, "loaded_at", timedelta(hours=1))

    assert result.severity == "warn"
