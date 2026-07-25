"""Tests for add_rolling_event_intensity's trailing-window aggregation."""

from datetime import date

from pyspark.sql import Row, SparkSession

from lakehouse.processing.features.rolling_event_intensity.feature import (
    add_rolling_event_intensity,
)


def test_rolling_windows_include_only_events_within_range(spark: SparkSession) -> None:
    """An event 10 days back counts toward the 30-day window but not the 7-day one."""
    df = spark.createDataFrame(
        [
            Row(REGION="TX", EVENT_DATE=date(2020, 8, 1), DAMAGE_PROPERTY_USD=10_000.0),
            Row(REGION="TX", EVENT_DATE=date(2020, 8, 11), DAMAGE_PROPERTY_USD=20_000.0),
        ]
    )

    result = {row["EVENT_DATE"]: row for row in add_rolling_event_intensity(df).collect()}
    latest = result[date(2020, 8, 11)]

    assert latest["rolling_7d_event_count"] == 1
    assert latest["rolling_7d_damage_usd"] == 20_000.0
    assert latest["rolling_30d_event_count"] == 2
    assert latest["rolling_30d_damage_usd"] == 30_000.0


def test_rolling_windows_are_partitioned_by_region(spark: SparkSession) -> None:
    """Events in a different region don't contribute to another region's rolling count."""
    df = spark.createDataFrame(
        [
            Row(REGION="TX", EVENT_DATE=date(2020, 8, 1), DAMAGE_PROPERTY_USD=10_000.0),
            Row(REGION="FL", EVENT_DATE=date(2020, 8, 1), DAMAGE_PROPERTY_USD=5_000.0),
        ]
    )

    result = {row["REGION"]: row for row in add_rolling_event_intensity(df).collect()}

    assert result["TX"]["rolling_30d_event_count"] == 1
    assert result["FL"]["rolling_30d_event_count"] == 1
