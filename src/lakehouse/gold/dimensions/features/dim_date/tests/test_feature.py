"""Tests for build_dim_date's calendar generation."""

from datetime import date

from pyspark.sql import SparkSession

from lakehouse.gold.dimensions.features.dim_date.feature import build_dim_date


def test_build_dim_date_covers_full_range_inclusive(spark: SparkSession) -> None:
    """build_dim_date produces one row per day, inclusive of both endpoints."""
    df = build_dim_date(spark, date(2020, 1, 1), date(2020, 1, 3))

    assert df.count() == 3
    assert {row["date"] for row in df.collect()} == {
        date(2020, 1, 1),
        date(2020, 1, 2),
        date(2020, 1, 3),
    }


def test_build_dim_date_derives_calendar_attributes(spark: SparkSession) -> None:
    """build_dim_date derives date_key, year, quarter, and weekend flag correctly."""
    df = build_dim_date(spark, date(2020, 8, 1), date(2020, 8, 1))  # a Saturday

    row = df.collect()[0]
    assert row["date_key"] == 20200801
    assert row["year"] == 2020
    assert row["quarter"] == 3
    assert row["is_weekend"] is True
