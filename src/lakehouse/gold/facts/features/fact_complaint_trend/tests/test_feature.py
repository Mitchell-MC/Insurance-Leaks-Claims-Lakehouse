"""Tests for build_fact_complaint_trend's placeholder shape."""

from pyspark.sql import SparkSession

from lakehouse.gold.facts.features.fact_complaint_trend.feature import build_fact_complaint_trend


def test_build_fact_complaint_trend_is_empty_with_correct_schema(spark: SparkSession) -> None:
    """The placeholder table has zero rows but the documented four-column schema."""
    df = build_fact_complaint_trend(spark)

    assert df.count() == 0
    assert df.columns == ["date_key", "geography_key", "complaint_count", "complaint_rate_trend"]
