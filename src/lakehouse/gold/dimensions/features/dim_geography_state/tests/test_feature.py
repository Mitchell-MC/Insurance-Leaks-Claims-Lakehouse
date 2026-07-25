"""Tests for build_dim_geography_state's state-grain collapse."""

from pyspark.sql import Row, SparkSession

from lakehouse.gold.dimensions.features.dim_geography_state.feature import (
    build_dim_geography_state,
)


def test_collapses_counties_to_one_row_per_state(spark: SparkSession) -> None:
    """Many counties per state collapse to exactly one dimension row per state."""
    geography_df = spark.createDataFrame(
        [
            Row(USPS="TX", GEOID="48201", NAME="Harris County"),
            Row(USPS="TX", GEOID="48113", NAME="Dallas County"),
            Row(USPS="TX", GEOID="48029", NAME="Bexar County"),
            Row(USPS="FL", GEOID="12086", NAME="Miami-Dade County"),
        ]
    )

    result = {row["state"]: row for row in build_dim_geography_state(geography_df).collect()}

    assert len(result) == 2
    assert result["TX"]["county_count"] == 3
    assert result["FL"]["county_count"] == 1


def test_surrogate_keys_are_unique(spark: SparkSession) -> None:
    """Every state gets a distinct surrogate key, so fact joins stay 1:1."""
    geography_df = spark.createDataFrame(
        [
            Row(USPS="TX", GEOID="48201", NAME="Harris County"),
            Row(USPS="FL", GEOID="12086", NAME="Miami-Dade County"),
            Row(USPS="LA", GEOID="22071", NAME="Orleans Parish"),
        ]
    )

    result = build_dim_geography_state(geography_df)

    assert result.select("state_geography_key").distinct().count() == 3
