"""Tests for build_dim_geography's surrogate-key assignment."""

from pyspark.sql import Row, SparkSession

from lakehouse.gold.dimensions.features.dim_geography.feature import build_dim_geography


def test_build_dim_geography_assigns_surrogate_keys(spark: SparkSession) -> None:
    """build_dim_geography renames columns and assigns a unique geography_key per row."""
    df = spark.createDataFrame(
        [
            Row(USPS="TX", GEOID="48201", NAME="Harris County"),
            Row(USPS="FL", GEOID="12086", NAME="Miami-Dade County"),
        ]
    )

    result = build_dim_geography(df).collect()

    assert {row["state"] for row in result} == {"TX", "FL"}
    assert len({row["geography_key"] for row in result}) == 2
