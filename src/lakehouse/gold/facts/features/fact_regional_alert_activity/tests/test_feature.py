"""Tests for build_fact_regional_alert_activity's snapshot join."""

from datetime import date

from pyspark.sql import Row, SparkSession

from lakehouse.gold.dimensions.features.dim_date.feature import build_dim_date
from lakehouse.gold.dimensions.features.dim_geography.feature import build_dim_geography
from lakehouse.gold.facts.features.fact_regional_alert_activity.feature import (
    build_fact_regional_alert_activity,
)


def test_build_fact_regional_alert_activity_attaches_dimension_keys(spark: SparkSession) -> None:
    """Each region's pressure score is attached to its date_key and geography_key."""
    pressure_df = spark.createDataFrame([Row(REGION="TX", claims_surge_risk=0.75)])
    dim_date = build_dim_date(spark, date(2020, 1, 1), date(2020, 1, 5))
    dim_geography = build_dim_geography(
        spark.createDataFrame([Row(USPS="TX", GEOID="48201", NAME="Harris County")])
    )

    result = build_fact_regional_alert_activity(
        pressure_df, date(2020, 1, 3), dim_date, dim_geography
    ).collect()

    assert result[0]["date_key"] == 20200103
    assert result[0]["claims_surge_risk"] == 0.75
    assert result[0]["geography_key"] is not None
