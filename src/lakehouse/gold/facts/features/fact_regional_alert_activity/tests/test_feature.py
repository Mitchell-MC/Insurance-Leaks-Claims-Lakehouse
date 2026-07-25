"""Tests for build_fact_regional_alert_activity's snapshot join."""

from datetime import date

from pyspark.sql import DataFrame, Row, SparkSession

from lakehouse.gold.dimensions.features.dim_date.feature import build_dim_date
from lakehouse.gold.dimensions.features.dim_geography_state.feature import (
    build_dim_geography_state,
)
from lakehouse.gold.facts.features.fact_regional_alert_activity.feature import (
    build_fact_regional_alert_activity,
)


def _geography_df(spark: SparkSession) -> DataFrame:
    """Gazetteer rows with MULTIPLE counties per state.

    Reason: a single-county-per-state fixture cannot detect the state-vs-county
    grain fan-out this fact previously had -- see dim_geography_state.
    """
    return spark.createDataFrame(
        [
            Row(USPS="TX", GEOID="48201", NAME="Harris County"),
            Row(USPS="TX", GEOID="48113", NAME="Dallas County"),
            Row(USPS="TX", GEOID="48029", NAME="Bexar County"),
            Row(USPS="FL", GEOID="12086", NAME="Miami-Dade County"),
        ]
    )


def test_build_fact_regional_alert_activity_attaches_dimension_keys(spark: SparkSession) -> None:
    """Each region's pressure score is attached to its date_key and state_geography_key."""
    pressure_df = spark.createDataFrame([Row(REGION="TX", claims_surge_risk=0.75)])
    dim_date = build_dim_date(spark, date(2020, 1, 1), date(2020, 1, 5))
    dim_geography_state = build_dim_geography_state(_geography_df(spark))

    result = build_fact_regional_alert_activity(
        pressure_df, date(2020, 1, 3), dim_date, dim_geography_state
    ).collect()

    assert result[0]["date_key"] == 20200103
    assert result[0]["claims_surge_risk"] == 0.75
    assert result[0]["state_geography_key"] is not None


def test_does_not_fan_out_across_counties_in_a_state(spark: SparkSession) -> None:
    """Grain stays one row per region even when a state has many counties.

    Regression: joining the county-grain `dim_geography` on `state` alone
    multiplied every region's `claims_surge_risk` row by that state's county
    count (254x for real Texas Gazetteer data), inflating KPI-1 region counts.
    """
    pressure_df = spark.createDataFrame(
        [
            Row(REGION="TX", claims_surge_risk=0.75),
            Row(REGION="FL", claims_surge_risk=0.40),
        ]
    )
    dim_date = build_dim_date(spark, date(2020, 1, 1), date(2020, 1, 5))
    # 3 TX counties + 1 FL county -- a fan-out would yield 3 TX rows.
    dim_geography_state = build_dim_geography_state(_geography_df(spark))

    result = build_fact_regional_alert_activity(
        pressure_df, date(2020, 1, 3), dim_date, dim_geography_state
    )

    assert result.count() == 2, "expected exactly one row per region"
    assert result.select("state_geography_key").distinct().count() == 2
    assert result.filter(result["state_geography_key"].isNull()).count() == 0
