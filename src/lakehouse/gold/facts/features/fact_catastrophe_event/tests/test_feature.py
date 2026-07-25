"""Tests for build_fact_catastrophe_event's declaration-grain aggregation."""

from datetime import date

from pyspark.sql import DataFrame, Row, SparkSession

from lakehouse.gold.dimensions.features.dim_date.feature import build_dim_date
from lakehouse.gold.dimensions.features.dim_geography_state.feature import (
    build_dim_geography_state,
)
from lakehouse.gold.facts.features.fact_catastrophe_event.feature import (
    build_fact_catastrophe_event,
)


def _fema_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [
            Row(
                disasterNumber=4586,
                state="TX",
                incidentType="Hurricane",
                declarationDate=date(2020, 8, 27),
                incidentBeginDate=date(2020, 8, 23),
                designatedArea="Harris (County)",
            ),
            Row(
                disasterNumber=9999,
                state="FL",
                incidentType="Flood",
                declarationDate=date(2021, 1, 10),
                incidentBeginDate=date(2021, 1, 5),
                designatedArea="Dade (County)",
            ),
        ]
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
            Row(USPS="FL", GEOID="12011", NAME="Broward County"),
        ]
    )


def test_aggregates_matched_storm_events_to_declaration_grain(spark: SparkSession) -> None:
    """A declaration with two matched storm events gets summed damage and peak rolling count."""
    fema_df = _fema_df(spark)
    events_df = spark.createDataFrame(
        [
            Row(
                REGION="TX",
                disasterNumber=4586,
                DAMAGE_PROPERTY_USD=100.0,
                rolling_30d_event_count=1,
            ),
            Row(
                REGION="TX",
                disasterNumber=4586,
                DAMAGE_PROPERTY_USD=200.0,
                rolling_30d_event_count=2,
            ),
        ]
    )
    dim_date = build_dim_date(spark, date(2020, 1, 1), date(2021, 12, 31))
    dim_geography_state = build_dim_geography_state(_geography_df(spark))

    result = {
        row["disasterNumber"]: row
        for row in build_fact_catastrophe_event(
            events_df, fema_df, dim_date, dim_geography_state
        ).collect()
    }

    row = result[4586]
    assert row["days_to_declaration"] == 4
    assert row["matched_storm_event_count"] == 2
    assert row["total_damage_property_usd"] == 300.0
    assert row["peak_rolling_30d_event_count"] == 2
    assert row["date_key"] == 20200827


def test_declaration_with_no_matched_events_gets_zeroed_measures(spark: SparkSession) -> None:
    """A declaration with zero matched storm events gets coalesced-to-zero measures, not nulls."""
    fema_df = _fema_df(spark)
    events_df = spark.createDataFrame(
        [
            Row(
                REGION="TX",
                disasterNumber=4586,
                DAMAGE_PROPERTY_USD=100.0,
                rolling_30d_event_count=1,
            )
        ]
    )
    dim_date = build_dim_date(spark, date(2020, 1, 1), date(2021, 12, 31))
    dim_geography_state = build_dim_geography_state(_geography_df(spark))

    result = {
        row["disasterNumber"]: row
        for row in build_fact_catastrophe_event(
            events_df, fema_df, dim_date, dim_geography_state
        ).collect()
    }

    row = result[9999]
    assert row["matched_storm_event_count"] == 0
    assert row["total_damage_property_usd"] == 0.0
    assert row["days_to_declaration"] == 5


def test_does_not_fan_out_across_counties_in_a_state(spark: SparkSession) -> None:
    """Grain stays one row per declaration even when a state has many counties.

    Regression: joining the county-grain `dim_geography` on `state` alone
    multiplied every declaration by that state's county count (254x for real
    Texas Gazetteer data), silently inflating KPI-3's declaration counts.
    """
    fema_df = _fema_df(spark)
    events_df = spark.createDataFrame(
        [
            Row(
                REGION="TX",
                disasterNumber=4586,
                DAMAGE_PROPERTY_USD=100.0,
                rolling_30d_event_count=1,
            )
        ]
    )
    dim_date = build_dim_date(spark, date(2020, 1, 1), date(2021, 12, 31))
    # 3 TX counties + 2 FL counties -- a fan-out would yield 3 and 2 rows.
    dim_geography_state = build_dim_geography_state(_geography_df(spark))

    result = build_fact_catastrophe_event(events_df, fema_df, dim_date, dim_geography_state)

    assert result.count() == 2, "expected exactly one row per declaration"
    assert result.select("disasterNumber").distinct().count() == 2
    assert result.filter(result["disasterNumber"] == 4586).count() == 1
    assert result.filter(result["state_geography_key"].isNull()).count() == 0
