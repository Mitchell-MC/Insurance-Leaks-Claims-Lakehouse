"""Tests for build_fact_catastrophe_event's declaration-grain aggregation."""

from datetime import date

from pyspark.sql import DataFrame, Row, SparkSession

from lakehouse.gold.dimensions.features.dim_date.feature import build_dim_date
from lakehouse.gold.dimensions.features.dim_geography.feature import build_dim_geography
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
    dim_geography = build_dim_geography(
        spark.createDataFrame([Row(USPS="TX", GEOID="48201", NAME="Harris County")])
    )

    result = {
        row["disasterNumber"]: row
        for row in build_fact_catastrophe_event(
            events_df, fema_df, dim_date, dim_geography
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
    dim_geography = build_dim_geography(
        spark.createDataFrame(
            [
                Row(USPS="TX", GEOID="48201", NAME="Harris County"),
                Row(USPS="FL", GEOID="12086", NAME="Miami-Dade County"),
            ]
        )
    )

    result = {
        row["disasterNumber"]: row
        for row in build_fact_catastrophe_event(
            events_df, fema_df, dim_date, dim_geography
        ).collect()
    }

    row = result[9999]
    assert row["matched_storm_event_count"] == 0
    assert row["total_damage_property_usd"] == 0.0
    assert row["days_to_declaration"] == 5
