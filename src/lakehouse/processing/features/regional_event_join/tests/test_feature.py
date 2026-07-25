"""Tests for join_events_to_declarations's region/date-window join logic."""

from datetime import date

from pyspark.sql import Row, SparkSession

from lakehouse.processing.features.regional_event_join.feature import (
    join_events_to_declarations,
)


def test_join_matches_event_within_window(spark: SparkSession) -> None:
    """A storm event within the date window of a same-state declaration is matched."""
    noaa_df = spark.createDataFrame(
        [
            Row(
                STATE="TX",
                EVENT_DATE=date(2020, 8, 25),
                EVENT_TYPE="Hurricane (Typhoon)",
                SEVERITY_BAND="severe",
                DAMAGE_PROPERTY_USD=1_000_000.0,
            )
        ]
    )
    fema_df = spark.createDataFrame(
        [
            Row(
                state="TX",
                disasterNumber=4586,
                incidentType="Hurricane",
                declarationDate=date(2020, 8, 27),
                incidentBeginDate=date(2020, 8, 23),
            )
        ]
    )

    result = join_events_to_declarations(noaa_df, fema_df).collect()

    assert result[0]["disasterNumber"] == 4586
    assert result[0]["REGION"] == "TX"


def test_join_leaves_unmatched_event_with_null_declaration(spark: SparkSession) -> None:
    """A storm event outside the window (or different state) gets null FEMA columns."""
    noaa_df = spark.createDataFrame(
        [
            Row(
                STATE="TX",
                EVENT_DATE=date(2020, 1, 1),
                EVENT_TYPE="Thunderstorm Wind",
                SEVERITY_BAND="minor",
                DAMAGE_PROPERTY_USD=0.0,
            )
        ]
    )
    fema_df = spark.createDataFrame(
        [
            Row(
                state="TX",
                disasterNumber=4586,
                incidentType="Hurricane",
                declarationDate=date(2020, 8, 27),
                incidentBeginDate=date(2020, 8, 23),
            )
        ]
    )

    result = join_events_to_declarations(noaa_df, fema_df).collect()

    assert result[0]["disasterNumber"] is None
