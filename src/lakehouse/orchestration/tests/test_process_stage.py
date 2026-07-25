"""Tests for the Phase 4 processing stage orchestrator."""

from datetime import date

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

from lakehouse.orchestration.process_stage import (
    aggregate_alert_severity,
    aggregate_declaration_frequency,
    aggregate_latest_storm_intensity,
    extract_state_from_area_desc,
    run_process_stage,
)


def test_extract_state_from_area_desc_pulls_trailing_code(spark: SparkSession) -> None:
    """extract_state_from_area_desc pulls the trailing 2-letter state code."""
    df = spark.createDataFrame([Row(area_desc="Harris, TX; Galveston, TX")]).select(
        extract_state_from_area_desc(F.col("area_desc")).alias("region")
    )

    assert df.collect()[0]["region"] == "TX"


def test_aggregate_alert_severity_takes_max_per_region(spark: SparkSession) -> None:
    """aggregate_alert_severity keeps the highest severity score per region."""
    df = spark.createDataFrame(
        [
            Row(severity="Moderate", areaDesc="Harris, TX"),
            Row(severity="Extreme", areaDesc="Galveston, TX"),
        ]
    )

    result = {
        row["REGION"]: row["active_alert_severity_score"]
        for row in aggregate_alert_severity(df).collect()
    }

    assert result["TX"] == 4.0


def test_aggregate_declaration_frequency_counts_per_region(spark: SparkSession) -> None:
    """aggregate_declaration_frequency counts declarations per state."""
    df = spark.createDataFrame([Row(state="TX"), Row(state="TX"), Row(state="FL")])

    result = {
        row["REGION"]: row["historical_declaration_frequency"]
        for row in aggregate_declaration_frequency(df).collect()
    }

    assert result["TX"] == 2
    assert result["FL"] == 1


def test_aggregate_latest_storm_intensity_picks_most_recent_event(spark: SparkSession) -> None:
    """aggregate_latest_storm_intensity keeps only the latest-dated row per region."""
    df = spark.createDataFrame(
        [
            Row(REGION="TX", EVENT_DATE=date(2020, 8, 1), rolling_30d_damage_usd=100.0),
            Row(REGION="TX", EVENT_DATE=date(2020, 8, 15), rolling_30d_damage_usd=500.0),
        ]
    )

    result = aggregate_latest_storm_intensity(df).collect()

    assert result[0]["rolling_30d_storm_intensity"] == 500.0


def test_run_process_stage_produces_enriched_events_and_pressure_scores(
    spark: SparkSession,
) -> None:
    """run_process_stage wires the join, rolling metrics, and pressure score together."""
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
    nws_df = spark.createDataFrame([Row(severity="Extreme", areaDesc="Harris, TX")])

    enriched_events_df, pressure_score_df = run_process_stage(noaa_df, fema_df, nws_df)

    assert enriched_events_df.count() == 1
    assert pressure_score_df.collect()[0]["REGION"] == "TX"
