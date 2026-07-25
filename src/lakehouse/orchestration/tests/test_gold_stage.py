"""Tests for the Gold dimensional-modeling stage orchestrator."""

from datetime import date

from pyspark.sql import Row, SparkSession

from lakehouse.orchestration.gold_stage import run_gold_stage
from lakehouse.orchestration.process_stage import run_process_stage


def test_run_gold_stage_builds_every_table(spark: SparkSession) -> None:
    """run_gold_stage returns all four dimensions, three facts, and the leakage metric."""
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
                designatedArea="Harris (County)",
            )
        ]
    )
    geography_df = spark.createDataFrame([Row(USPS="TX", GEOID="48201", NAME="Harris County")])
    nws_df = spark.createDataFrame(
        [Row(severity="Extreme", urgency="Immediate", certainty="Observed", areaDesc="Harris, TX")]
    )

    enriched_events_df, pressure_score_df = run_process_stage(noaa_df, fema_df, nws_df)

    tables = run_gold_stage(
        spark,
        noaa_df,
        fema_df,
        geography_df,
        nws_df,
        enriched_events_df,
        pressure_score_df,
        as_of_date=date(2020, 8, 27),
        dim_date_start=date(2020, 1, 1),
        dim_date_end=date(2020, 12, 31),
    )

    assert set(tables) == {
        "dim_date",
        "dim_geography",
        "dim_event_type",
        "dim_alert_status",
        "fact_catastrophe_event",
        "fact_regional_alert_activity",
        "fact_complaint_trend",
        "leakage_risk_metric",
    }
    assert tables["fact_catastrophe_event"].count() == 1
    assert tables["leakage_risk_metric"].count() == 1
