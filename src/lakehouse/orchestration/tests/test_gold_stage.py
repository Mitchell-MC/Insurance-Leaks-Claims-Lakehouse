"""Tests for the Gold dimensional-modeling stage orchestrator."""

from datetime import date

import pytest
from pyspark.sql import Row, SparkSession

from lakehouse.orchestration.gold_stage import run_gold_dq_checks, run_gold_stage
from lakehouse.orchestration.process_stage import run_process_stage
from lakehouse.silver.data_quality.checks import DQCheckFailure


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
        "dim_geography_state",
        "dim_event_type",
        "dim_alert_status",
        "fact_catastrophe_event",
        "fact_regional_alert_activity",
        "fact_complaint_trend",
        "leakage_risk_metric",
    }
    assert tables["fact_catastrophe_event"].count() == 1
    assert tables["leakage_risk_metric"].count() == 1


def test_run_gold_dq_checks_passes_for_well_formed_tables(spark: SparkSession) -> None:
    """run_gold_dq_checks reports every check passing for reconciled, valid facts."""
    dim_date = spark.createDataFrame([Row(date_key=20200827)])
    dim_geography_state = spark.createDataFrame([Row(state_geography_key=1)])
    fact_catastrophe_event = spark.createDataFrame(
        [
            Row(
                date_key=20200827,
                state_geography_key=1,
                days_to_declaration=4,
                total_damage_property_usd=100.0,
            )
        ]
    )
    fact_regional_alert_activity = spark.createDataFrame(
        [Row(date_key=20200827, state_geography_key=1)]
    )

    results = run_gold_dq_checks(
        dim_date, dim_geography_state, fact_catastrophe_event, fact_regional_alert_activity
    )

    assert all(result.passed for result in results)


def test_run_gold_dq_checks_flags_orphaned_foreign_key(spark: SparkSession) -> None:
    """run_gold_dq_checks flags a fact row whose state_geography_key isn't in the dimension.

    Mirrors dbt's `relationships` test -- a left join that silently drops a
    match would otherwise surface only as a null foreign key, not a failure.
    """
    dim_date = spark.createDataFrame([Row(date_key=20200827)])
    dim_geography_state = spark.createDataFrame([Row(state_geography_key=1)])
    fact_catastrophe_event = spark.createDataFrame(
        [
            Row(
                date_key=20200827,
                state_geography_key=99,
                days_to_declaration=4,
                total_damage_property_usd=100.0,
            )
        ]
    )
    fact_regional_alert_activity = spark.createDataFrame(
        [Row(date_key=20200827, state_geography_key=1)]
    )

    results = {
        result.check_name: result
        for result in run_gold_dq_checks(
            dim_date, dim_geography_state, fact_catastrophe_event, fact_regional_alert_activity
        )
    }

    assert not results["fact_catastrophe_event_referential_integrity_state_geography_key"].passed
    assert results["fact_regional_alert_activity_referential_integrity_state_geography_key"].passed


def test_run_gold_dq_checks_flags_negative_declaration_lag(spark: SparkSession) -> None:
    """run_gold_dq_checks flags a negative days_to_declaration as a business-rule violation."""
    dim_date = spark.createDataFrame([Row(date_key=20200827)])
    dim_geography_state = spark.createDataFrame([Row(state_geography_key=1)])
    fact_catastrophe_event = spark.createDataFrame(
        [
            Row(
                date_key=20200827,
                state_geography_key=1,
                days_to_declaration=-1,
                total_damage_property_usd=100.0,
            )
        ]
    )
    fact_regional_alert_activity = spark.createDataFrame(
        [Row(date_key=20200827, state_geography_key=1)]
    )

    results = {
        result.check_name: result
        for result in run_gold_dq_checks(
            dim_date, dim_geography_state, fact_catastrophe_event, fact_regional_alert_activity
        )
    }

    assert not results["days_to_declaration_non_negative"].passed


def test_run_gold_stage_raises_when_geography_fails_to_reconcile(spark: SparkSession) -> None:
    """run_gold_stage raises DQCheckFailure when a FEMA state code has no matching dimension row.

    Regression guard: `dim_geography_state` is derived from the Gazetteer's
    `USPS` codes (see dim_geography_state/feature.py), so a FEMA `state`
    value that doesn't reconcile against it -- a typo, a territory code the
    Gazetteer doesn't carry, etc. -- silently produced a null
    `state_geography_key` before this check existed.
    """
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
                state="ZZ",
                disasterNumber=4586,
                incidentType="Hurricane",
                declarationDate=date(2020, 8, 27),
                incidentBeginDate=date(2020, 8, 23),
                designatedArea="Nowhere (County)",
            )
        ]
    )
    geography_df = spark.createDataFrame([Row(USPS="TX", GEOID="48201", NAME="Harris County")])
    nws_df = spark.createDataFrame(
        [Row(severity="Extreme", urgency="Immediate", certainty="Observed", areaDesc="Harris, TX")]
    )

    enriched_events_df, pressure_score_df = run_process_stage(noaa_df, fema_df, nws_df)

    with pytest.raises(DQCheckFailure, match="referential_integrity_state_geography_key"):
        run_gold_stage(
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
