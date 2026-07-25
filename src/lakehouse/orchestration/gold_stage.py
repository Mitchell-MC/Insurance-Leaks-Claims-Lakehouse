"""Orchestrates the Gold dimensional-modeling stage."""

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.gold.dimensions.features.dim_alert_status.feature import build_dim_alert_status
from lakehouse.gold.dimensions.features.dim_date.feature import build_dim_date
from lakehouse.gold.dimensions.features.dim_event_type.feature import build_dim_event_type
from lakehouse.gold.dimensions.features.dim_geography.feature import build_dim_geography
from lakehouse.gold.dimensions.features.dim_geography_state.feature import (
    build_dim_geography_state,
)
from lakehouse.gold.facts.features.fact_catastrophe_event.feature import (
    build_fact_catastrophe_event,
)
from lakehouse.gold.facts.features.fact_complaint_trend.feature import build_fact_complaint_trend
from lakehouse.gold.facts.features.fact_regional_alert_activity.feature import (
    build_fact_regional_alert_activity,
)
from lakehouse.gold.features.leakage_risk_metric.feature import calculate_leakage_exposure_proxy
from lakehouse.orchestration.run_logger import log_stage_run
from lakehouse.silver.data_quality.checks import (
    DQCheckResult,
    check_expression,
    check_referential_integrity,
    raise_on_failures,
)


def _build_declaration_lag(fema_silver_df: DataFrame) -> DataFrame:
    """Aggregates Silver FEMA declarations into a per-region average declaration lag.

    Args:
        fema_silver_df (DataFrame): Silver FEMA declarations.

    Returns:
        DataFrame: `REGION`, `avg_days_event_to_declaration` (KPI 3, per region).
    """
    return (
        fema_silver_df.withColumn(
            "_days", F.datediff(F.col("declarationDate"), F.col("incidentBeginDate"))
        )
        .groupBy(F.col("state").alias("REGION"))
        .agg(F.avg("_days").alias("avg_days_event_to_declaration"))
    )


def _build_complaint_trend_proxy(pressure_score_df: DataFrame) -> DataFrame:
    """Builds the KPI-2 fallback proxy input (see docs/data_limitations.md).

    Reason: a real period-over-period trend needs yesterday's persisted
    `gold.fact_regional_alert_activity` snapshot, not derivable from a single
    stateless run -- so this defaults to "no trend signal" (0.0) until the
    orchestrated job (`infra/jobs.tf`) has enough run history to diff against.

    Args:
        pressure_score_df (DataFrame): `REGION`, `claims_surge_risk`.

    Returns:
        DataFrame: `REGION`, `complaint_rate_trend_proxy`.
    """
    return pressure_score_df.select(F.col("REGION"), F.lit(0.0).alias("complaint_rate_trend_proxy"))


def run_gold_dq_checks(
    dim_date: DataFrame,
    dim_geography_state: DataFrame,
    fact_catastrophe_event: DataFrame,
    fact_regional_alert_activity: DataFrame,
) -> list[DQCheckResult]:
    """Runs Gold-layer referential-integrity and business-rule checks.

    Mirrors dbt's `relationships` and `expression_is_true` generic tests --
    the Gold facts here are built with left joins against the dimensions
    (see fact_catastrophe_event/feature.py, fact_regional_alert_activity/
    feature.py), so a state code or date that fails to reconcile would
    otherwise surface only as a silent null foreign key.

    Args:
        dim_date (DataFrame): Gold `dim_date`.
        dim_geography_state (DataFrame): Gold `dim_geography_state`.
        fact_catastrophe_event (DataFrame): Built `fact_catastrophe_event`.
        fact_regional_alert_activity (DataFrame): Built `fact_regional_alert_activity`.

    Returns:
        list[DQCheckResult]: One result per check run.
    """
    return [
        check_referential_integrity(
            fact_catastrophe_event,
            dim_geography_state,
            "state_geography_key",
            "state_geography_key",
            check_name="fact_catastrophe_event_referential_integrity_state_geography_key",
        ),
        check_referential_integrity(
            fact_catastrophe_event,
            dim_date,
            "date_key",
            "date_key",
            check_name="fact_catastrophe_event_referential_integrity_date_key",
        ),
        check_referential_integrity(
            fact_regional_alert_activity,
            dim_geography_state,
            "state_geography_key",
            "state_geography_key",
            check_name="fact_regional_alert_activity_referential_integrity_state_geography_key",
        ),
        check_referential_integrity(
            fact_regional_alert_activity,
            dim_date,
            "date_key",
            "date_key",
            check_name="fact_regional_alert_activity_referential_integrity_date_key",
        ),
        check_expression(
            fact_catastrophe_event,
            F.col("days_to_declaration") >= 0,
            "days_to_declaration_non_negative",
        ),
        check_expression(
            fact_catastrophe_event,
            F.col("total_damage_property_usd") >= 0,
            "total_damage_property_usd_non_negative",
        ),
    ]


def run_gold_stage(
    spark: SparkSession,
    noaa_silver_df: DataFrame,
    fema_silver_df: DataFrame,
    geography_silver_df: DataFrame,
    nws_bronze_df: DataFrame,
    enriched_events_df: DataFrame,
    pressure_score_df: DataFrame,
    as_of_date: date,
    dim_date_start: date,
    dim_date_end: date,
) -> dict[str, DataFrame]:
    """Builds every Gold dimension, fact, and derived-metric table.

    Args:
        spark (SparkSession): Active Spark session.
        noaa_silver_df (DataFrame): Silver NOAA storm events.
        fema_silver_df (DataFrame): Silver FEMA declarations.
        geography_silver_df (DataFrame): Silver Census Gazetteer counties.
        nws_bronze_df (DataFrame): Bronze NWS active-alert snapshot.
        enriched_events_df (DataFrame): From `orchestration.process_stage.run_process_stage`.
        pressure_score_df (DataFrame): From `orchestration.process_stage.run_process_stage`.
        as_of_date (date): Snapshot date for `fact_regional_alert_activity`.
        dim_date_start (date): First date `dim_date` should cover.
        dim_date_end (date): Last date `dim_date` should cover.

    Returns:
        dict[str, DataFrame]: Gold table name -> built DataFrame. Writing is
            left to the caller so this stays testable without live storage.
    """
    with log_stage_run("gold.dimensions"):
        dim_date = build_dim_date(spark, dim_date_start, dim_date_end)
        dim_geography = build_dim_geography(geography_silver_df)
        dim_geography_state = build_dim_geography_state(geography_silver_df)
        dim_event_type = build_dim_event_type(noaa_silver_df)
        dim_alert_status = build_dim_alert_status(nws_bronze_df)

    with log_stage_run("gold.facts"):
        fact_catastrophe_event = build_fact_catastrophe_event(
            enriched_events_df, fema_silver_df, dim_date, dim_geography_state
        )
        fact_regional_alert_activity = build_fact_regional_alert_activity(
            pressure_score_df, as_of_date, dim_date, dim_geography_state
        )
        fact_complaint_trend = build_fact_complaint_trend(spark)

    with log_stage_run("gold.leakage_risk_metric"):
        declaration_lag_df = _build_declaration_lag(fema_silver_df)
        complaint_trend_proxy_df = _build_complaint_trend_proxy(pressure_score_df)
        leakage_risk_metric = calculate_leakage_exposure_proxy(
            pressure_score_df, complaint_trend_proxy_df, declaration_lag_df
        )

    with log_stage_run("gold.dq_checks"):
        raise_on_failures(
            run_gold_dq_checks(
                dim_date, dim_geography_state, fact_catastrophe_event, fact_regional_alert_activity
            )
        )

    return {
        "dim_date": dim_date,
        "dim_geography": dim_geography,
        "dim_geography_state": dim_geography_state,
        "dim_event_type": dim_event_type,
        "dim_alert_status": dim_alert_status,
        "fact_catastrophe_event": fact_catastrophe_event,
        "fact_regional_alert_activity": fact_regional_alert_activity,
        "fact_complaint_trend": fact_complaint_trend,
        "leakage_risk_metric": leakage_risk_metric,
    }
