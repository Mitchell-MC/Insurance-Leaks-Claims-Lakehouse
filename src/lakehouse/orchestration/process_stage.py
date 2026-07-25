"""Orchestrates the Phase 4 processing stage: join, enrich, and score regions by risk.

Assembles the three per-region inputs `catastrophe_pressure_score` needs
(active alert severity, rolling storm intensity, historical declaration
frequency) from Bronze/Silver data, since those aggregations don't live in
any single upstream table.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from lakehouse.orchestration.run_logger import log_stage_run
from lakehouse.processing.features.catastrophe_pressure_score.feature import (
    calculate_catastrophe_pressure_score,
)
from lakehouse.processing.features.regional_event_join.feature import (
    join_events_to_declarations,
)
from lakehouse.processing.features.rolling_event_intensity.feature import (
    add_rolling_event_intensity,
)

_SEVERITY_SCORES = {"EXTREME": 4.0, "SEVERE": 3.0, "MODERATE": 2.0, "MINOR": 1.0}


def extract_state_from_area_desc(area_desc: Column) -> Column:
    """Extracts a trailing 2-letter USPS state code from an NWS `areaDesc` string.

    NWS alert area descriptions are free text ending in a state code by
    convention (e.g. "Harris, TX; Galveston, TX" -> "TX").

    Args:
        area_desc (Column): Raw `areaDesc` column from Bronze NWS alerts.

    Returns:
        Column: The extracted 2-letter code, or empty string if none is found.
    """
    return F.regexp_extract(area_desc, r"([A-Z]{2})(;[^;]*)?$", 1)


def aggregate_alert_severity(alerts_df: DataFrame) -> DataFrame:
    """Aggregates Bronze NWS alerts into a per-region active-alert severity score.

    Args:
        alerts_df (DataFrame): Bronze NWS alerts (`severity`, `areaDesc`).

    Returns:
        DataFrame: `REGION`, `active_alert_severity_score` (max severity
            score among currently active alerts for that region).
    """
    severity_map = F.create_map(*[F.lit(x) for pair in _SEVERITY_SCORES.items() for x in pair])
    scored = alerts_df.withColumn(
        "_severity_score", F.coalesce(severity_map[F.upper(F.col("severity"))], F.lit(0.0))
    ).withColumn("REGION", extract_state_from_area_desc(F.col("areaDesc")))
    return (
        scored.filter(F.col("REGION") != "")
        .groupBy("REGION")
        .agg(F.max("_severity_score").alias("active_alert_severity_score"))
    )


def aggregate_declaration_frequency(fema_df: DataFrame) -> DataFrame:
    """Aggregates Silver FEMA declarations into a per-region historical count.

    Args:
        fema_df (DataFrame): Silver FEMA declarations (`state`).

    Returns:
        DataFrame: `REGION`, `historical_declaration_frequency` (declaration count).
    """
    return fema_df.groupBy(F.col("state").alias("REGION")).agg(
        F.count(F.lit(1)).alias("historical_declaration_frequency")
    )


def aggregate_latest_storm_intensity(enriched_events_df: DataFrame) -> DataFrame:
    """Extracts each region's most recent rolling-30-day storm intensity.

    Args:
        enriched_events_df (DataFrame): Output of `add_rolling_event_intensity`
            (`REGION`, `EVENT_DATE`, `rolling_30d_damage_usd`).

    Returns:
        DataFrame: `REGION`, `rolling_30d_storm_intensity` (latest damage sum).
    """
    # Reason: aliasing both sides avoids Spark's ambiguous-self-join error,
    # since `latest_per_region` is derived from `enriched_events_df` itself.
    events = enriched_events_df.alias("events")
    latest_per_region = (
        events.groupBy("REGION").agg(F.max("EVENT_DATE").alias("_latest_date")).alias("latest")
    )
    joined = events.join(
        latest_per_region,
        (F.col("events.REGION") == F.col("latest.REGION"))
        & (F.col("events.EVENT_DATE") == F.col("latest._latest_date")),
    )
    return joined.groupBy(F.col("events.REGION").alias("REGION")).agg(
        F.max(F.col("events.rolling_30d_damage_usd")).alias("rolling_30d_storm_intensity")
    )


def run_process_stage(
    noaa_silver_df: DataFrame, fema_silver_df: DataFrame, nws_bronze_df: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """Runs the full Phase 4 processing stage: join, enrich, and score.

    Args:
        noaa_silver_df (DataFrame): Silver NOAA storm events.
        fema_silver_df (DataFrame): Silver FEMA declarations.
        nws_bronze_df (DataFrame): Bronze NWS active-alert snapshot.

    Returns:
        tuple[DataFrame, DataFrame]: `(enriched_events_df, pressure_score_df)`
            -- the event-grain enrichment (feeds `fact_catastrophe_event`) and
            the region-grain `claims_surge_risk` score (feeds
            `fact_regional_alert_activity`).
    """
    with log_stage_run("process.regional_event_join"):
        joined = join_events_to_declarations(noaa_silver_df, fema_silver_df)
        enriched_events_df = add_rolling_event_intensity(joined)

    with log_stage_run("process.catastrophe_pressure_score"):
        alert_severity_df = aggregate_alert_severity(nws_bronze_df)
        storm_intensity_df = aggregate_latest_storm_intensity(enriched_events_df)
        declaration_frequency_df = aggregate_declaration_frequency(fema_silver_df)
        pressure_score_df = calculate_catastrophe_pressure_score(
            alert_severity_df, storm_intensity_df, declaration_frequency_df
        )

    return enriched_events_df, pressure_score_df
