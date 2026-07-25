"""Gold fact_catastrophe_event: one row per FEMA disaster declaration.

Grain matches kpi_definitions.md's KPI-3 (Average Days from Event to
Declaration), which is computed entirely from FEMA's own two date columns.
Matched storm-event measures (from Phase 4's regional join + rolling
intensity) are aggregated up to declaration grain and attached alongside.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_fact_catastrophe_event(
    enriched_events_df: DataFrame,
    fema_df: DataFrame,
    dim_date: DataFrame,
    dim_geography_state: DataFrame,
) -> DataFrame:
    """Builds fact_catastrophe_event at one-row-per-declaration grain.

    Args:
        enriched_events_df (DataFrame): Storm events joined to declarations
            and enriched with rolling metrics (from
            `processing.features.regional_event_join` +
            `processing.features.rolling_event_intensity`).
        fema_df (DataFrame): Silver FEMA declarations.
        dim_date (DataFrame): Gold `dim_date`.
        dim_geography_state (DataFrame): Gold `dim_geography_state`. Reason:
            this fact is declaration-grain (state-level); joining the
            county-grain `dim_geography` on `state` would fan out one
            declaration into one row per county in that state.

    Returns:
        DataFrame: One row per `disasterNumber` with `date_key` (declaration
            date), `state_geography_key`, `incidentType`, `days_to_declaration`
            (KPI 3), and aggregated matched-storm-event measures.
    """
    event_aggregates = (
        enriched_events_df.filter(F.col("disasterNumber").isNotNull())
        .groupBy("disasterNumber")
        .agg(
            F.count(F.lit(1)).alias("matched_storm_event_count"),
            F.sum("DAMAGE_PROPERTY_USD").alias("total_damage_property_usd"),
            F.max("rolling_30d_event_count").alias("peak_rolling_30d_event_count"),
        )
    )
    declarations = fema_df.withColumn(
        "days_to_declaration", F.datediff(F.col("declarationDate"), F.col("incidentBeginDate"))
    )
    joined = (
        declarations.join(event_aggregates, "disasterNumber", "left")
        .join(dim_date, declarations["declarationDate"] == dim_date["date"], "left")
        .join(
            dim_geography_state,
            declarations["state"] == dim_geography_state["state"],
            "left",
        )
    )
    return joined.select(
        F.col("disasterNumber"),
        F.col("date_key"),
        F.col("state_geography_key"),
        F.col("incidentType"),
        F.col("days_to_declaration"),
        F.coalesce(F.col("matched_storm_event_count"), F.lit(0)).alias("matched_storm_event_count"),
        F.coalesce(F.col("total_damage_property_usd"), F.lit(0.0)).alias(
            "total_damage_property_usd"
        ),
        F.coalesce(F.col("peak_rolling_30d_event_count"), F.lit(0)).alias(
            "peak_rolling_30d_event_count"
        ),
    )
