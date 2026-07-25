"""Gold fact_regional_alert_activity: one row per region per as-of date."""

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_fact_regional_alert_activity(
    pressure_score_df: DataFrame,
    as_of_date: date,
    dim_date: DataFrame,
    dim_geography_state: DataFrame,
) -> DataFrame:
    """Builds fact_regional_alert_activity for a single as-of-date snapshot.

    Args:
        pressure_score_df (DataFrame): `REGION`, `claims_surge_risk` (from
            `processing.features.catastrophe_pressure_score`).
        as_of_date (date): Snapshot date this run represents.
        dim_date (DataFrame): Gold `dim_date`.
        dim_geography_state (DataFrame): Gold `dim_geography_state`. Reason:
            this fact is region (state) grain; joining the county-grain
            `dim_geography` on `state` would fan out one region into one row
            per county in that state.

    Returns:
        DataFrame: One row per region with `date_key`, `state_geography_key`,
            and `claims_surge_risk` (KPI 1).
    """
    dated = pressure_score_df.withColumn("as_of_date", F.lit(as_of_date))
    joined = dated.join(dim_date, dated["as_of_date"] == dim_date["date"], "left").join(
        dim_geography_state, dated["REGION"] == dim_geography_state["state"], "left"
    )
    return joined.select("date_key", "state_geography_key", "claims_surge_risk")
