"""Gold dim_geography_state: one row per U.S. state, keyed for state-grain facts.

Reason: `dim_geography` is county-grain (one row per Census GEOID), but every
KPI in docs/kpi_definitions.md is defined at *region* (state) level -- KPI 1's
`claims_surge_risk(region, ...)`, KPI 3's "grouped by state", and KPI 4's
per-region z-scores. Joining a state-grain fact to the county-grain dimension
on `state` alone fans each fact row out to one row per county in that state
(254x for Texas), silently inflating every count-based KPI. State-grain facts
join this dimension instead; county-grain facts (none yet) still use
`dim_geography`.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_dim_geography_state(geography_df: DataFrame) -> DataFrame:
    """Builds the state geography dimension from Silver Census Gazetteer data.

    Args:
        geography_df (DataFrame): Silver geography reference (`USPS`, `GEOID`, `NAME`).

    Returns:
        DataFrame: One row per state with a surrogate `state_geography_key`,
            the `state` (USPS code facts join on), and `county_count` (how many
            counties the Gazetteer lists for that state).
    """
    return (
        geography_df.groupBy(F.col("USPS").alias("state"))
        .agg(F.count(F.lit(1)).alias("county_count"))
        .select(
            F.monotonically_increasing_id().alias("state_geography_key"),
            F.col("state"),
            F.col("county_count"),
        )
    )
