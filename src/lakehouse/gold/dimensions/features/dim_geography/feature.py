"""Gold dim_geography: one row per U.S. county, keyed for fact-table joins."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_dim_geography(geography_df: DataFrame) -> DataFrame:
    """Builds the county geography dimension from Silver Census Gazetteer data.

    Args:
        geography_df (DataFrame): Silver geography reference (`USPS`, `GEOID`, `NAME`).

    Returns:
        DataFrame: One row per county with a surrogate `geography_key`, the
            `state` (USPS code fact tables join on), `county_geoid`, and
            `county_name`.
    """
    return geography_df.select(
        F.monotonically_increasing_id().alias("geography_key"),
        F.col("USPS").alias("state"),
        F.col("GEOID").alias("county_geoid"),
        F.col("NAME").alias("county_name"),
    )
