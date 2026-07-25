"""Silver transformer for the Census Gazetteer county reference data."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse.silver.base_transformer import BaseSilverTransformer
from lakehouse.silver.data_quality.checks import (
    DQCheckResult,
    check_no_duplicate_keys,
    check_no_null_geography,
    check_schema_drift,
)

_EXPECTED_COLUMNS = {"USPS", "GEOID", "NAME", "ALAND", "AWATER"}


class GeographyReferenceTransformer(BaseSilverTransformer):
    """Standardizes Bronze Census Gazetteer county data for join compatibility.

    Types the numeric land/water area columns and normalizes the USPS state
    code so it joins cleanly against FEMA and NOAA's `state`/`STATE` columns.
    """

    silver_table_name = "geography_reference_counties"

    def transform(self, bronze_df: DataFrame) -> DataFrame:
        """Types numeric columns and normalizes the USPS state code.

        Args:
            bronze_df (DataFrame): Raw Gazetteer county rows from Bronze.

        Returns:
            DataFrame: One row per county with typed area columns.
        """
        return bronze_df.select(
            F.upper(F.trim(F.col("USPS"))).alias("USPS"),
            F.trim(F.col("GEOID")).alias("GEOID"),
            F.trim(F.col("NAME")).alias("NAME"),
            F.col("ALAND").cast("double").alias("ALAND"),
            F.col("AWATER").cast("double").alias("AWATER"),
        )

    def dq_checks(self, df: DataFrame) -> list[DQCheckResult]:
        """Validates geography completeness, county-key uniqueness, and schema.

        Args:
            df (DataFrame): Transformed Gazetteer county rows.

        Returns:
            list[DQCheckResult]: Results for all three checks.
        """
        return [
            check_no_null_geography(df, ["USPS", "GEOID"]),
            check_no_duplicate_keys(df, ["GEOID"]),
            check_schema_drift(df, _EXPECTED_COLUMNS),
        ]
