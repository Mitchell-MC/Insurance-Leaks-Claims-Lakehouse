"""Gold fact_complaint_trend: correctly-shaped placeholder, populated with zero rows.

See docs/data_limitations.md: no structured complaint-data source is
available (Texas DOI/Florida OIR only expose a consumer-facing lookup form,
not an API or bulk extract). Rather than fabricate rows, this table exists
with the correct grain/schema so downstream joins and Power BI queries
resolve to "no data yet" instead of erroring on a missing table. KPI 2's
`complaint_rate_trend` is served by a documented proxy elsewhere
(`processing.features.catastrophe_pressure_score`-driven trend) until a real
source is connected.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, IntegerType, LongType, StructField, StructType

SCHEMA = StructType(
    [
        StructField("date_key", IntegerType(), nullable=False),
        # Reason: state grain, matching the other region-grain facts -- see
        # dim_geography_state's module docstring for why these don't key off
        # the county-grain dim_geography.
        StructField("state_geography_key", LongType(), nullable=False),
        StructField("complaint_count", IntegerType(), nullable=False),
        StructField("complaint_rate_trend", DoubleType(), nullable=False),
    ]
)


def build_fact_complaint_trend(spark: SparkSession) -> DataFrame:
    """Builds the (currently empty) fact_complaint_trend table.

    Grain: one row per region (`state_geography_key`) per date (`date_key`).

    Args:
        spark (SparkSession): Active Spark session.

    Returns:
        DataFrame: Zero rows, with the schema real complaint data will use
            once a structured source is connected.
    """
    return spark.createDataFrame([], schema=SCHEMA)
