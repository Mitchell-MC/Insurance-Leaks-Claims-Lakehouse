"""Gold dim_event_type: one row per distinct (event type, severity band)."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_dim_event_type(noaa_df: DataFrame) -> DataFrame:
    """Builds the event-type dimension from Silver NOAA storm events.

    Args:
        noaa_df (DataFrame): Silver NOAA storm events (`EVENT_TYPE`, `SEVERITY_BAND`).

    Returns:
        DataFrame: One row per distinct (event_type, severity_band) pair
            with a surrogate `event_type_key`.
    """
    return (
        noaa_df.select(
            F.col("EVENT_TYPE").alias("event_type"), F.col("SEVERITY_BAND").alias("severity_band")
        )
        .distinct()
        .withColumn("event_type_key", F.monotonically_increasing_id())
        .select("event_type_key", "event_type", "severity_band")
    )
