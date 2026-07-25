"""Rolling 7-day and 30-day regional storm-event intensity metrics."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

_SECONDS_PER_DAY = 86400


def add_rolling_event_intensity(df: DataFrame) -> DataFrame:
    """Adds trailing 7-day and 30-day event-count and damage-sum columns per region.

    Uses a range-based window (partitioned by region, ordered by event date)
    rather than a row-based one, so the trailing window reflects actual
    elapsed days even when events aren't evenly spaced.

    Args:
        df (DataFrame): One row per storm event, with `REGION`, `EVENT_DATE`,
            and `DAMAGE_PROPERTY_USD` columns (e.g. from
            `regional_event_join.join_events_to_declarations`).

    Returns:
        DataFrame: Input with `rolling_7d_event_count`, `rolling_7d_damage_usd`,
            `rolling_30d_event_count`, and `rolling_30d_damage_usd` added.
    """
    event_epoch_seconds = F.col("EVENT_DATE").cast("timestamp").cast("long")
    window_7d = (
        Window.partitionBy("REGION")
        .orderBy(event_epoch_seconds)
        .rangeBetween(-7 * _SECONDS_PER_DAY, 0)
    )
    window_30d = (
        Window.partitionBy("REGION")
        .orderBy(event_epoch_seconds)
        .rangeBetween(-30 * _SECONDS_PER_DAY, 0)
    )
    return (
        df.withColumn("rolling_7d_event_count", F.count(F.lit(1)).over(window_7d))
        .withColumn("rolling_7d_damage_usd", F.sum("DAMAGE_PROPERTY_USD").over(window_7d))
        .withColumn("rolling_30d_event_count", F.count(F.lit(1)).over(window_30d))
        .withColumn("rolling_30d_damage_usd", F.sum("DAMAGE_PROPERTY_USD").over(window_30d))
    )
