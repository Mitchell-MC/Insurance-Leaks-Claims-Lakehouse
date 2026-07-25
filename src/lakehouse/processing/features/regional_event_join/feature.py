"""Joins Silver NOAA storm events to Silver FEMA declarations by region and date window."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Reason: a storm event is considered "associated with" a FEMA declaration if
# it falls within this many days of the declaration's incident start -- wide
# enough to capture multi-day storm systems, narrow enough not to conflate
# unrelated events in the same state.
DEFAULT_WINDOW_DAYS = 7


def join_events_to_declarations(
    noaa_df: DataFrame, fema_df: DataFrame, window_days: int = DEFAULT_WINDOW_DAYS
) -> DataFrame:
    """Left-joins storm events to the FEMA declaration they likely drove.

    Args:
        noaa_df (DataFrame): Silver NOAA storm events (STATE, EVENT_DATE,
            EVENT_TYPE, SEVERITY_BAND, DAMAGE_PROPERTY_USD).
        fema_df (DataFrame): Silver FEMA declarations (state, disasterNumber,
            declarationDate, incidentBeginDate, incidentType).
        window_days (int): Days on either side of `incidentBeginDate` a storm
            event's `EVENT_DATE` must fall within to be joined.

    Returns:
        DataFrame: One row per storm event, with FEMA declaration columns
            attached where a matching declaration exists (else null).
    """
    noaa = noaa_df.alias("noaa")
    # Reason: FEMA declarations are dimension-like (tens of thousands of rows
    # even across the full historical range) against NOAA's millions of storm
    # events, so broadcasting the FEMA side avoids shuffling the large NOAA
    # side entirely -- see docs/partitioning_benchmark_memo.md for the
    # measured plan-shape difference this makes.
    fema = F.broadcast(fema_df).alias("fema")
    join_condition = (
        (F.col("noaa.STATE") == F.col("fema.state"))
        & (F.col("noaa.EVENT_DATE") >= F.date_sub(F.col("fema.incidentBeginDate"), window_days))
        & (F.col("noaa.EVENT_DATE") <= F.date_add(F.col("fema.incidentBeginDate"), window_days))
    )
    return noaa.join(fema, on=join_condition, how="left").select(
        F.col("noaa.STATE").alias("REGION"),
        F.col("noaa.EVENT_DATE"),
        F.col("noaa.EVENT_TYPE"),
        F.col("noaa.SEVERITY_BAND"),
        F.col("noaa.DAMAGE_PROPERTY_USD"),
        F.col("fema.disasterNumber"),
        F.col("fema.incidentType").alias("fema_incidentType"),
        F.col("fema.declarationDate"),
        F.col("fema.incidentBeginDate"),
    )
