"""Gold dim_alert_status: one row per distinct NWS severity/urgency/certainty combo."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_dim_alert_status(alerts_df: DataFrame) -> DataFrame:
    """Builds the alert-status dimension from Bronze NWS alert snapshots.

    Args:
        alerts_df (DataFrame): Bronze NWS alerts (`severity`, `urgency`, `certainty`).

    Returns:
        DataFrame: One row per distinct (severity, urgency, certainty) triple
            with a surrogate `alert_status_key`.
    """
    return (
        alerts_df.select("severity", "urgency", "certainty")
        .distinct()
        .withColumn("alert_status_key", F.monotonically_increasing_id())
        .select("alert_status_key", "severity", "urgency", "certainty")
    )
