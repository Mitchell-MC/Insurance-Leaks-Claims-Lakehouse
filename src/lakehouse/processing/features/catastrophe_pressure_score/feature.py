"""Regional catastrophe pressure score (kpi_definitions.md's claims_surge_risk).

claims_surge_risk(region, as_of_date) =
    w1 * normalized(active_alert_severity_score)
  + w2 * normalized(rolling_30d_storm_intensity)
  + w3 * normalized(historical_declaration_frequency)
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Reason: weights sum to 1.0 and are ordered by how quickly each signal
# reflects emerging risk. Active alerts are the fastest-moving, most direct
# leading indicator (hours), recent storm intensity is a strong but slightly
# lagged secondary signal (days), and historical declaration frequency is the
# slowest-moving baseline-risk signal (years) -- present so a single noisy
# alert day doesn't dominate the score for an otherwise low-risk region.
W1_ACTIVE_ALERT_SEVERITY = 0.5
W2_ROLLING_STORM_INTENSITY = 0.3
W3_HISTORICAL_DECLARATION_FREQUENCY = 0.2

_ALERT_COLUMN = "active_alert_severity_score"
_STORM_COLUMN = "rolling_30d_storm_intensity"
_FREQUENCY_COLUMN = "historical_declaration_frequency"


def _min_max_normalize(df: DataFrame, column: str, output_column: str) -> DataFrame:
    """Min-max scales a column to [0, 1] across the regions present in `df`.

    Args:
        df (DataFrame): DataFrame containing `column`.
        column (str): Name of the numeric column to normalize.
        output_column (str): Name for the resulting [0, 1] scaled column.

    Returns:
        DataFrame: Input with `output_column` added.
    """
    bounds = df.agg(F.min(column).alias("_min"), F.max(column).alias("_max")).first()
    assert bounds is not None
    min_value, max_value = bounds["_min"] or 0.0, bounds["_max"] or 0.0
    span = (max_value - min_value) or 1.0
    return df.withColumn(output_column, (F.col(column) - F.lit(min_value)) / F.lit(span))


def calculate_catastrophe_pressure_score(
    alert_severity_df: DataFrame,
    storm_intensity_df: DataFrame,
    declaration_frequency_df: DataFrame,
) -> DataFrame:
    """Combines the three KPI-1 signals into a single 0-1 `claims_surge_risk` per region.

    Args:
        alert_severity_df (DataFrame): One row per region with `REGION` and
            `active_alert_severity_score` (from Bronze NWS alert snapshots).
        storm_intensity_df (DataFrame): One row per region with `REGION` and
            `rolling_30d_storm_intensity` (from `rolling_event_intensity`).
        declaration_frequency_df (DataFrame): One row per region with `REGION`
            and `historical_declaration_frequency` (from Silver FEMA declarations).

    Returns:
        DataFrame: One row per region with `REGION` and `claims_surge_risk`.
    """
    combined = (
        alert_severity_df.join(storm_intensity_df, on="REGION", how="outer")
        .join(declaration_frequency_df, on="REGION", how="outer")
        .fillna(0.0, subset=[_ALERT_COLUMN, _STORM_COLUMN, _FREQUENCY_COLUMN])
    )
    normalized = _min_max_normalize(combined, _ALERT_COLUMN, "_alert_norm")
    normalized = _min_max_normalize(normalized, _STORM_COLUMN, "_storm_norm")
    normalized = _min_max_normalize(normalized, _FREQUENCY_COLUMN, "_freq_norm")
    return normalized.withColumn(
        "claims_surge_risk",
        F.col("_alert_norm") * F.lit(W1_ACTIVE_ALERT_SEVERITY)
        + F.col("_storm_norm") * F.lit(W2_ROLLING_STORM_INTENSITY)
        + F.col("_freq_norm") * F.lit(W3_HISTORICAL_DECLARATION_FREQUENCY),
    ).select("REGION", "claims_surge_risk")
