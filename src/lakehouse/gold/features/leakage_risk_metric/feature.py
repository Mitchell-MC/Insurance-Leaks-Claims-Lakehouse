"""Gold leakage_exposure_proxy (KPI 4): combined regional leakage-risk z-score.

    leakage_exposure_proxy(region, as_of_date) =
        z(claims_surge_risk)
      + z(complaint_rate_trend)
      + z(avg_days_event_to_declaration - regional_baseline_days)

Per docs/data_limitations.md, `complaint_rate_trend` resolves to the
documented catastrophe-pressure-acceleration proxy (no structured complaint
source is available), not real complaint data.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _z_score(df: DataFrame, column: str, output_column: str) -> DataFrame:
    """Z-score normalizes a column across the regions present in `df`.

    Args:
        df (DataFrame): DataFrame containing `column`.
        column (str): Name of the numeric column to normalize.
        output_column (str): Name for the resulting z-score column.

    Returns:
        DataFrame: Input with `output_column` added.
    """
    stats = df.agg(F.mean(column).alias("_mean"), F.stddev(column).alias("_std")).first()
    assert stats is not None
    mean_value = stats["_mean"] or 0.0
    std_value = stats["_std"] or 0.0
    std_value = std_value or 1.0  # Reason: avoid divide-by-zero when every region ties.
    return df.withColumn(output_column, (F.col(column) - F.lit(mean_value)) / F.lit(std_value))


def calculate_leakage_exposure_proxy(
    surge_risk_df: DataFrame,
    complaint_trend_proxy_df: DataFrame,
    declaration_lag_df: DataFrame,
) -> DataFrame:
    """Combines surge risk, complaint-trend proxy, and declaration lag into one z-sum.

    Args:
        surge_risk_df (DataFrame): `REGION`, `claims_surge_risk` (KPI 1, from
            `processing.features.catastrophe_pressure_score`).
        complaint_trend_proxy_df (DataFrame): `REGION`, `complaint_rate_trend_proxy`
            (see docs/data_limitations.md for the proxy definition).
        declaration_lag_df (DataFrame): `REGION`, `avg_days_event_to_declaration` (KPI 3).

    Returns:
        DataFrame: One row per region with `REGION` and `leakage_exposure_proxy`.
    """
    combined = (
        surge_risk_df.join(complaint_trend_proxy_df, "REGION", "outer")
        .join(declaration_lag_df, "REGION", "outer")
        .fillna(0.0, subset=["claims_surge_risk", "complaint_rate_trend_proxy"])
    )
    baseline_row = combined.agg(F.avg("avg_days_event_to_declaration").alias("_baseline")).first()
    assert baseline_row is not None
    baseline_days = baseline_row["_baseline"] or 0.0
    combined = combined.withColumn(
        "declaration_lag_delta", F.col("avg_days_event_to_declaration") - F.lit(baseline_days)
    ).fillna(0.0, subset=["declaration_lag_delta"])

    combined = _z_score(combined, "claims_surge_risk", "_z_surge")
    combined = _z_score(combined, "complaint_rate_trend_proxy", "_z_complaint")
    combined = _z_score(combined, "declaration_lag_delta", "_z_lag")
    return combined.withColumn(
        "leakage_exposure_proxy", F.col("_z_surge") + F.col("_z_complaint") + F.col("_z_lag")
    ).select("REGION", "leakage_exposure_proxy")
