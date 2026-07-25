"""Tests for calculate_catastrophe_pressure_score's normalization and weighting."""

from pyspark.sql import Row, SparkSession

from lakehouse.processing.features.catastrophe_pressure_score.feature import (
    W1_ACTIVE_ALERT_SEVERITY,
    W2_ROLLING_STORM_INTENSITY,
    W3_HISTORICAL_DECLARATION_FREQUENCY,
    calculate_catastrophe_pressure_score,
)


def test_weights_sum_to_one() -> None:
    """The three named weights sum to 1.0, so a max-everywhere region scores 1.0."""
    assert (
        W1_ACTIVE_ALERT_SEVERITY + W2_ROLLING_STORM_INTENSITY + W3_HISTORICAL_DECLARATION_FREQUENCY
        == 1.0
    )


def test_max_region_scores_one_and_min_region_scores_zero(spark: SparkSession) -> None:
    """A region maxing out every signal scores 1.0; a region at zero scores 0.0."""
    alert_df = spark.createDataFrame(
        [
            Row(REGION="A", active_alert_severity_score=10.0),
            Row(REGION="B", active_alert_severity_score=0.0),
        ]
    )
    storm_df = spark.createDataFrame(
        [
            Row(REGION="A", rolling_30d_storm_intensity=100.0),
            Row(REGION="B", rolling_30d_storm_intensity=0.0),
        ]
    )
    frequency_df = spark.createDataFrame(
        [
            Row(REGION="A", historical_declaration_frequency=5.0),
            Row(REGION="B", historical_declaration_frequency=0.0),
        ]
    )

    result = {
        row["REGION"]: row["claims_surge_risk"]
        for row in calculate_catastrophe_pressure_score(alert_df, storm_df, frequency_df).collect()
    }

    assert result["A"] == 1.0
    assert result["B"] == 0.0
