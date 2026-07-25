"""Tests for calculate_leakage_exposure_proxy's z-score combination."""

from pyspark.sql import Row, SparkSession

from lakehouse.gold.features.leakage_risk_metric.feature import calculate_leakage_exposure_proxy


def test_higher_signals_across_the_board_yield_a_higher_leakage_score(spark: SparkSession) -> None:
    """A region elevated on all three signals scores higher than one at baseline."""
    surge_df = spark.createDataFrame(
        [
            Row(REGION="A", claims_surge_risk=10.0),
            Row(REGION="B", claims_surge_risk=5.0),
            Row(REGION="C", claims_surge_risk=0.0),
        ]
    )
    complaint_df = spark.createDataFrame(
        [
            Row(REGION="A", complaint_rate_trend_proxy=5.0),
            Row(REGION="B", complaint_rate_trend_proxy=2.5),
            Row(REGION="C", complaint_rate_trend_proxy=0.0),
        ]
    )
    lag_df = spark.createDataFrame(
        [
            Row(REGION="A", avg_days_event_to_declaration=30.0),
            Row(REGION="B", avg_days_event_to_declaration=15.0),
            Row(REGION="C", avg_days_event_to_declaration=0.0),
        ]
    )

    result = {
        row["REGION"]: row["leakage_exposure_proxy"]
        for row in calculate_leakage_exposure_proxy(surge_df, complaint_df, lag_df).collect()
    }

    assert result["A"] > result["B"] > result["C"]
