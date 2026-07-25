"""Tests for build_dim_alert_status's distinct-triple extraction."""

from pyspark.sql import Row, SparkSession

from lakehouse.gold.dimensions.features.dim_alert_status.feature import build_dim_alert_status


def test_build_dim_alert_status_deduplicates_triples(spark: SparkSession) -> None:
    """build_dim_alert_status collapses repeated severity/urgency/certainty triples."""
    df = spark.createDataFrame(
        [
            Row(severity="Severe", urgency="Immediate", certainty="Observed"),
            Row(severity="Severe", urgency="Immediate", certainty="Observed"),
            Row(severity="Moderate", urgency="Expected", certainty="Likely"),
        ]
    )

    result = build_dim_alert_status(df)

    assert result.count() == 2
