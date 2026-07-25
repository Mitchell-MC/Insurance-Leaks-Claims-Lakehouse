"""Tests for build_dim_event_type's distinct-pair extraction."""

from pyspark.sql import Row, SparkSession

from lakehouse.gold.dimensions.features.dim_event_type.feature import build_dim_event_type


def test_build_dim_event_type_deduplicates_pairs(spark: SparkSession) -> None:
    """build_dim_event_type collapses repeated (event_type, severity_band) pairs to one row."""
    df = spark.createDataFrame(
        [
            Row(EVENT_TYPE="Tornado", SEVERITY_BAND="severe"),
            Row(EVENT_TYPE="Tornado", SEVERITY_BAND="severe"),
            Row(EVENT_TYPE="Thunderstorm Wind", SEVERITY_BAND="minor"),
        ]
    )

    result = build_dim_event_type(df)

    assert result.count() == 2
