"""Tests for BaseSilverTransformer's DQ-metadata attachment."""

from pyspark.sql import DataFrame, Row, SparkSession

from lakehouse.config.config import LakehouseSettings
from lakehouse.silver.base_transformer import BaseSilverTransformer
from lakehouse.silver.data_quality.checks import DQCheckResult, check_no_null_geography


class _DummyTransformer(BaseSilverTransformer):
    """Minimal concrete transformer for exercising BaseSilverTransformer behavior."""

    silver_table_name = "dummy_table"

    def transform(self, bronze_df: DataFrame) -> DataFrame:
        return bronze_df

    def dq_checks(self, df: DataFrame) -> list[DQCheckResult]:
        return [check_no_null_geography(df, ["state"])]


def test_run_attaches_passing_dq_metadata(spark: SparkSession) -> None:
    """run() adds a _dq_metadata struct reflecting a fully-passing check."""
    settings = LakehouseSettings()
    transformer = _DummyTransformer(settings, spark)
    bronze_df = spark.createDataFrame([Row(state="TX"), Row(state="FL")])

    result = transformer.run(bronze_df).collect()

    metadata = result[0]["_dq_metadata"]
    assert metadata["checks_passed"] == 1
    assert metadata["checks_failed"] == 0
    assert metadata["run_id"]


def test_run_attaches_failing_dq_metadata(spark: SparkSession) -> None:
    """run() reflects a failing check's count in the attached metadata."""
    settings = LakehouseSettings()
    transformer = _DummyTransformer(settings, spark)
    bronze_df = spark.createDataFrame([Row(state="TX"), Row(state=None)])

    result = transformer.run(bronze_df).collect()

    metadata = result[0]["_dq_metadata"]
    assert metadata["checks_passed"] == 0
    assert metadata["checks_failed"] == 1
