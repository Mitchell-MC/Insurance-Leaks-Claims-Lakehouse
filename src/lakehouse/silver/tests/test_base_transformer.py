"""Tests for BaseSilverTransformer's DQ-metadata attachment and enforcement."""

import pytest
from pyspark.sql import DataFrame, Row, SparkSession

from lakehouse.config.config import LakehouseSettings
from lakehouse.silver.base_transformer import BaseSilverTransformer
from lakehouse.silver.data_quality.checks import (
    DQCheckFailure,
    DQCheckResult,
    check_no_null_geography,
)


class _DummyTransformer(BaseSilverTransformer):
    """Minimal concrete transformer for exercising BaseSilverTransformer behavior."""

    silver_table_name = "dummy_table"

    def transform(self, bronze_df: DataFrame) -> DataFrame:
        return bronze_df

    def dq_checks(self, df: DataFrame) -> list[DQCheckResult]:
        return [check_no_null_geography(df, ["state"])]


class _WarnOnlyTransformer(BaseSilverTransformer):
    """Transformer whose only check is "warn"-severity, to exercise non-blocking failures."""

    silver_table_name = "dummy_table"

    def transform(self, bronze_df: DataFrame) -> DataFrame:
        return bronze_df

    def dq_checks(self, df: DataFrame) -> list[DQCheckResult]:
        return [check_no_null_geography(df, ["state"], severity="warn")]


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


def test_run_raises_on_error_severity_failure(spark: SparkSession) -> None:
    """run() raises DQCheckFailure instead of returning data that fails an error-severity check.

    Reason: promoting Silver data whose only trace of a failed check is a
    _dq_metadata count column (with no downstream consumer of it) is the
    "pipeline ran green anyway" failure mode this enforcement closes.
    """
    settings = LakehouseSettings()
    transformer = _DummyTransformer(settings, spark)
    bronze_df = spark.createDataFrame([Row(state="TX"), Row(state=None)])

    with pytest.raises(DQCheckFailure, match="no_null_geography"):
        transformer.run(bronze_df)


def test_run_does_not_raise_on_warn_severity_failure(spark: SparkSession) -> None:
    """run() still returns data (with failing metadata) when only warn-severity checks fail."""
    settings = LakehouseSettings()
    transformer = _WarnOnlyTransformer(settings, spark)
    bronze_df = spark.createDataFrame([Row(state="TX"), Row(state=None)])

    result = transformer.run(bronze_df).collect()

    metadata = result[0]["_dq_metadata"]
    assert metadata["checks_passed"] == 0
    assert metadata["checks_failed"] == 1
