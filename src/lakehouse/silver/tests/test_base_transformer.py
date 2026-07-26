"""Tests for BaseSilverTransformer's DQ-metadata attachment and enforcement."""

from pathlib import Path

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


def test_run_attaches_passing_dq_metadata(spark: SparkSession, tmp_path: Path) -> None:
    """run() adds a _dq_metadata struct reflecting a fully-passing check."""
    settings = LakehouseSettings(storage_root=str(tmp_path))
    transformer = _DummyTransformer(settings, spark)
    bronze_df = spark.createDataFrame([Row(state="TX"), Row(state="FL")])

    result = transformer.run(bronze_df).collect()

    metadata = result[0]["_dq_metadata"]
    assert metadata["checks_passed"] == 1
    assert metadata["checks_failed"] == 0
    assert metadata["run_id"]


def test_run_raises_on_error_severity_failure(spark: SparkSession, tmp_path: Path) -> None:
    """run() raises DQCheckFailure instead of returning data that fails an error-severity check.

    Reason: promoting Silver data whose only trace of a failed check is a
    _dq_metadata count column (with no downstream consumer of it) is the
    "pipeline ran green anyway" failure mode this enforcement closes.
    """
    settings = LakehouseSettings(storage_root=str(tmp_path))
    transformer = _DummyTransformer(settings, spark)
    bronze_df = spark.createDataFrame([Row(state="TX"), Row(state=None)])

    with pytest.raises(DQCheckFailure, match="no_null_geography"):
        transformer.run(bronze_df)


def test_run_does_not_raise_on_warn_severity_failure(spark: SparkSession, tmp_path: Path) -> None:
    """run() still returns data (with failing metadata) when only warn-severity checks fail."""
    settings = LakehouseSettings(storage_root=str(tmp_path))
    transformer = _WarnOnlyTransformer(settings, spark)
    bronze_df = spark.createDataFrame([Row(state="TX"), Row(state=None)])

    result = transformer.run(bronze_df).collect()

    metadata = result[0]["_dq_metadata"]
    assert metadata["checks_passed"] == 0
    assert metadata["checks_failed"] == 1


def test_run_skips_drift_check_on_first_run(spark: SparkSession, tmp_path: Path) -> None:
    """run() doesn't attempt a drift comparison when the Silver table doesn't exist yet."""
    settings = LakehouseSettings(storage_root=str(tmp_path))
    transformer = _DummyTransformer(settings, spark)
    bronze_df = spark.createDataFrame([Row(state="TX")])

    result = transformer.run(bronze_df).collect()

    assert result[0]["_dq_metadata"]["checks_passed"] == 1


def test_run_flags_drift_against_current_table_contents(
    spark: SparkSession, tmp_path: Path
) -> None:
    """run() compares the new output against the table's existing contents and flags a big swing.

    Reason: this is pillar 2 from Stint's pipeline-testing write-up -- running
    the new logic and comparing it against the last known-good production
    data -- adapted to use the table's own pre-overwrite contents as that
    baseline instead of a separate pre-prod environment. The drift check is
    "warn"-severity by default, so it surfaces in metadata without blocking.
    """
    settings = LakehouseSettings(storage_root=str(tmp_path))
    transformer = _DummyTransformer(settings, spark)
    existing_df = spark.createDataFrame([Row(state="TX")] * 100)
    existing_df.write.format("delta").save(settings.silver_table_path("dummy_table"))

    new_bronze_df = spark.createDataFrame([Row(state="TX")] * 5)
    result = transformer.run(new_bronze_df).collect()

    metadata = result[0]["_dq_metadata"]
    assert metadata["checks_failed"] == 1
    assert metadata["checks_passed"] == 1
