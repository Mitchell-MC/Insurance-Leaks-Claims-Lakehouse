"""Tests for the Silver standardization stage orchestrator."""

from unittest.mock import Mock

from pyspark.sql import SparkSession

from lakehouse.config.config import LakehouseSettings
from lakehouse.orchestration.silver_stage import build_transformers, run_silver_stage


def test_build_transformers_returns_one_per_bronze_table(spark: SparkSession) -> None:
    """build_transformers constructs a transformer for each configured Bronze table."""
    transformers = build_transformers(LakehouseSettings(), spark)

    assert set(transformers) == {
        "fema_declarations",
        "noaa_storm_events",
        "geography_reference_counties",
    }


def test_run_silver_stage_reads_bronze_and_writes_silver() -> None:
    """run_silver_stage reads the right Bronze path and writes the transform's result."""
    settings = LakehouseSettings(storage_root="dbfs:/lakehouse")
    bronze_df = Mock(name="bronze_df")
    silver_df = Mock(name="silver_df")
    spark = Mock()
    spark.read.format.return_value.load.return_value = bronze_df

    transformer = Mock(silver_table_name="fema_declarations")
    transformer.run.return_value = silver_df

    run_silver_stage(spark, settings, {"fema_declarations": transformer})

    spark.read.format.return_value.load.assert_called_once_with(
        "dbfs:/lakehouse/bronze/fema_declarations"
    )
    transformer.run.assert_called_once_with(bronze_df)
    transformer.write_silver.assert_called_once_with(silver_df)
