"""Tests for the Gold-to-Parquet export stage orchestrator."""

from unittest.mock import Mock, call

from pyspark.sql import SparkSession

from lakehouse.config.config import LakehouseSettings
from lakehouse.orchestration.export_stage import GOLD_TABLE_NAMES, run_export_stage


def test_run_export_stage_reads_and_writes_every_gold_table() -> None:
    """run_export_stage reads each Gold Delta table and writes it as Parquet."""
    settings = LakehouseSettings()
    spark = Mock(spec=SparkSession)
    df = Mock()
    spark.read.format.return_value.load.return_value = df

    run_export_stage(spark, settings)

    assert spark.read.format.return_value.load.call_args_list == [
        call(settings.gold_table_path(name)) for name in GOLD_TABLE_NAMES
    ]
    assert df.write.format.return_value.mode.return_value.save.call_args_list == [
        call(settings.powerbi_export_path(name)) for name in GOLD_TABLE_NAMES
    ]


def test_run_export_stage_writes_parquet_in_overwrite_mode() -> None:
    """Each export write uses format('parquet') and mode('overwrite')."""
    settings = LakehouseSettings()
    spark = Mock(spec=SparkSession)
    df = Mock()
    spark.read.format.return_value.load.return_value = df

    run_export_stage(spark, settings)

    df.write.format.assert_called_with("parquet")
    df.write.format.return_value.mode.assert_called_with("overwrite")
