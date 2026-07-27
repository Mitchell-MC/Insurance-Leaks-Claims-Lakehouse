"""Tests for the Bronze ingestion stage orchestrator."""

from unittest.mock import Mock

from pyspark.sql import SparkSession

from lakehouse.config.config import LakehouseSettings
from lakehouse.orchestration.ingest_stage import build_ingestors, run_ingest_stage


def test_build_ingestors_returns_one_per_source(spark: SparkSession) -> None:
    """build_ingestors constructs all four configured Bronze ingestors."""
    ingestors = build_ingestors(LakehouseSettings(), spark)

    table_names = {ingestor.bronze_table_name for ingestor in ingestors}
    assert table_names == {
        "fema_declarations",
        "noaa_storm_events",
        "nws_alerts_snapshots",
        "geography_reference_counties",
    }


def test_build_ingestors_share_one_watermark_store(spark: SparkSession) -> None:
    """All four ingestors read/write the same WatermarkStore instance."""
    ingestors = build_ingestors(LakehouseSettings(), spark)

    stores = {id(ingestor._watermark_store) for ingestor in ingestors}
    assert len(stores) == 1


def test_run_ingest_stage_runs_and_writes_each_ingestor() -> None:
    """run_ingest_stage calls run() then write_bronze(result) for every ingestor."""
    raw_df = Mock(name="raw_df")
    ingestor = Mock(bronze_table_name="dummy_table")
    ingestor.run.return_value = raw_df

    run_ingest_stage([ingestor])

    ingestor.run.assert_called_once()
    ingestor.write_bronze.assert_called_once_with(raw_df)
