"""Orchestrates the Bronze ingestion stage: runs every configured ingestor."""

from pyspark.sql import SparkSession

from lakehouse.config.config import LakehouseSettings
from lakehouse.ingestion.base_ingestor import BaseIngestor
from lakehouse.ingestion.features.fema_declarations.feature import FemaDeclarationsIngestor
from lakehouse.ingestion.features.geography_reference.feature import GeographyReferenceIngestor
from lakehouse.ingestion.features.noaa_storm_events.feature import NoaaStormEventsIngestor
from lakehouse.ingestion.features.nws_alerts.feature import NwsAlertsIngestor
from lakehouse.orchestration.run_logger import log_stage_run


def build_ingestors(settings: LakehouseSettings, spark: SparkSession) -> list[BaseIngestor]:
    """Constructs every Bronze ingestor this pipeline runs.

    Args:
        settings (LakehouseSettings): Catalog/API configuration.
        spark (SparkSession): Active Spark session.

    Returns:
        list[BaseIngestor]: One instance per configured source.
    """
    return [
        FemaDeclarationsIngestor(settings, spark),
        NoaaStormEventsIngestor(settings, spark),
        NwsAlertsIngestor(settings, spark),
        GeographyReferenceIngestor(settings, spark),
    ]


def run_ingest_stage(ingestors: list[BaseIngestor]) -> None:
    """Runs and writes every Bronze ingestor, logging each as a separate stage run.

    Args:
        ingestors (list[BaseIngestor]): Ingestors to run, e.g. from `build_ingestors`.
    """
    for ingestor in ingestors:
        with log_stage_run(f"ingest.{ingestor.bronze_table_name}"):
            ingestor.write_bronze(ingestor.run())
