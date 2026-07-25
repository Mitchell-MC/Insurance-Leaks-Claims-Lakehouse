"""Orchestrates the Silver standardization stage: reads Bronze, writes Silver."""

from pyspark.sql import SparkSession

from lakehouse.config.config import LakehouseSettings
from lakehouse.orchestration.run_logger import log_stage_run
from lakehouse.silver.base_transformer import BaseSilverTransformer
from lakehouse.silver.features.fema_declarations.feature import FemaDeclarationsTransformer
from lakehouse.silver.features.geography_reference.feature import GeographyReferenceTransformer
from lakehouse.silver.features.noaa_storm_events.feature import NoaaStormEventsTransformer


def build_transformers(
    settings: LakehouseSettings, spark: SparkSession
) -> dict[str, BaseSilverTransformer]:
    """Constructs every Silver transformer, keyed by the Bronze table it reads.

    Args:
        settings (LakehouseSettings): Catalog/storage configuration.
        spark (SparkSession): Active Spark session.

    Returns:
        dict[str, BaseSilverTransformer]: Bronze table name -> transformer.
    """
    return {
        "fema_declarations": FemaDeclarationsTransformer(settings, spark),
        "noaa_storm_events": NoaaStormEventsTransformer(settings, spark),
        "geography_reference_counties": GeographyReferenceTransformer(settings, spark),
    }


def run_silver_stage(
    spark: SparkSession,
    settings: LakehouseSettings,
    transformers: dict[str, BaseSilverTransformer],
) -> None:
    """Reads each Bronze table and writes its standardized Silver counterpart.

    Args:
        spark (SparkSession): Active Spark session, used to read Bronze Delta tables.
        settings (LakehouseSettings): Catalog/storage configuration.
        transformers (dict[str, BaseSilverTransformer]): From `build_transformers`.
    """
    for bronze_table_name, transformer in transformers.items():
        with log_stage_run(f"silver.{transformer.silver_table_name}"):
            bronze_path = settings.bronze_table_path(bronze_table_name)
            bronze_df = spark.read.format("delta").load(bronze_path)
            transformer.write_silver(transformer.run(bronze_df))
