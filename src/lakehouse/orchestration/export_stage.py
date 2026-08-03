"""Orchestrates exporting Gold Delta tables to Parquet for local BI tools."""

from pyspark.sql import SparkSession

from lakehouse.config.config import LakehouseSettings
from lakehouse.orchestration.run_logger import log_stage_run

GOLD_TABLE_NAMES = [
    "dim_date",
    "dim_geography",
    "dim_geography_state",
    "dim_event_type",
    "dim_alert_status",
    "fact_catastrophe_event",
    "fact_regional_alert_activity",
    "fact_complaint_trend",
    "leakage_risk_metric",
]


def run_export_stage(spark: SparkSession, settings: LakehouseSettings) -> None:
    """Exports every Gold Delta table to Parquet under `settings.powerbi_export_root`.

    Reason: Power BI Desktop has no Delta Lake connector, so pointing it at
    this pipeline's local Docker Compose output (rather than a real
    Databricks SQL warehouse) needs a format Get Data > Folder/Parquet can
    read directly -- see docs/dashboard_usage_guide.md's "Local Power BI
    Desktop" section. A static Import, not DirectQuery: rerun this stage and
    refresh in Power BI Desktop to pick up new Gold output.

    Args:
        spark (SparkSession): Active Spark session.
        settings (LakehouseSettings): Catalog/storage configuration.
    """
    for table_name in GOLD_TABLE_NAMES:
        with log_stage_run(f"export.{table_name}"):
            df = spark.read.format("delta").load(settings.gold_table_path(table_name))
            df.write.format("parquet").mode("overwrite").save(
                settings.powerbi_export_path(table_name)
            )
