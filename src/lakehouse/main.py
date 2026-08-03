"""CLI entry point for orchestrating lakehouse pipeline stages."""

import argparse
import sys
from collections.abc import Callable
from datetime import UTC, date, datetime

from pyspark.sql import SparkSession

from lakehouse.config.config import LakehouseSettings
from lakehouse.orchestration.export_stage import run_export_stage
from lakehouse.orchestration.gold_stage import run_gold_stage
from lakehouse.orchestration.ingest_stage import build_ingestors, run_ingest_stage
from lakehouse.orchestration.process_stage import run_process_stage
from lakehouse.orchestration.silver_stage import build_transformers, run_silver_stage


def build_parser() -> argparse.ArgumentParser:
    """Builds the top-level CLI argument parser.

    Returns:
        argparse.ArgumentParser: Parser with a `stage` positional argument
            selecting which pipeline stage to run.
    """
    parser = argparse.ArgumentParser(
        prog="lakehouse",
        description="Insurance Claims Leakage & Catastrophe Response Analytics Lakehouse CLI",
    )
    parser.add_argument(
        "stage",
        choices=["ingest", "silver", "process", "gold", "export"],
        help="Pipeline stage to run.",
    )
    parser.add_argument(
        "--source",
        choices=["all", "historical", "nws_alerts_snapshots"],
        default="all",
        # Reason: the PDF's Phase 6 explicitly asks to schedule historical
        # batch refreshes separately from near-real-time alert refreshes --
        # see infra/jobs.tf's two independently-scheduled databricks_job
        # resources and docs/batch_vs_streaming_memo.md.
        help="For the 'ingest' stage only: which sources to run.",
    )
    return parser


def _run_ingest(settings: LakehouseSettings, spark: SparkSession, source: str = "all") -> None:
    """Runs the Bronze ingestion stage for the requested source group.

    Args:
        settings (LakehouseSettings): Catalog/API configuration.
        spark (SparkSession): Active Spark session.
        source (str): "all", "historical" (everything but NWS alerts), or
            "nws_alerts_snapshots" (alerts only).
    """
    ingestors = build_ingestors(settings, spark)
    if source == "historical":
        ingestors = [i for i in ingestors if i.bronze_table_name != "nws_alerts_snapshots"]
    elif source != "all":
        ingestors = [i for i in ingestors if i.bronze_table_name == source]
    run_ingest_stage(ingestors)


def _run_silver(settings: LakehouseSettings, spark: SparkSession) -> None:
    """Runs the Silver standardization stage.

    Args:
        settings (LakehouseSettings): Catalog/storage configuration.
        spark (SparkSession): Active Spark session.
    """
    run_silver_stage(spark, settings, build_transformers(settings, spark))


def _run_process(settings: LakehouseSettings, spark: SparkSession) -> None:
    """Runs the Phase 4 processing stage and stages its output for the Gold stage.

    Args:
        settings (LakehouseSettings): Catalog/storage configuration.
        spark (SparkSession): Active Spark session.
    """
    noaa_df = spark.read.format("delta").load(settings.silver_table_path("noaa_storm_events"))
    fema_df = spark.read.format("delta").load(settings.silver_table_path("fema_declarations"))
    nws_df = spark.read.format("delta").load(settings.bronze_table_path("nws_alerts_snapshots"))
    enriched_events_df, pressure_score_df = run_process_stage(noaa_df, fema_df, nws_df)
    # Reason: "process" and "gold" are separate CLI/job-task invocations (see
    # infra/jobs.tf), so the process stage's output must be persisted for the
    # gold stage's later, separate process to read back.
    enriched_events_df.write.format("delta").mode("overwrite").save(
        settings.gold_table_path("_staging_enriched_events")
    )
    pressure_score_df.write.format("delta").mode("overwrite").save(
        settings.gold_table_path("_staging_pressure_score")
    )


def _run_gold(settings: LakehouseSettings, spark: SparkSession) -> None:
    """Runs the Gold dimensional-modeling stage and writes every Gold table.

    Args:
        settings (LakehouseSettings): Catalog/storage configuration.
        spark (SparkSession): Active Spark session.
    """
    noaa_df = spark.read.format("delta").load(settings.silver_table_path("noaa_storm_events"))
    fema_df = spark.read.format("delta").load(settings.silver_table_path("fema_declarations"))
    geography_df = spark.read.format("delta").load(
        settings.silver_table_path("geography_reference_counties")
    )
    nws_df = spark.read.format("delta").load(settings.bronze_table_path("nws_alerts_snapshots"))
    enriched_events_df = spark.read.format("delta").load(
        settings.gold_table_path("_staging_enriched_events")
    )
    pressure_score_df = spark.read.format("delta").load(
        settings.gold_table_path("_staging_pressure_score")
    )
    today = datetime.now(UTC).date()
    tables = run_gold_stage(
        spark,
        noaa_df,
        fema_df,
        geography_df,
        nws_df,
        enriched_events_df,
        pressure_score_df,
        as_of_date=today,
        dim_date_start=date(today.year - 5, 1, 1),
        dim_date_end=date(today.year + 1, 12, 31),
    )
    for table_name, df in tables.items():
        df.write.format("delta").mode("overwrite").save(settings.gold_table_path(table_name))


def _run_export(settings: LakehouseSettings, spark: SparkSession) -> None:
    """Exports every Gold Delta table to Parquet for local BI tools (e.g. Power BI Desktop).

    Args:
        settings (LakehouseSettings): Catalog/storage configuration.
        spark (SparkSession): Active Spark session.
    """
    run_export_stage(spark, settings)


_STAGE_RUNNERS: dict[str, Callable[[LakehouseSettings, SparkSession], None]] = {
    "silver": _run_silver,
    "process": _run_process,
    "gold": _run_gold,
    "export": _run_export,
}


def main(argv: list[str] | None = None) -> int:
    """Parses CLI arguments and dispatches to the requested pipeline stage.

    Args:
        argv (list[str] | None): Command-line arguments, defaults to sys.argv.

    Returns:
        int: Process exit code.
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    settings = LakehouseSettings()
    spark = SparkSession.builder.appName("lakehouse").getOrCreate()
    if args.stage == "ingest":
        _run_ingest(settings, spark, args.source)
    else:
        _STAGE_RUNNERS[args.stage](settings, spark)
    return 0


if __name__ == "__main__":
    sys.exit(main())
