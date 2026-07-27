"""Abstract base class for Silver-layer data transformers."""

import uuid
from abc import ABC, abstractmethod
from datetime import UTC, datetime

from delta.tables import DeltaTable
from pydantic import BaseModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.config.config import LakehouseSettings
from lakehouse.silver.data_quality.checks import (
    DQCheckResult,
    check_no_silent_drift,
    raise_on_failures,
)


class DQMetadata(BaseModel):
    """Metadata summarizing the data-quality checks run for one Silver transform.

    Attributes:
        checked_at (datetime): UTC timestamp when checks ran.
        run_id (str): Unique identifier for this transform run.
        checks_passed (int): Number of DQ checks that passed.
        checks_failed (int): Number of DQ checks that failed.
    """

    checked_at: datetime
    run_id: str
    checks_passed: int
    checks_failed: int


class BaseSilverTransformer(ABC):
    """Standardizes a Bronze DataFrame into a quality-checked Silver DataFrame.

    Concrete subclasses implement `transform()` to normalize/dedupe/enrich and
    `dq_checks()` to declare the checks that must run before promotion. `run()`
    applies both and attaches a `_dq_metadata` struct; `write_silver()`
    persists the result as Delta.
    """

    def __init__(self, settings: LakehouseSettings, spark: SparkSession) -> None:
        """Initializes the transformer with typed settings and a Spark session.

        Args:
            settings (LakehouseSettings): Catalog/storage configuration.
            spark (SparkSession): Active Spark session used to build DataFrames.
        """
        self._settings = settings
        self._spark = spark

    @property
    @abstractmethod
    def silver_table_name(self) -> str:
        """str: Unqualified Silver table name this transformer writes to."""

    @abstractmethod
    def transform(self, bronze_df: DataFrame) -> DataFrame:
        """Normalizes, dedupes, and enriches a raw Bronze DataFrame.

        Args:
            bronze_df (DataFrame): Raw records from the Bronze layer.

        Returns:
            DataFrame: Standardized records ready for Silver.
        """

    @abstractmethod
    def dq_checks(self, df: DataFrame) -> list[DQCheckResult]:
        """Runs the data-quality checks required before promoting to Silver.

        Args:
            df (DataFrame): Transformed DataFrame to validate.

        Returns:
            list[DQCheckResult]: One result per check run.
        """

    @property
    def drift_numeric_columns(self) -> list[str]:
        """list[str]: Numeric columns to include in the silent-drift check.

        Empty by default (row-count drift is still checked for every
        transformer). Override to add columns whose aggregate total should
        also stay stable run-over-run (e.g. a damage-total column), so a bug
        that silently shifts a sum (unit conversion, a join fan-out) gets
        caught even though every per-row check still passes.
        """
        return []

    def run(self, bronze_df: DataFrame) -> DataFrame:
        """Transforms, runs DQ checks (incl. drift vs. the current table), and attaches metadata.

        Args:
            bronze_df (DataFrame): Raw records from the Bronze layer.

        Returns:
            DataFrame: Standardized records with an added `_dq_metadata` column.

        Raises:
            DQCheckFailure: If any "error"-severity check in `dq_checks()`
                or the silent-drift check failed. Reason: promoting Silver
                data that fails an error-severity check (e.g. duplicate
                keys) would let bad rows flow into Gold with only a metadata
                column noting it -- see checks.raise_on_failures.
        """
        silver_df = self.transform(bronze_df)
        results = self.dq_checks(silver_df)
        results.extend(self._drift_checks(silver_df))
        raise_on_failures(results)
        metadata = DQMetadata(
            checked_at=datetime.now(UTC),
            run_id=str(uuid.uuid4()),
            checks_passed=sum(1 for result in results if result.passed),
            checks_failed=sum(1 for result in results if not result.passed),
        )
        return silver_df.withColumn(
            "_dq_metadata",
            F.struct(
                F.lit(metadata.checked_at).alias("checked_at"),
                F.lit(metadata.run_id).alias("run_id"),
                F.lit(metadata.checks_passed).alias("checks_passed"),
                F.lit(metadata.checks_failed).alias("checks_failed"),
            ),
        )

    def _drift_checks(self, silver_df: DataFrame) -> list[DQCheckResult]:
        """Compares `silver_df` against the table's current (pre-overwrite) contents.

        This is the "run against real data and compare to the last known-good
        version" pillar from Stint's pipeline-testing write-up, adapted to
        this repo's overwrite-per-run Silver tables: the table on disk right
        now *is* the last known-good output, so it doubles as the comparison
        baseline with no separate pre-prod environment required.

        Args:
            silver_df (DataFrame): Freshly transformed DataFrame about to be
                written by `write_silver()`.

        Returns:
            list[DQCheckResult]: Drift-check results, or an empty list on the
                table's first run (nothing to compare against yet).
        """
        path = self._settings.silver_table_path(self.silver_table_name)
        if not DeltaTable.isDeltaTable(self._spark, path):
            return []
        previous_df = self._spark.read.format("delta").load(path)
        return check_no_silent_drift(silver_df, previous_df, self.drift_numeric_columns)

    def filter_current(self, bronze_df: DataFrame) -> DataFrame:
        """Drops Bronze rows the ingestor flagged as tombstoned (superseded upstream).

        Args:
            bronze_df (DataFrame): Raw records from the Bronze layer, which
                may or may not carry an `_is_current` column -- only
                ingestors with tombstone detection (FEMA, NOAA) attach it,
                and rows appended before that existed have no value at all.

        Returns:
            DataFrame: `bronze_df` unchanged if it has no `_is_current`
                column (nothing to filter); otherwise, rows with
                `_is_current = false` excluded. A NULL value (pre-existing
                row, or a source with no tombstone tracking) counts as
                current, so it is never dropped by this filter.
        """
        if "_is_current" not in bronze_df.columns:
            return bronze_df
        return bronze_df.filter(F.col("_is_current") != False)  # noqa: E712

    def write_silver(self, df: DataFrame) -> None:
        """Writes a DataFrame to the Silver Delta table for this transformer.

        Args:
            df (DataFrame): DataFrame produced by `run()`, ready to persist.
        """
        # Reason: Silver re-derives its full state from Bronze each run (unlike
        # Bronze's incremental append), so overwrite keeps it idempotent.
        path = self._settings.silver_table_path(self.silver_table_name)
        df.write.format("delta").mode("overwrite").save(path)
