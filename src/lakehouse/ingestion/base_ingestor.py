"""Abstract base class for Bronze-layer data ingestors."""

import json
import time
import uuid
from abc import ABC, abstractmethod
from datetime import UTC, date, datetime
from typing import Any

import requests
from pydantic import BaseModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.config.config import LakehouseSettings
from lakehouse.ingestion.watermark_store import DEFAULT_WATERMARK_KEY, WatermarkStore
from lakehouse.silver.data_quality.checks import DQCheckResult, raise_on_failures


class IngestionMetadata(BaseModel):
    """Metadata captured for every Bronze ingestion run.

    Attributes:
        source_file_date (date | None): Date the source data was produced,
            when the source exposes one (e.g. a NOAA bulk file's year). None
            for sources without a meaningful file date, like NWS snapshots.
        loaded_at (datetime): UTC timestamp when the run started.
        run_id (str): Unique identifier for this ingestion run.
        record_count (int): Number of records fetched.
    """

    source_file_date: date | None = None
    loaded_at: datetime
    run_id: str
    record_count: int


class BaseIngestor(ABC):
    """Retry-aware base class for Bronze-layer ingestors.

    Concrete subclasses implement `fetch()` to return a raw Spark DataFrame
    and `bronze_table_name` to name the destination table. `run()` attaches
    ingestion metadata; `write_bronze()` persists the result as Delta.
    """

    def __init__(
        self,
        settings: LakehouseSettings,
        spark: SparkSession,
        watermark_store: WatermarkStore | None = None,
    ) -> None:
        """Initializes the ingestor with typed settings, Spark, and watermark state.

        Args:
            settings (LakehouseSettings): Catalog/retry/API configuration.
            spark (SparkSession): Active Spark session used to build DataFrames.
            watermark_store (WatermarkStore | None): Incremental-fetch
                checkpoint store. Defaults to a real `WatermarkStore` backed by
                this run's settings/spark -- constructing one performs no I/O,
                so this default is always safe.
        """
        self._settings = settings
        self._spark = spark
        self._watermark_store = watermark_store or WatermarkStore(settings, spark)

    @property
    @abstractmethod
    def bronze_table_name(self) -> str:
        """str: Unqualified Bronze table name this ingestor writes to."""

    @abstractmethod
    def fetch(self) -> DataFrame:
        """Fetches raw source data as a Spark DataFrame (no metadata attached).

        Returns:
            DataFrame: Raw records in their source shape.
        """

    def dq_checks(self, df: DataFrame) -> list[DQCheckResult]:
        """Runs the data-quality checks required after fetching Bronze data.

        Default is no checks; concrete ingestors override this to add
        source-specific checks (e.g. freshness for a near-real-time feed).

        Args:
            df (DataFrame): Fetched DataFrame, with `_ingestion_metadata` attached.

        Returns:
            list[DQCheckResult]: One result per check run. Empty by default.
        """
        return []

    def run(self, source_file_date: date | None = None) -> DataFrame:
        """Fetches source data, attaches `_ingestion_metadata`, and runs DQ checks.

        Args:
            source_file_date (date | None): Date of the underlying source file,
                if the source exposes one. Defaults to None.

        Returns:
            DataFrame: Raw records with an added `_ingestion_metadata` column.

        Raises:
            DQCheckFailure: If any "error"-severity check in `dq_checks()` failed.
        """
        raw_df = self.fetch()
        metadata = IngestionMetadata(
            source_file_date=source_file_date,
            loaded_at=datetime.now(UTC),
            run_id=str(uuid.uuid4()),
            record_count=raw_df.count(),
        )
        result_df = raw_df.withColumn(
            "_ingestion_metadata",
            F.struct(
                F.lit(metadata.source_file_date).cast("date").alias("source_file_date"),
                F.lit(metadata.loaded_at).alias("loaded_at"),
                F.lit(metadata.run_id).alias("run_id"),
                F.lit(metadata.record_count).alias("record_count"),
            ),
        )
        raise_on_failures(self.dq_checks(result_df))
        return result_df

    def write_bronze(self, df: DataFrame) -> None:
        """Writes a DataFrame to the Bronze Delta table for this ingestor.

        Args:
            df (DataFrame): DataFrame produced by `run()`, ready to persist.
        """
        path = self._settings.bronze_table_path(self.bronze_table_name)
        df.write.format("delta").mode("append").save(path)

    def _detect_tombstones(
        self, df: DataFrame, key_column: str, watermark_key: str = DEFAULT_WATERMARK_KEY
    ) -> DataFrame:
        """Flags rows whose key from the previous run is missing from `df`.

        Compares this run's `key_column` values against the previous run's
        key-set (persisted in the watermark store's `extra` field as a sorted
        JSON list), adds `_is_current = true` to every row in `df` (nothing
        fetched this run is stale), and unions in synthetic tombstone rows --
        one per key present last run but absent this run -- with
        `_is_current = false` and every other column null except the key.
        Bronze itself stays append-only; this only adds a column Silver uses
        to filter out rows the source has since retracted.

        Args:
            df (DataFrame): This run's fetched records, before
                `_ingestion_metadata` is attached.
            key_column (str): Column holding each record's stable natural key.
            watermark_key (str): Which watermark row's `extra` field holds the
                previous key-set to compare against -- the source's default
                key for a single-scalar-watermark source (FEMA), or a
                per-partition key (NOAA's year string) when tombstones are
                tracked per sub-partition rather than for the whole source.

        Returns:
            DataFrame: `df` plus zero or more tombstone rows, with
                `_is_current` added. The caller is responsible for persisting
                this run's key-set via `self._watermark_store` afterward.
        """
        current_keys = {row[key_column] for row in df.select(key_column).distinct().collect()}
        watermark = self._watermark_store.get(self.bronze_table_name, watermark_key)
        previous_keys: set[str] = (
            set(json.loads(watermark.extra)) if watermark and watermark.extra else set()
        )

        missing_keys = previous_keys - current_keys
        result_df = df.withColumn("_is_current", F.lit(True))
        if missing_keys:
            tombstone_rows = [
                {**{f.name: None for f in df.schema.fields}, key_column: key}
                for key in missing_keys
            ]
            tombstone_df = self._spark.createDataFrame(tombstone_rows, schema=df.schema).withColumn(
                "_is_current", F.lit(False)
            )
            result_df = result_df.unionByName(tombstone_df)
        return result_df

    def _key_set_json(self, df: DataFrame, key_column: str) -> str:
        """Serializes this run's distinct key values, for a watermark row's `extra` field.

        Args:
            df (DataFrame): This run's fetched records (before tombstone rows
                were unioned in).
            key_column (str): Column holding each record's stable natural key.

        Returns:
            str: Sorted JSON list of key values, to store in
                `WatermarkRecord.extra` so the next run's
                `_detect_tombstones()` call can compare against it.
        """
        keys = sorted(str(row[key_column]) for row in df.select(key_column).distinct().collect())
        return json.dumps(keys)

    def _make_request(self, url: str, **kwargs: Any) -> requests.Response:
        """Issues a GET request with exponential-backoff retry.

        Args:
            url (str): Request URL.
            **kwargs (Any): Extra kwargs forwarded to `requests.get`
                (e.g. `params`, `headers`).

        Returns:
            requests.Response: The successful response.

        Raises:
            requests.RequestException: If all retry attempts fail.
        """
        delay = self._settings.retry_delay_seconds
        last_error: requests.RequestException | None = None
        for attempt in range(self._settings.max_retries + 1):
            try:
                response = requests.get(url, timeout=30, **kwargs)
                response.raise_for_status()
                return response
            except requests.RequestException as exc:
                last_error = exc
                if attempt < self._settings.max_retries:
                    time.sleep(delay)
                    delay *= self._settings.retry_backoff_factor
        assert last_error is not None
        raise last_error
