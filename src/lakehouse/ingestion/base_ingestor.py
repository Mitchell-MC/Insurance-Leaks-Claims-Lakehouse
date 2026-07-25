"""Abstract base class for Bronze-layer data ingestors."""

import time
import uuid
from abc import ABC, abstractmethod
from datetime import UTC, date, datetime

import requests
from pydantic import BaseModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.config.config import LakehouseSettings


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

    def __init__(self, settings: LakehouseSettings, spark: SparkSession) -> None:
        """Initializes the ingestor with typed settings and a Spark session.

        Args:
            settings (LakehouseSettings): Catalog/retry/API configuration.
            spark (SparkSession): Active Spark session used to build DataFrames.
        """
        self._settings = settings
        self._spark = spark

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

    def run(self, source_file_date: date | None = None) -> DataFrame:
        """Fetches source data and attaches the `_ingestion_metadata` struct column.

        Args:
            source_file_date (date | None): Date of the underlying source file,
                if the source exposes one. Defaults to None.

        Returns:
            DataFrame: Raw records with an added `_ingestion_metadata` column.
        """
        raw_df = self.fetch()
        metadata = IngestionMetadata(
            source_file_date=source_file_date,
            loaded_at=datetime.now(UTC),
            run_id=str(uuid.uuid4()),
            record_count=raw_df.count(),
        )
        return raw_df.withColumn(
            "_ingestion_metadata",
            F.struct(
                F.lit(metadata.source_file_date).cast("date").alias("source_file_date"),
                F.lit(metadata.loaded_at).alias("loaded_at"),
                F.lit(metadata.run_id).alias("run_id"),
                F.lit(metadata.record_count).alias("record_count"),
            ),
        )

    def write_bronze(self, df: DataFrame) -> None:
        """Writes a DataFrame to the Bronze Delta table for this ingestor.

        Args:
            df (DataFrame): DataFrame produced by `run()`, ready to persist.
        """
        path = self._settings.bronze_table_path(self.bronze_table_name)
        df.write.format("delta").mode("append").save(path)

    def _make_request(self, url: str, **kwargs: object) -> requests.Response:
        """Issues a GET request with exponential-backoff retry.

        Args:
            url (str): Request URL.
            **kwargs (object): Extra kwargs forwarded to `requests.get`
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
