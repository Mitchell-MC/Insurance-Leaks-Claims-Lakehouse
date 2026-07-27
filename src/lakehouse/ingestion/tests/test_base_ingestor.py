"""Tests for BaseIngestor's metadata attachment and retry behavior."""

from datetime import UTC, datetime
from unittest.mock import Mock, patch

import pytest
import requests
from pyspark.sql import DataFrame, Row, SparkSession

from lakehouse.config.config import LakehouseSettings
from lakehouse.ingestion.base_ingestor import BaseIngestor
from lakehouse.ingestion.watermark_store import WatermarkRecord, WatermarkStore
from lakehouse.silver.data_quality.checks import DQCheckFailure, DQCheckResult


class _DummyIngestor(BaseIngestor):
    """Minimal concrete ingestor for exercising BaseIngestor behavior."""

    bronze_table_name = "dummy_table"

    def __init__(
        self,
        settings: LakehouseSettings,
        spark: SparkSession,
        rows: list[Row],
        watermark_store: WatermarkStore | None = None,
    ) -> None:
        super().__init__(settings, spark, watermark_store)
        self._rows = rows

    def fetch(self) -> DataFrame:
        return self._spark.createDataFrame(self._rows)


class _FailingDqIngestor(_DummyIngestor):
    """Ingestor whose dq_checks() always reports an error-severity failure."""

    def dq_checks(self, df: DataFrame) -> list[DQCheckResult]:
        return [DQCheckResult(check_name="always_fails", passed=False, failed_count=1)]


@pytest.fixture
def settings() -> LakehouseSettings:
    return LakehouseSettings(max_retries=2, retry_delay_seconds=0.01, retry_backoff_factor=2.0)


@pytest.fixture
def isolated_settings(tmp_path: object) -> LakehouseSettings:
    # Reason: tombstone-detection tests write real watermark state; each test
    # needs its own storage_root so that state can't leak between tests.
    return LakehouseSettings(storage_root=f"file:///{tmp_path}".replace("\\", "/"))


def test_run_attaches_ingestion_metadata(spark: SparkSession, settings: LakehouseSettings) -> None:
    """run() adds an _ingestion_metadata struct with the correct record count."""
    rows = [Row(value="a"), Row(value="b"), Row(value="c")]
    ingestor = _DummyIngestor(settings, spark, rows)

    result = ingestor.run().collect()

    assert len(result) == 3
    metadata = result[0]["_ingestion_metadata"]
    assert metadata["record_count"] == 3
    assert metadata["run_id"]
    assert metadata["loaded_at"] is not None


def test_run_raises_on_error_severity_dq_failure(
    spark: SparkSession, settings: LakehouseSettings
) -> None:
    """run() raises DQCheckFailure when dq_checks() reports an error-severity failure."""
    ingestor = _FailingDqIngestor(settings, spark, [Row(value="a")])

    with pytest.raises(DQCheckFailure, match="always_fails"):
        ingestor.run()


def test_make_request_retries_then_succeeds(
    spark: SparkSession, settings: LakehouseSettings
) -> None:
    """_make_request retries on failure and returns the response once it succeeds."""
    ingestor = _DummyIngestor(settings, spark, [])
    failing_response = Mock()
    failing_response.raise_for_status.side_effect = requests.ConnectionError("boom")
    ok_response = Mock()
    ok_response.raise_for_status.return_value = None

    with patch("lakehouse.ingestion.base_ingestor.requests.get") as mock_get:
        mock_get.side_effect = [failing_response, ok_response]
        result = ingestor._make_request("https://example.test")

    assert result is ok_response
    assert mock_get.call_count == 2


def test_make_request_raises_after_exhausting_retries(
    spark: SparkSession, settings: LakehouseSettings
) -> None:
    """_make_request re-raises the last error once retries are exhausted."""
    ingestor = _DummyIngestor(settings, spark, [])
    failing_response = Mock()
    failing_response.raise_for_status.side_effect = requests.ConnectionError("boom")

    with patch("lakehouse.ingestion.base_ingestor.requests.get", return_value=failing_response):
        with pytest.raises(requests.ConnectionError):
            ingestor._make_request("https://example.test")


def test_detect_tombstones_marks_all_rows_current_with_no_prior_key_set(
    spark: SparkSession, isolated_settings: LakehouseSettings
) -> None:
    """With no stored key-set, every fetched row is flagged current and nothing is tombstoned."""
    ingestor = _DummyIngestor(isolated_settings, spark, [Row(key="a"), Row(key="b")])

    result_df = ingestor._detect_tombstones(
        spark.createDataFrame([Row(key="a"), Row(key="b")]), key_column="key"
    )

    rows = {row["key"]: row["_is_current"] for row in result_df.collect()}
    assert rows == {"a": True, "b": True}


def test_detect_tombstones_flags_a_key_missing_from_this_run(
    spark: SparkSession, isolated_settings: LakehouseSettings
) -> None:
    """A key present in the stored previous key-set but absent from `df` is tombstoned."""
    store = WatermarkStore(isolated_settings, spark)
    store.set(
        WatermarkRecord(
            source_name="dummy_table",
            watermark_value="checkpoint",
            updated_at=datetime.now(UTC),
            extra='["a", "b"]',
        )
    )
    ingestor = _DummyIngestor(isolated_settings, spark, [], watermark_store=store)

    result_df = ingestor._detect_tombstones(spark.createDataFrame([Row(key="a")]), key_column="key")

    rows = {row["key"]: row["_is_current"] for row in result_df.collect()}
    assert rows == {"a": True, "b": False}
