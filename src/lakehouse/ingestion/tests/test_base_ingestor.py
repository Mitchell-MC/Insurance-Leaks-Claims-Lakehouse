"""Tests for BaseIngestor's metadata attachment and retry behavior."""

from unittest.mock import Mock, patch

import pytest
import requests
from pyspark.sql import Row, SparkSession

from lakehouse.config.config import LakehouseSettings
from lakehouse.ingestion.base_ingestor import BaseIngestor


class _DummyIngestor(BaseIngestor):
    """Minimal concrete ingestor for exercising BaseIngestor behavior."""

    bronze_table_name = "dummy_table"

    def __init__(self, settings: LakehouseSettings, spark: SparkSession, rows: list[Row]) -> None:
        super().__init__(settings, spark)
        self._rows = rows

    def fetch(self):
        return self._spark.createDataFrame(self._rows)


@pytest.fixture
def settings() -> LakehouseSettings:
    return LakehouseSettings(max_retries=2, retry_delay_seconds=0.01, retry_backoff_factor=2.0)


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
