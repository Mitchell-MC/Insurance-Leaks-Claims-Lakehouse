"""Tests for NwsAlertsIngestor's GeoJSON parsing, request headers, and watermark recording."""

from unittest.mock import Mock, patch

import pytest
from pyspark.sql import SparkSession

from lakehouse.config.config import LakehouseSettings
from lakehouse.ingestion.features.nws_alerts.feature import NwsAlertsIngestor
from lakehouse.ingestion.watermark_store import WatermarkStore

_FEATURE_COLLECTION = {
    "features": [
        {
            "properties": {
                "id": "urn:oid:1",
                "areaDesc": "Harris, TX",
                "severity": "Severe",
                "urgency": "Immediate",
                "certainty": "Observed",
                "effective": "2026-07-24T00:00:00-05:00",
                "expires": "2026-07-24T06:00:00-05:00",
            },
            "geometry": None,
        },
        {
            "properties": {
                "id": "urn:oid:2",
                "areaDesc": "Miami-Dade, FL",
                "severity": "Moderate",
                "urgency": "Expected",
                "certainty": "Likely",
                "effective": "2026-07-24T00:00:00-05:00",
                "expires": "2026-07-24T12:00:00-05:00",
            },
            "geometry": {"type": "Point", "coordinates": [-80.19, 25.76]},
        },
    ]
}


@pytest.fixture
def settings(tmp_path: object) -> LakehouseSettings:
    # Reason: each test gets its own storage_root so watermark state from one
    # test never leaks into another via a shared Delta table path.
    return LakehouseSettings(storage_root=f"file:///{tmp_path}".replace("\\", "/"))


def test_bronze_table_name() -> None:
    """The ingestor targets the nws_alerts_snapshots Bronze table."""
    assert NwsAlertsIngestor.bronze_table_name == "nws_alerts_snapshots"


def test_fetch_parses_features_into_rows(spark: SparkSession, settings: LakehouseSettings) -> None:
    """fetch() flattens each GeoJSON feature's properties (+ geometry) into a row."""
    ingestor = NwsAlertsIngestor(settings, spark)
    response = Mock()
    response.raise_for_status.return_value = None
    response.json.return_value = _FEATURE_COLLECTION

    with patch("lakehouse.ingestion.base_ingestor.requests.get", return_value=response):
        df = ingestor.fetch()

    rows = {row["id"]: row for row in df.collect()}
    assert set(rows) == {"urn:oid:1", "urn:oid:2"}
    assert rows["urn:oid:1"]["severity"] == "Severe"
    assert rows["urn:oid:1"]["geometry"] is None


def test_fetch_sends_identifying_user_agent(
    spark: SparkSession, settings: LakehouseSettings
) -> None:
    """fetch() sends the configured User-Agent, as required by the NWS API."""
    settings = settings.model_copy(update={"nws_user_agent": "(my-app, test@example.com)"})
    ingestor = NwsAlertsIngestor(settings, spark)
    response = Mock()
    response.raise_for_status.return_value = None
    response.json.return_value = {
        "features": [{"properties": {"id": "urn:oid:1"}, "geometry": None}]
    }

    with patch("lakehouse.ingestion.base_ingestor.requests.get", return_value=response) as mock_get:
        ingestor.fetch()

    _, kwargs = mock_get.call_args
    assert kwargs["headers"]["User-Agent"] == "(my-app, test@example.com)"


def test_run_passes_freshness_check_for_a_fresh_snapshot(
    spark: SparkSession, settings: LakehouseSettings
) -> None:
    """run() reports the freshness check as passing right after a fresh fetch."""
    ingestor = NwsAlertsIngestor(settings, spark)
    response = Mock()
    response.raise_for_status.return_value = None
    response.json.return_value = _FEATURE_COLLECTION

    with patch("lakehouse.ingestion.base_ingestor.requests.get", return_value=response):
        df = ingestor.run()

    results = {result.check_name: result for result in ingestor.dq_checks(df)}
    assert results["freshness__ingestion_metadata.loaded_at"].passed
    assert results["freshness__ingestion_metadata.loaded_at"].severity == "warn"


def test_fetch_issues_one_request_regardless_of_watermark_state(
    spark: SparkSession, settings: LakehouseSettings
) -> None:
    """A full snapshot is fetched every call -- no filter/skip based on a prior watermark."""
    store = WatermarkStore(settings, spark)
    ingestor = NwsAlertsIngestor(settings, spark, store)
    response = Mock()
    response.raise_for_status.return_value = None
    response.json.return_value = _FEATURE_COLLECTION

    with patch("lakehouse.ingestion.base_ingestor.requests.get", return_value=response) as mock_get:
        ingestor.fetch()
        ingestor.fetch()

    assert mock_get.call_count == 2


def test_fetch_records_a_fresh_watermark_every_call(
    spark: SparkSession, settings: LakehouseSettings
) -> None:
    """Each fetch() call updates the watermark's timestamp, as a last-polled-at marker."""
    store = WatermarkStore(settings, spark)
    ingestor = NwsAlertsIngestor(settings, spark, store)
    response = Mock()
    response.raise_for_status.return_value = None
    response.json.return_value = _FEATURE_COLLECTION

    with patch("lakehouse.ingestion.base_ingestor.requests.get", return_value=response):
        ingestor.fetch()

    watermark = store.get("nws_alerts_snapshots")
    assert watermark is not None
    assert watermark.watermark_value
