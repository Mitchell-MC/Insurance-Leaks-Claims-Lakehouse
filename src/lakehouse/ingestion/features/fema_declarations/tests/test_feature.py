"""Tests for FemaDeclarationsIngestor's pagination and record shape."""

from unittest.mock import Mock, patch

from pyspark.sql import SparkSession

from lakehouse.config.config import LakehouseSettings
from lakehouse.ingestion.features.fema_declarations.feature import FemaDeclarationsIngestor


def _page_response(records: list[dict[str, object]]) -> Mock:
    response = Mock()
    response.raise_for_status.return_value = None
    response.json.return_value = {"DisasterDeclarationsSummaries": records}
    return response


def test_bronze_table_name() -> None:
    """The ingestor targets the fema_declarations Bronze table."""
    assert FemaDeclarationsIngestor.bronze_table_name == "fema_declarations"


def test_fetch_paginates_until_short_page(spark: SparkSession) -> None:
    """fetch() keeps requesting pages until one returns fewer than page_size rows."""
    settings = LakehouseSettings(fema_page_size=2)
    ingestor = FemaDeclarationsIngestor(settings, spark)

    page1 = _page_response(
        [
            {"disasterNumber": 1, "state": "TX", "incidentType": "Hurricane"},
            {"disasterNumber": 2, "state": "FL", "incidentType": "Flood"},
        ]
    )
    page2 = _page_response([{"disasterNumber": 3, "state": "TX", "incidentType": "Severe Storm"}])

    with patch("lakehouse.ingestion.base_ingestor.requests.get") as mock_get:
        mock_get.return_value.raise_for_status.return_value = None
        mock_get.side_effect = [page1, page2]
        df = ingestor.fetch()

    rows = {row["disasterNumber"] for row in df.collect()}
    assert rows == {1, 2, 3}
    assert mock_get.call_count == 2


def test_fetch_stops_after_single_short_page(spark: SparkSession) -> None:
    """fetch() issues one request when the first page is already short."""
    settings = LakehouseSettings(fema_page_size=10)
    ingestor = FemaDeclarationsIngestor(settings, spark)

    page1 = _page_response([{"disasterNumber": 1, "state": "TX", "incidentType": "Hurricane"}])

    with patch("lakehouse.ingestion.base_ingestor.requests.get", return_value=page1) as mock_get:
        df = ingestor.fetch()

    assert df.count() == 1
    assert mock_get.call_count == 1
