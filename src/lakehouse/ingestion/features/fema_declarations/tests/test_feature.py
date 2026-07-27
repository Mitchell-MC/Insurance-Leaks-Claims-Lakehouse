"""Tests for FemaDeclarationsIngestor's pagination, watermarking, and tombstoning."""

from datetime import UTC, datetime
from unittest.mock import Mock, patch

import pytest
from pyspark.sql import SparkSession

from lakehouse.config.config import LakehouseSettings
from lakehouse.ingestion.features.fema_declarations.feature import FemaDeclarationsIngestor
from lakehouse.ingestion.watermark_store import WatermarkRecord, WatermarkStore


def _page_response(records: list[dict[str, object]]) -> Mock:
    response = Mock()
    response.raise_for_status.return_value = None
    response.json.return_value = {"DisasterDeclarationsSummaries": records}
    return response


@pytest.fixture
def settings(tmp_path: object) -> LakehouseSettings:
    # Reason: an isolated storage_root so this test's WatermarkStore reads/
    # writes never leak into another test via a shared Delta table path.
    return LakehouseSettings(storage_root=f"file:///{tmp_path}".replace("\\", "/"))


def test_bronze_table_name() -> None:
    """The ingestor targets the fema_declarations Bronze table."""
    assert FemaDeclarationsIngestor.bronze_table_name == "fema_declarations"


def test_fetch_paginates_until_short_page(spark: SparkSession, settings: LakehouseSettings) -> None:
    """fetch() keeps requesting pages until one returns fewer than page_size rows."""
    settings = settings.model_copy(update={"fema_page_size": 2})
    ingestor = FemaDeclarationsIngestor(settings, spark)

    page1 = _page_response(
        [
            {
                "id": "a",
                "disasterNumber": 1,
                "state": "TX",
                "incidentType": "Hurricane",
                "lastRefresh": "2024-01-01T00:00:00.000Z",
            },
            {
                "id": "b",
                "disasterNumber": 2,
                "state": "FL",
                "incidentType": "Flood",
                "lastRefresh": "2024-01-02T00:00:00.000Z",
            },
        ]
    )
    page2 = _page_response(
        [
            {
                "id": "c",
                "disasterNumber": 3,
                "state": "TX",
                "incidentType": "Severe Storm",
                "lastRefresh": "2024-01-03T00:00:00.000Z",
            }
        ]
    )

    with patch("lakehouse.ingestion.base_ingestor.requests.get") as mock_get:
        mock_get.return_value.raise_for_status.return_value = None
        mock_get.side_effect = [page1, page2]
        df = ingestor.fetch()

    rows = {row["disasterNumber"] for row in df.collect()}
    assert rows == {1, 2, 3}
    assert mock_get.call_count == 2


def test_fetch_stops_after_single_short_page(
    spark: SparkSession, settings: LakehouseSettings
) -> None:
    """fetch() issues one request when the first page is already short."""
    settings = settings.model_copy(update={"fema_page_size": 10})
    ingestor = FemaDeclarationsIngestor(settings, spark)

    page1 = _page_response(
        [
            {
                "id": "a",
                "disasterNumber": 1,
                "state": "TX",
                "incidentType": "Hurricane",
                "lastRefresh": "2024-01-01T00:00:00.000Z",
            }
        ]
    )

    with patch("lakehouse.ingestion.base_ingestor.requests.get", return_value=page1) as mock_get:
        df = ingestor.fetch()

    assert df.count() == 1
    assert mock_get.call_count == 1


def test_fetch_omits_filter_on_first_run(spark: SparkSession, settings: LakehouseSettings) -> None:
    """No $filter param is sent when no watermark has been stored yet."""
    ingestor = FemaDeclarationsIngestor(settings, spark)
    page1 = _page_response(
        [
            {
                "id": "a",
                "disasterNumber": 1,
                "state": "TX",
                "incidentType": "Hurricane",
                "lastRefresh": "2024-01-01T00:00:00.000Z",
            }
        ]
    )

    with patch("lakehouse.ingestion.base_ingestor.requests.get", return_value=page1) as mock_get:
        ingestor.fetch()

    _, kwargs = mock_get.call_args
    assert "$filter" not in kwargs["params"]


def test_fetch_adds_filter_when_watermark_exists(
    spark: SparkSession, settings: LakehouseSettings
) -> None:
    """A stored watermark is used as a $filter=lastRefresh gt '...' param."""
    store = WatermarkStore(settings, spark)
    store.set(
        WatermarkRecord(
            source_name="fema_declarations",
            watermark_value="2024-01-01T00:00:00.000Z",
            updated_at=datetime.now(UTC),
        )
    )
    ingestor = FemaDeclarationsIngestor(settings, spark, store)
    page1 = _page_response(
        [
            {
                "id": "b",
                "disasterNumber": 2,
                "state": "FL",
                "incidentType": "Flood",
                "lastRefresh": "2024-02-01T00:00:00.000Z",
            }
        ]
    )

    with patch("lakehouse.ingestion.base_ingestor.requests.get", return_value=page1) as mock_get:
        ingestor.fetch()

    _, kwargs = mock_get.call_args
    assert kwargs["params"]["$filter"] == "lastRefresh gt '2024-01-01T00:00:00.000Z'"


def test_fetch_advances_watermark_to_max_last_refresh(
    spark: SparkSession, settings: LakehouseSettings
) -> None:
    """After a successful fetch, the watermark advances to the newest lastRefresh seen."""
    store = WatermarkStore(settings, spark)
    ingestor = FemaDeclarationsIngestor(settings, spark, store)
    page1 = _page_response(
        [
            {
                "id": "a",
                "disasterNumber": 1,
                "state": "TX",
                "incidentType": "Hurricane",
                "lastRefresh": "2024-01-01T00:00:00.000Z",
            },
            {
                "id": "b",
                "disasterNumber": 2,
                "state": "FL",
                "incidentType": "Flood",
                "lastRefresh": "2024-03-01T00:00:00.000Z",
            },
        ]
    )

    with patch("lakehouse.ingestion.base_ingestor.requests.get", return_value=page1):
        ingestor.fetch()

    watermark = store.get("fema_declarations")
    assert watermark is not None
    assert watermark.watermark_value == "2024-03-01T00:00:00.000Z"


def test_fetch_leaves_watermark_untouched_when_no_new_records(
    spark: SparkSession, settings: LakehouseSettings
) -> None:
    """An empty incremental page (nothing new) does not overwrite the existing watermark."""
    store = WatermarkStore(settings, spark)
    store.set(
        WatermarkRecord(
            source_name="fema_declarations",
            watermark_value="2024-05-01T00:00:00.000Z",
            updated_at=datetime.now(UTC),
        )
    )
    ingestor = FemaDeclarationsIngestor(settings, spark, store)
    empty_page = _page_response([])

    with patch("lakehouse.ingestion.base_ingestor.requests.get", return_value=empty_page):
        ingestor.fetch()

    watermark = store.get("fema_declarations")
    assert watermark is not None
    assert watermark.watermark_value == "2024-05-01T00:00:00.000Z"


def test_fetch_marks_first_run_records_current_with_no_prior_key_set(
    spark: SparkSession, settings: LakehouseSettings
) -> None:
    """A first (unfiltered) fetch flags every fetched record as current.

    Tombstone detection itself (a previously-seen key going missing) is unit
    tested directly against `BaseIngestor._detect_tombstones` in
    `test_base_ingestor.py`; this just confirms FEMA's `fetch()` wires the
    `_is_current` column through correctly on the first-run path.
    """
    ingestor = FemaDeclarationsIngestor(settings, spark)
    page1 = _page_response(
        [
            {
                "id": "a",
                "disasterNumber": 1,
                "state": "TX",
                "incidentType": "Hurricane",
                "lastRefresh": "2024-01-01T00:00:00.000Z",
            }
        ]
    )

    with patch("lakehouse.ingestion.base_ingestor.requests.get", return_value=page1):
        df = ingestor.fetch()

    rows = {row["id"]: row["_is_current"] for row in df.collect()}
    assert rows["a"] is True


def test_fetch_marks_incremental_run_records_current(
    spark: SparkSession, settings: LakehouseSettings
) -> None:
    """A filtered/incremental fetch flags every fetched record as current."""
    store = WatermarkStore(settings, spark)
    store.set(
        WatermarkRecord(
            source_name="fema_declarations",
            watermark_value="2024-01-01T00:00:00.000Z",
            updated_at=datetime.now(UTC),
        )
    )
    ingestor = FemaDeclarationsIngestor(settings, spark, store)
    page1 = _page_response(
        [
            {
                "id": "b",
                "disasterNumber": 2,
                "state": "FL",
                "incidentType": "Flood",
                "lastRefresh": "2024-02-01T00:00:00.000Z",
            }
        ]
    )

    with patch("lakehouse.ingestion.base_ingestor.requests.get", return_value=page1):
        df = ingestor.fetch()

    rows = {row["id"]: row["_is_current"] for row in df.collect()}
    assert rows["b"] is True
