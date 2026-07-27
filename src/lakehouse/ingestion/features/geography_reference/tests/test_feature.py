"""Tests for GeographyReferenceIngestor's zip/TSV parsing and idempotent skip."""

import io
import zipfile
from datetime import UTC, datetime
from unittest.mock import Mock, patch

import pytest
from pyspark.sql import SparkSession

from lakehouse.config.config import LakehouseSettings
from lakehouse.ingestion.features.geography_reference.feature import GeographyReferenceIngestor
from lakehouse.ingestion.watermark_store import WatermarkRecord, WatermarkStore

_GAZETTEER_TSV = (
    "USPS\tGEOID\tNAME\tALAND\tAWATER\t\n"
    "TX\t48201\tHarris County\t4416420000\t309291000\t\n"
    "FL\t12086\tMiami-Dade County\t4922843000\t1400682000\t\n"
)


def _zipped_gazetteer() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w") as archive:
        archive.writestr("2024_Gaz_counties_national.txt", _GAZETTEER_TSV)
    return buffer.getvalue()


@pytest.fixture
def settings(tmp_path: object) -> LakehouseSettings:
    # Reason: each test gets its own storage_root so watermark state from one
    # test never leaks into another via a shared Delta table path.
    return LakehouseSettings(storage_root=f"file:///{tmp_path}".replace("\\", "/"))


def test_bronze_table_name() -> None:
    """The ingestor targets the geography_reference_counties Bronze table."""
    assert GeographyReferenceIngestor.bronze_table_name == "geography_reference_counties"


def test_fetch_parses_gazetteer_rows_and_drops_trailing_tab_column(
    spark: SparkSession, settings: LakehouseSettings
) -> None:
    """fetch() parses each county row and drops the stray trailing-tab column."""
    settings = settings.model_copy(update={"census_gazetteer_year": 2024})
    ingestor = GeographyReferenceIngestor(settings, spark)
    response = Mock()
    response.raise_for_status.return_value = None
    response.content = _zipped_gazetteer()

    with patch("lakehouse.ingestion.base_ingestor.requests.get", return_value=response):
        df = ingestor.fetch()

    assert "" not in df.columns
    rows = {row["GEOID"]: row for row in df.collect()}
    assert rows["48201"]["NAME"] == "Harris County"
    assert rows["48201"]["USPS"] == "TX"
    assert rows["12086"]["NAME"] == "Miami-Dade County"


def test_fetch_requests_configured_gazetteer_year(
    spark: SparkSession, settings: LakehouseSettings
) -> None:
    """fetch() builds the download URL from the configured Gazetteer year."""
    settings = settings.model_copy(update={"census_gazetteer_year": 2023})
    ingestor = GeographyReferenceIngestor(settings, spark)
    response = Mock()
    response.raise_for_status.return_value = None
    response.content = _zipped_gazetteer()

    with patch("lakehouse.ingestion.base_ingestor.requests.get", return_value=response) as mock_get:
        ingestor.fetch()

    requested_url = mock_get.call_args[0][0]
    assert "2023_Gazetteer/2023_Gaz_counties_national.zip" in requested_url


def test_fetch_sets_watermark_after_a_real_fetch(
    spark: SparkSession, settings: LakehouseSettings
) -> None:
    """After fetching a vintage year for real, the watermark records that year."""
    settings = settings.model_copy(update={"census_gazetteer_year": 2024})
    store = WatermarkStore(settings, spark)
    ingestor = GeographyReferenceIngestor(settings, spark, store)
    response = Mock()
    response.raise_for_status.return_value = None
    response.content = _zipped_gazetteer()

    with patch("lakehouse.ingestion.base_ingestor.requests.get", return_value=response):
        ingestor.fetch()

    watermark = store.get("geography_reference_counties")
    assert watermark is not None
    assert watermark.watermark_value == "2024"


def test_fetch_skips_download_when_year_already_ingested(
    spark: SparkSession, settings: LakehouseSettings
) -> None:
    """A matching stored watermark skips the HTTP call and returns an empty, correctly shaped df."""
    settings = settings.model_copy(update={"census_gazetteer_year": 2024})
    store = WatermarkStore(settings, spark)
    store.set(
        WatermarkRecord(
            source_name="geography_reference_counties",
            watermark_value="2024",
            updated_at=datetime.now(UTC),
        )
    )
    ingestor = GeographyReferenceIngestor(settings, spark, store)

    with patch("lakehouse.ingestion.base_ingestor.requests.get") as mock_get:
        df = ingestor.fetch()

    mock_get.assert_not_called()
    assert df.count() == 0
    assert set(df.columns) == {"USPS", "GEOID", "NAME", "ALAND", "AWATER"}


def test_fetch_does_not_skip_a_different_vintage_year(
    spark: SparkSession, settings: LakehouseSettings
) -> None:
    """A stored watermark for a different year does not suppress a real fetch."""
    settings = settings.model_copy(update={"census_gazetteer_year": 2025})
    store = WatermarkStore(settings, spark)
    store.set(
        WatermarkRecord(
            source_name="geography_reference_counties",
            watermark_value="2024",
            updated_at=datetime.now(UTC),
        )
    )
    ingestor = GeographyReferenceIngestor(settings, spark, store)
    response = Mock()
    response.raise_for_status.return_value = None
    response.content = _zipped_gazetteer()

    with patch("lakehouse.ingestion.base_ingestor.requests.get", return_value=response) as mock_get:
        df = ingestor.fetch()

    mock_get.assert_called_once()
    assert df.count() == 2
