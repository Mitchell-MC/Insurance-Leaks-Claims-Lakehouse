"""Tests for GeographyReferenceIngestor's zip/TSV parsing."""

import io
import zipfile
from unittest.mock import Mock, patch

from pyspark.sql import SparkSession

from lakehouse.config.config import LakehouseSettings
from lakehouse.ingestion.features.geography_reference.feature import GeographyReferenceIngestor

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


def test_bronze_table_name() -> None:
    """The ingestor targets the geography_reference_counties Bronze table."""
    assert GeographyReferenceIngestor.bronze_table_name == "geography_reference_counties"


def test_fetch_parses_gazetteer_rows_and_drops_trailing_tab_column(
    spark: SparkSession,
) -> None:
    """fetch() parses each county row and drops the stray trailing-tab column."""
    settings = LakehouseSettings(census_gazetteer_year=2024)
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


def test_fetch_requests_configured_gazetteer_year(spark: SparkSession) -> None:
    """fetch() builds the download URL from the configured Gazetteer year."""
    settings = LakehouseSettings(census_gazetteer_year=2023)
    ingestor = GeographyReferenceIngestor(settings, spark)
    response = Mock()
    response.raise_for_status.return_value = None
    response.content = _zipped_gazetteer()

    with patch("lakehouse.ingestion.base_ingestor.requests.get", return_value=response) as mock_get:
        ingestor.fetch()

    requested_url = mock_get.call_args[0][0]
    assert "2023_Gazetteer/2023_Gaz_counties_national.zip" in requested_url
