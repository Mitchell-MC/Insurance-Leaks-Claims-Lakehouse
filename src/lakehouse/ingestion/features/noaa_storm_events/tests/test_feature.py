"""Tests for NoaaStormEventsIngestor's directory discovery, watermarking, and tombstoning."""

import gzip
from datetime import UTC, datetime
from unittest.mock import Mock, patch

import pytest
from pyspark.sql import SparkSession

from lakehouse.config.config import LakehouseSettings
from lakehouse.ingestion.features.noaa_storm_events.feature import NoaaStormEventsIngestor
from lakehouse.ingestion.watermark_store import WatermarkRecord, WatermarkStore

_BASE_URL = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles"

_LISTING_HTML = """
<html><body>
<a href="StormEvents_details-ftp_v1.0_d2018_c20190101.csv.gz">2018 (out of range)</a>
<a href="StormEvents_details-ftp_v1.0_d2019_c20191201.csv.gz">2019 (older)</a>
<a href="StormEvents_details-ftp_v1.0_d2019_c20200516.csv.gz">2019 (newer, should win)</a>
<a href="StormEvents_details-ftp_v1.0_d2020_c20210408.csv.gz">2020</a>
</body></html>
"""

_YEAR_2019_CSV = "STATE,EVENT_TYPE,BEGIN_DATE_TIME\nTEXAS,Hurricane,01-JAN-19 00:00:00\n"
_YEAR_2020_CSV = "STATE,EVENT_TYPE,BEGIN_DATE_TIME,MAGNITUDE\nFLORIDA,Flood,15-JUN-20 00:00:00,50\n"


def _text_response(text: str) -> Mock:
    response = Mock()
    response.raise_for_status.return_value = None
    response.text = text
    return response


def _gzip_response(csv_text: str) -> Mock:
    response = Mock()
    response.raise_for_status.return_value = None
    response.content = gzip.compress(csv_text.encode("utf-8"))
    return response


def _mock_get_side_effect(url: str, **_kwargs: object) -> Mock:
    if url == f"{_BASE_URL}/":
        return _text_response(_LISTING_HTML)
    if url.endswith("d2019_c20200516.csv.gz"):
        return _gzip_response(_YEAR_2019_CSV)
    if url.endswith("d2020_c20210408.csv.gz"):
        return _gzip_response(_YEAR_2020_CSV)
    raise AssertionError(f"Unexpected request URL in test: {url}")


@pytest.fixture
def settings(tmp_path: object) -> LakehouseSettings:
    # Reason: each test gets its own storage_root so watermark state from one
    # test never leaks into another via a shared Delta table path.
    return LakehouseSettings(
        storage_root=f"file:///{tmp_path}".replace("\\", "/"),
        noaa_start_year=2019,
        noaa_end_year=2020,
    )


def test_bronze_table_name() -> None:
    """The ingestor targets the noaa_storm_events Bronze table."""
    assert NoaaStormEventsIngestor.bronze_table_name == "noaa_storm_events"


def test_fetch_filters_years_dedups_and_unions_with_column_drift(
    spark: SparkSession, settings: LakehouseSettings
) -> None:
    """fetch() picks the latest file per in-range year and unions drifted columns."""
    ingestor = NoaaStormEventsIngestor(settings, spark)

    with patch("lakehouse.ingestion.base_ingestor.requests.get", side_effect=_mock_get_side_effect):
        df = ingestor.fetch()

    rows = {row["DATA_YEAR"]: row for row in df.collect()}
    assert set(rows) == {"2019", "2020"}
    assert rows["2019"]["STATE"] == "TEXAS"
    assert rows["2019"]["MAGNITUDE"] is None
    assert rows["2020"]["MAGNITUDE"] == "50"


def test_latest_filename_per_year_picks_newest_creation_date(
    spark: SparkSession, settings: LakehouseSettings
) -> None:
    """The 2019 year, which has two published files, resolves to the newer c-date."""
    ingestor = NoaaStormEventsIngestor(settings, spark)

    resolved = ingestor._latest_filename_per_year(_LISTING_HTML)

    assert resolved[2019] == ("StormEvents_details-ftp_v1.0_d2019_c20200516.csv.gz", "20200516")
    assert resolved[2020] == ("StormEvents_details-ftp_v1.0_d2020_c20210408.csv.gz", "20210408")
    assert 2018 not in resolved


def test_fetch_skips_a_year_with_a_current_watermark_outside_recheck_window(
    spark: SparkSession, settings: LakehouseSettings
) -> None:
    """A year already ingested, and outside the recheck window, is never re-requested."""
    settings = settings.model_copy(update={"noaa_recheck_recent_years": 1})  # only 2020 rechecked
    store = WatermarkStore(settings, spark)
    store.set(
        WatermarkRecord(
            source_name="noaa_storm_events",
            watermark_key="2019",
            watermark_value="20200516",
            updated_at=datetime.now(UTC),
        )
    )
    ingestor = NoaaStormEventsIngestor(settings, spark, store)

    def _side_effect(url: str, **_kwargs: object) -> Mock:
        if url == f"{_BASE_URL}/":
            return _text_response(_LISTING_HTML)
        if url.endswith("d2020_c20210408.csv.gz"):
            return _gzip_response(_YEAR_2020_CSV)
        raise AssertionError(f"2019 should not be re-requested, got: {url}")

    with patch("lakehouse.ingestion.base_ingestor.requests.get", side_effect=_side_effect):
        df = ingestor.fetch()

    years = {row["DATA_YEAR"] for row in df.collect()}
    assert years == {"2020"}


def test_fetch_rechecks_a_recent_year_with_a_newer_creation_date(
    spark: SparkSession, settings: LakehouseSettings
) -> None:
    """A recent year whose stored watermark predates the listing's c-date is re-fetched."""
    store = WatermarkStore(settings, spark)
    store.set(
        WatermarkRecord(
            source_name="noaa_storm_events",
            watermark_key="2020",
            watermark_value="20200101",  # older than the listing's 20210408
            updated_at=datetime.now(UTC),
        )
    )
    ingestor = NoaaStormEventsIngestor(settings, spark, store)

    with patch("lakehouse.ingestion.base_ingestor.requests.get", side_effect=_mock_get_side_effect):
        df = ingestor.fetch()

    years = {row["DATA_YEAR"] for row in df.collect()}
    assert "2020" in years

    watermark = store.get("noaa_storm_events", "2020")
    assert watermark is not None
    assert watermark.watermark_value == "20210408"


def test_fetch_first_run_fetches_every_configured_year(
    spark: SparkSession, settings: LakehouseSettings
) -> None:
    """With no stored watermarks, every in-range year is fetched, unchanged from full-load."""
    ingestor = NoaaStormEventsIngestor(settings, spark)

    with patch("lakehouse.ingestion.base_ingestor.requests.get", side_effect=_mock_get_side_effect):
        df = ingestor.fetch()

    years = {row["DATA_YEAR"] for row in df.collect()}
    assert years == {"2019", "2020"}


def test_fetch_handles_missing_key_columns_without_raising(
    spark: SparkSession, settings: LakehouseSettings
) -> None:
    """A year's file lacking EVENT_ID/CZ_NAME (schema drift) still fetches successfully."""
    ingestor = NoaaStormEventsIngestor(settings, spark)

    with patch("lakehouse.ingestion.base_ingestor.requests.get", side_effect=_mock_get_side_effect):
        df = ingestor.fetch()

    assert df.count() == 2
    assert "_is_current" in df.columns
