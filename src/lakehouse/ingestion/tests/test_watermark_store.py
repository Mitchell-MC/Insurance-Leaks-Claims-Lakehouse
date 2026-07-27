"""Tests for WatermarkStore's Delta-backed read/write round-trip."""

from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession

from lakehouse.config.config import LakehouseSettings
from lakehouse.ingestion.watermark_store import WatermarkRecord, WatermarkStore


@pytest.fixture
def settings(tmp_path: object) -> LakehouseSettings:
    # Reason: each test gets its own storage_root so Delta state from one
    # test can never leak into another via a shared table path.
    return LakehouseSettings(storage_root=f"file:///{tmp_path}".replace("\\", "/"))


def test_get_returns_none_before_any_write(
    spark: SparkSession, settings: LakehouseSettings
) -> None:
    """get() returns None for a source with no watermark ever set."""
    store = WatermarkStore(settings, spark)

    assert store.get("fema_declarations") is None
    assert store.get_all("fema_declarations") == {}


def test_set_then_get_round_trips(spark: SparkSession, settings: LakehouseSettings) -> None:
    """A written watermark is returned by get() for its source/key."""
    store = WatermarkStore(settings, spark)
    record = WatermarkRecord(
        source_name="fema_declarations",
        watermark_value="2024-01-01T00:00:00.000Z",
        updated_at=datetime.now(UTC),
    )

    store.set(record)
    result = store.get("fema_declarations")

    assert result is not None
    assert result.watermark_value == "2024-01-01T00:00:00.000Z"


def test_set_twice_updates_in_place(spark: SparkSession, settings: LakehouseSettings) -> None:
    """Writing the same source/key twice updates the row rather than duplicating it."""
    store = WatermarkStore(settings, spark)
    store.set(
        WatermarkRecord(
            source_name="fema_declarations", watermark_value="first", updated_at=datetime.now(UTC)
        )
    )
    store.set(
        WatermarkRecord(
            source_name="fema_declarations", watermark_value="second", updated_at=datetime.now(UTC)
        )
    )

    result = store.get("fema_declarations")

    assert result is not None
    assert result.watermark_value == "second"
    raw_count = (
        spark.read.format("delta")
        .load(settings.bronze_table_path(settings.ingestion_state_table_name))
        .filter("source_name = 'fema_declarations'")
        .count()
    )
    assert raw_count == 1


def test_set_many_and_get_all_round_trip_per_year_keys(
    spark: SparkSession, settings: LakehouseSettings
) -> None:
    """set_many()/get_all() round-trip multiple watermark_keys under one source (NOAA's case)."""
    store = WatermarkStore(settings, spark)
    now = datetime.now(UTC)
    store.set_many(
        [
            WatermarkRecord(
                source_name="noaa_storm_events",
                watermark_key="2023",
                watermark_value="20240115",
                updated_at=now,
            ),
            WatermarkRecord(
                source_name="noaa_storm_events",
                watermark_key="2024",
                watermark_value="20250110",
                updated_at=now,
            ),
        ]
    )

    result = store.get_all("noaa_storm_events")

    assert set(result.keys()) == {"2023", "2024"}
    assert result["2023"].watermark_value == "20240115"
    assert result["2024"].watermark_value == "20250110"


def test_get_all_scoped_to_source_name(spark: SparkSession, settings: LakehouseSettings) -> None:
    """get_all() for one source never returns another source's rows."""
    store = WatermarkStore(settings, spark)
    now = datetime.now(UTC)
    store.set_many(
        [
            WatermarkRecord(source_name="fema_declarations", watermark_value="a", updated_at=now),
            WatermarkRecord(
                source_name="noaa_storm_events",
                watermark_key="2024",
                watermark_value="b",
                updated_at=now,
            ),
        ]
    )

    assert set(store.get_all("fema_declarations").keys()) == {"_default"}
    assert set(store.get_all("noaa_storm_events").keys()) == {"2024"}
