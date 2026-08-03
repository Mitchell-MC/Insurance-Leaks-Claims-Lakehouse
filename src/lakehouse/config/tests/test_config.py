"""Tests for LakehouseSettings."""

import pytest

from lakehouse.config.config import LakehouseSettings


def test_defaults_are_populated() -> None:
    """Settings load with sensible defaults when no env vars are set."""
    settings = LakehouseSettings()
    assert settings.catalog_name == "insurance_lakehouse"
    assert settings.bronze_schema == "bronze"
    assert settings.max_retries == 3


def test_env_prefix_overrides_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """LAKEHOUSE_-prefixed env vars override the corresponding field default."""
    monkeypatch.setenv("LAKEHOUSE_CATALOG_NAME", "custom_catalog")
    settings = LakehouseSettings()
    assert settings.catalog_name == "custom_catalog"


def test_bronze_table_path_joins_storage_root_and_schema() -> None:
    """bronze_table_path() builds a path under storage_root/bronze_schema/table_name."""
    settings = LakehouseSettings(storage_root="dbfs:/lakehouse")
    assert settings.bronze_table_path("fema_declarations") == (
        "dbfs:/lakehouse/bronze/fema_declarations"
    )


def test_silver_table_path_joins_storage_root_and_schema() -> None:
    """silver_table_path() builds a path under storage_root/silver_schema/table_name."""
    settings = LakehouseSettings(storage_root="dbfs:/lakehouse")
    assert settings.silver_table_path("fema_declarations") == (
        "dbfs:/lakehouse/silver/fema_declarations"
    )


def test_gold_table_path_joins_storage_root_and_schema() -> None:
    """gold_table_path() builds a path under storage_root/gold_schema/table_name."""
    settings = LakehouseSettings(storage_root="dbfs:/lakehouse")
    assert settings.gold_table_path("fact_catastrophe_event") == (
        "dbfs:/lakehouse/gold/fact_catastrophe_event"
    )


def test_powerbi_export_path_is_independent_of_storage_root() -> None:
    """powerbi_export_path() builds a path under powerbi_export_root, not storage_root.

    Reason: production storage_root values (dbfs:/..., abfss://...) aren't
    paths a local BI tool can open, so this must not derive from storage_root
    the way bronze/silver/gold_table_path do.
    """
    settings = LakehouseSettings(storage_root="abfss://lakehouse@example.dfs.core.windows.net")
    assert settings.powerbi_export_path("fact_catastrophe_event") == (
        "file:///data/powerbi_export/fact_catastrophe_event"
    )


def test_source_defaults_are_populated() -> None:
    """New ingestion-source settings load with sensible defaults."""
    settings = LakehouseSettings()
    assert settings.fema_page_size == 1000
    assert settings.noaa_start_year == 1996
    assert settings.noaa_end_year is None
    assert settings.census_gazetteer_year == 2024
    assert "weather.gov" not in settings.nws_user_agent


def test_noaa_end_year_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """LAKEHOUSE_NOAA_END_YEAR overrides the default open-ended end year."""
    monkeypatch.setenv("LAKEHOUSE_NOAA_END_YEAR", "2020")
    settings = LakehouseSettings()
    assert settings.noaa_end_year == 2020
