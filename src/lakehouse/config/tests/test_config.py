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
