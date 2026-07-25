"""Typed, environment-driven settings for the lakehouse pipeline."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class LakehouseSettings(BaseSettings):
    """Environment-driven configuration for catalog names, storage paths, and API access.

    Attributes:
        catalog_name (str): Unity Catalog catalog holding bronze/silver/gold schemas.
        bronze_schema (str): Schema name for raw ingested data.
        silver_schema (str): Schema name for standardized/deduplicated data.
        gold_schema (str): Schema name for the dimensional model.
        storage_root (str): Root path (e.g. abfss://... or dbfs:/) for Delta table storage.
        nws_api_base_url (str): Base URL for the National Weather Service Alerts API.
        nws_user_agent (str): User-Agent header sent to the NWS API. NWS requests
            a real identifying string (app name + contact); override via
            `LAKEHOUSE_NWS_USER_AGENT` before hitting the live API.
        fema_api_base_url (str): Base URL for the OpenFEMA Disaster Declarations API.
        fema_page_size (int): Records requested per OpenFEMA pagination page.
        noaa_storm_events_base_url (str): Directory URL for NOAA Storm Events bulk CSV files.
        noaa_start_year (int): First year of NOAA Storm Events data to ingest.
        noaa_end_year (int | None): Last year of NOAA Storm Events data to ingest.
            None means "through the current year" at fetch time.
        census_gazetteer_base_url (str): Base URL for Census Gazetteer files.
        census_gazetteer_year (int): Gazetteer vintage year to download.
        max_retries (int): Maximum retry attempts for API/network calls.
        retry_delay_seconds (float): Initial delay before the first retry.
        retry_backoff_factor (float): Multiplier applied to the delay after each retry.
    """

    model_config = SettingsConfigDict(env_prefix="LAKEHOUSE_", env_file=".env", extra="ignore")

    catalog_name: str = "insurance_lakehouse"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"

    storage_root: str = "dbfs:/lakehouse"

    nws_api_base_url: str = "https://api.weather.gov"
    nws_user_agent: str = "(insurance-claims-lakehouse-portfolio, set LAKEHOUSE_NWS_USER_AGENT)"

    fema_api_base_url: str = "https://www.fema.gov/api/open/v2"
    fema_page_size: int = Field(default=1000, gt=0)

    noaa_storm_events_base_url: str = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles"
    noaa_start_year: int = 1996
    noaa_end_year: int | None = None

    census_gazetteer_base_url: str = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer"
    census_gazetteer_year: int = 2024

    max_retries: int = Field(default=3, ge=0)
    retry_delay_seconds: float = Field(default=1.0, gt=0)
    retry_backoff_factor: float = Field(default=2.0, ge=1.0)

    def bronze_table_path(self, table_name: str) -> str:
        """Builds the storage path for a bronze-layer Delta table.

        Args:
            table_name (str): Unqualified table name (e.g. "fema_declarations").

        Returns:
            str: Fully qualified storage path under the bronze schema.
        """
        return f"{self.storage_root}/{self.bronze_schema}/{table_name}"

    def silver_table_path(self, table_name: str) -> str:
        """Builds the storage path for a silver-layer Delta table.

        Args:
            table_name (str): Unqualified table name (e.g. "fema_declarations").

        Returns:
            str: Fully qualified storage path under the silver schema.
        """
        return f"{self.storage_root}/{self.silver_schema}/{table_name}"

    def gold_table_path(self, table_name: str) -> str:
        """Builds the storage path for a gold-layer Delta table.

        Args:
            table_name (str): Unqualified table name (e.g. "fact_catastrophe_event").

        Returns:
            str: Fully qualified storage path under the gold schema.
        """
        return f"{self.storage_root}/{self.gold_schema}/{table_name}"
