"""Bronze ingestor for the Census Gazetteer national counties reference file."""

import csv
import io
import zipfile
from datetime import UTC, datetime

from pyspark.sql import DataFrame, Row
from pyspark.sql.types import StringType, StructField, StructType

from lakehouse.ingestion.base_ingestor import BaseIngestor
from lakehouse.ingestion.watermark_store import WatermarkRecord

# Reason: shared by both the real fetch and the empty-skip path, so the two
# never drift apart -- an ingested row and a skipped run's empty DataFrame
# must be write-compatible with the same Bronze table.
_GAZETTEER_SCHEMA = StructType(
    [
        StructField("USPS", StringType()),
        StructField("GEOID", StringType()),
        StructField("NAME", StringType()),
        StructField("ALAND", StringType()),
        StructField("AWATER", StringType()),
    ]
)


class GeographyReferenceIngestor(BaseIngestor):
    """Ingests the Census Gazetteer national counties file for state/county normalization.

    Provides the reference geography (state, county name, FIPS codes, land/water
    area, centroid) used to normalize state/county fields across FEMA, NOAA, and
    complaint data during Silver standardization.

    Idempotent: this file only changes when Census publishes a new vintage
    year (`census_gazetteer_year`), so once a year has been successfully
    ingested, subsequent runs skip the download entirely rather than
    re-fetching the same file every time.
    """

    bronze_table_name = "geography_reference_counties"

    def fetch(self) -> DataFrame:
        """Downloads and parses the configured Gazetteer year, unless already ingested.

        Returns:
            DataFrame: One row per U.S. county, all columns as strings (typed
                casting happens in Silver). Empty (but correctly shaped) if
                this vintage year was already ingested by a previous run.
        """
        year = self._settings.census_gazetteer_year
        watermark = self._watermark_store.get(self.bronze_table_name)
        if watermark is not None and watermark.watermark_value == str(year):
            return self._spark.createDataFrame([], schema=_GAZETTEER_SCHEMA)

        url = (
            f"{self._settings.census_gazetteer_base_url}/{year}_Gazetteer/"
            f"{year}_Gaz_counties_national.zip"
        )
        response = self._make_request(url)
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            member = next(name for name in archive.namelist() if name.endswith(".txt"))
            text = archive.read(member).decode("latin-1")

        reader = csv.DictReader(io.StringIO(text), delimiter="\t")
        rows = [self._to_row(record) for record in reader]
        df = self._spark.createDataFrame(rows)

        self._watermark_store.set(
            WatermarkRecord(
                source_name=self.bronze_table_name,
                watermark_value=str(year),
                updated_at=datetime.now(UTC),
            )
        )
        return df

    @staticmethod
    def _to_row(record: dict[str, str | None]) -> Row:
        """Strips whitespace and drops the stray trailing-tab column, if present.

        Args:
            record (dict[str, str | None]): Raw DictReader record for one line.

        Returns:
            Row: Cleaned row with stripped column names and values.
        """
        cleaned = {
            key.strip(): (value.strip() if value is not None else None)
            for key, value in record.items()
            if key and key.strip()
        }
        return Row(**cleaned)
