"""Bronze ingestor for the Census Gazetteer national counties reference file."""

import csv
import io
import zipfile

from pyspark.sql import DataFrame, Row

from lakehouse.ingestion.base_ingestor import BaseIngestor


class GeographyReferenceIngestor(BaseIngestor):
    """Ingests the Census Gazetteer national counties file for state/county normalization.

    Provides the reference geography (state, county name, FIPS codes, land/water
    area, centroid) used to normalize state/county fields across FEMA, NOAA, and
    complaint data during Silver standardization.
    """

    bronze_table_name = "geography_reference_counties"

    def fetch(self) -> DataFrame:
        """Downloads and parses the Census Gazetteer national counties file.

        Returns:
            DataFrame: One row per U.S. county, all columns as strings (typed
                casting happens in Silver).
        """
        year = self._settings.census_gazetteer_year
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
        return self._spark.createDataFrame(rows)

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
