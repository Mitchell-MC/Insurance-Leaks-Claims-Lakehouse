"""Bronze ingestor for the NOAA Storm Events Database bulk CSV files."""

import csv
import functools
import gzip
import io
import re
from datetime import date

from pyspark.sql import DataFrame, Row
from pyspark.sql.types import StringType, StructField, StructType

from lakehouse.ingestion.base_ingestor import BaseIngestor

_FILENAME_PATTERN = re.compile(r"StormEvents_details-ftp_v1\.0_d(\d{4})_c(\d{8})\.csv\.gz")


class NoaaStormEventsIngestor(BaseIngestor):
    """Ingests NOAA Storm Events bulk CSV files as a large historical event source.

    NOAA publishes one CSV.gz file per year with no stable name (the
    creation-date suffix varies), so this ingestor discovers filenames from
    the directory listing rather than constructing them directly. Column
    sets have drifted across 1996-present, so per-year DataFrames are
    combined with `unionByName(allowMissingColumns=True)` rather than
    assuming a fixed schema.
    """

    bronze_table_name = "noaa_storm_events"

    def fetch(self) -> DataFrame:
        """Fetches and unions one storm-events DataFrame per configured year.

        Returns:
            DataFrame: All matched years' event records, unioned by column
                name with missing columns padded as null.
        """
        base_url = self._settings.noaa_storm_events_base_url
        listing = self._make_request(f"{base_url}/").text
        filenames_by_year = self._latest_filename_per_year(listing)

        year_frames = [
            self._fetch_year(base_url, year, filename)
            for year, filename in sorted(filenames_by_year.items())
        ]
        return functools.reduce(
            lambda left, right: left.unionByName(right, allowMissingColumns=True), year_frames
        )

    def _latest_filename_per_year(self, listing_html: str) -> dict[int, str]:
        """Parses the directory listing for the newest file per requested year.

        Args:
            listing_html (str): Raw HTML of the NOAA CSV directory listing.

        Returns:
            dict[int, str]: Filename to download for each year in range,
                keeping the latest creation-date suffix when a year has more
                than one published file.
        """
        start_year = self._settings.noaa_start_year
        end_year = self._settings.noaa_end_year or date.today().year

        latest_by_year: dict[int, tuple[str, str]] = {}
        for match in _FILENAME_PATTERN.finditer(listing_html):
            year, created = int(match.group(1)), match.group(2)
            if not start_year <= year <= end_year:
                continue
            current = latest_by_year.get(year)
            if current is None or created > current[1]:
                latest_by_year[year] = (match.group(0), created)

        return {year: filename for year, (filename, _created) in latest_by_year.items()}

    def _fetch_year(self, base_url: str, year: int, filename: str) -> DataFrame:
        """Downloads, decompresses, and parses one year's storm-events file.

        Args:
            base_url (str): Directory URL the file lives under.
            year (int): Data year, attached as the `DATA_YEAR` column.
            filename (str): Filename discovered from the directory listing.

        Returns:
            DataFrame: That year's event records, all columns as strings.
        """
        response = self._make_request(f"{base_url}/{filename}")
        csv_text = gzip.decompress(response.content).decode("utf-8", errors="replace")
        reader = csv.DictReader(io.StringIO(csv_text))

        rows = []
        for record in reader:
            fields = {key: value for key, value in record.items() if key is not None}
            rows.append(Row(**fields, DATA_YEAR=str(year)))
        if not rows:
            schema = StructType([StructField("DATA_YEAR", StringType())])
            return self._spark.createDataFrame([], schema=schema)
        return self._spark.createDataFrame(rows)
