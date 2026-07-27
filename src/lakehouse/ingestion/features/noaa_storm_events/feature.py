"""Bronze ingestor for the NOAA Storm Events Database bulk CSV files."""

import csv
import functools
import gzip
import io
import re
from datetime import UTC, date, datetime

from pyspark.sql import Column, DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from lakehouse.ingestion.base_ingestor import BaseIngestor
from lakehouse.ingestion.dedup_keys import noaa_event_key
from lakehouse.ingestion.watermark_store import WatermarkRecord

_FILENAME_PATTERN = re.compile(r"StormEvents_details-ftp_v1\.0_d(\d{4})_c(\d{8})\.csv\.gz")


def _col_or_null(df: DataFrame, name: str) -> Column:
    """Returns `df[name]`, or a typed null literal if `name` isn't a column.

    Args:
        df (DataFrame): DataFrame to look up the column on.
        name (str): Column name to reference.

    Returns:
        Column: The real column, or `NULL` if this year's file lacks it --
            NOAA's schema has drifted across 1996-present, so a plain
            `F.col(name)` would raise for a genuinely absent column.
    """
    return F.col(name) if name in df.columns else F.lit(None).cast("string")


class NoaaStormEventsIngestor(BaseIngestor):
    """Ingests NOAA Storm Events bulk CSV files as a large historical event source.

    NOAA publishes one CSV.gz file per year with no stable name (the
    creation-date suffix varies), so this ingestor discovers filenames from
    the directory listing rather than constructing them directly. Column
    sets have drifted across 1996-present, so per-year DataFrames are
    combined with `unionByName(allowMissingColumns=True)` rather than
    assuming a fixed schema.

    Incremental: the directory listing is always fetched (required to
    discover filenames), but a year's file is only downloaded if it's new,
    or within `noaa_recheck_recent_years` of the present AND its
    creation-date is newer than the one already ingested. Older years are
    treated as frozen once any watermark exists for them. Tombstone
    detection is restricted to years actually re-fetched this run -- NOAA's
    multi-decade history is too large to key-set-diff in full, but recent
    years are exactly where revisions/corrections happen.
    """

    bronze_table_name = "noaa_storm_events"

    def fetch(self) -> DataFrame:
        """Fetches and unions storm-events DataFrames for years needing a refresh.

        Returns:
            DataFrame: Every re-fetched year's event records (plus tombstone
                rows for events that disappeared from a re-fetched year),
                unioned by column name with missing columns padded as null.
        """
        base_url = self._settings.noaa_storm_events_base_url
        listing = self._make_request(f"{base_url}/").text
        latest_by_year = self._latest_filename_per_year(listing)
        years_to_fetch = self._years_needing_fetch(latest_by_year)

        if not years_to_fetch:
            return self._spark.createDataFrame(
                [], schema=StructType([StructField("DATA_YEAR", StringType())])
            ).withColumn("_is_current", F.lit(True))

        year_frames = []
        new_watermarks = []
        for year, filename in sorted(years_to_fetch.items()):
            year_df, key_set_json = self._fetch_year_with_tombstones(base_url, year, filename)
            year_frames.append(year_df)
            new_watermarks.append(
                WatermarkRecord(
                    source_name=self.bronze_table_name,
                    watermark_key=str(year),
                    watermark_value=latest_by_year[year][1],
                    updated_at=datetime.now(UTC),
                    extra=key_set_json,
                )
            )
        self._watermark_store.set_many(new_watermarks)

        return functools.reduce(
            lambda left, right: left.unionByName(right, allowMissingColumns=True), year_frames
        )

    def _latest_filename_per_year(self, listing_html: str) -> dict[int, tuple[str, str]]:
        """Parses the directory listing for the newest file per requested year.

        Args:
            listing_html (str): Raw HTML of the NOAA CSV directory listing.

        Returns:
            dict[int, tuple[str, str]]: For each year in range, the filename
                and its creation-date suffix, keeping the latest suffix when
                a year has more than one published file.
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

        return latest_by_year

    def _years_needing_fetch(self, latest_by_year: dict[int, tuple[str, str]]) -> dict[int, str]:
        """Filters to years that are new, or recent and newly revised.

        Args:
            latest_by_year (dict[int, tuple[str, str]]): Every in-range
                year's latest available filename and creation-date.

        Returns:
            dict[int, str]: Filename to download for each year actually
                needing a (re)fetch this run.
        """
        watermarks = self._watermark_store.get_all(self.bronze_table_name)
        end_year = self._settings.noaa_end_year or date.today().year
        recheck_window = set(
            range(end_year - self._settings.noaa_recheck_recent_years + 1, end_year + 1)
        )

        result: dict[int, str] = {}
        for year, (filename, created) in latest_by_year.items():
            existing = watermarks.get(str(year))
            if existing is None:
                result[year] = filename
            elif year in recheck_window and created > existing.watermark_value:
                result[year] = filename
        return result

    def _fetch_year_with_tombstones(
        self, base_url: str, year: int, filename: str
    ) -> tuple[DataFrame, str]:
        """Downloads one year's file and flags events missing since its last fetch.

        Args:
            base_url (str): Directory URL the file lives under.
            year (int): Data year being fetched.
            filename (str): Filename discovered from the directory listing.

        Returns:
            tuple[DataFrame, str]: That year's records plus any tombstone
                rows (with `_is_current` added), and this year's key-set as
                JSON, to store in that year's watermark row's `extra` field
                for the next run's tombstone comparison.
        """
        df = self._fetch_year(base_url, year, filename)
        key_column = "_NOAA_EVENT_KEY"
        keyed_df = df.withColumn(
            key_column,
            noaa_event_key(
                _col_or_null(df, "EVENT_ID"),
                _col_or_null(df, "STATE"),
                _col_or_null(df, "EVENT_TYPE"),
                _col_or_null(df, "BEGIN_DATE_TIME"),
                _col_or_null(df, "CZ_NAME"),
            ),
        )
        key_set_json = self._key_set_json(keyed_df, key_column=key_column)
        result_df = self._detect_tombstones(
            keyed_df, key_column=key_column, watermark_key=str(year)
        ).drop(key_column)
        return result_df, key_set_json

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
