"""Silver transformer for NOAA Storm Events."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from lakehouse.ingestion.dedup_keys import noaa_event_key
from lakehouse.silver.base_transformer import BaseSilverTransformer
from lakehouse.silver.data_quality.checks import DQCheckResult
from lakehouse.silver.data_quality.schema import check_against_schema
from lakehouse.silver.features.noaa_storm_events.schema import SCHEMA
from lakehouse.silver.us_state_codes import STATE_NAME_TO_USPS

# Reason: these event types carry outsized claims-leakage risk regardless of
# NOAA's self-reported dollar damage estimate (which is frequently a rough or
# missing figure for these types), so they're always banded "severe".
_ALWAYS_SEVERE_EVENT_TYPES = {
    "Hurricane (Typhoon)",
    "Tornado",
    "Flash Flood",
    "Storm Surge/Tide",
}


def _col_or_null(df: DataFrame, name: str) -> Column:
    """Returns `df[name]`, or a typed null literal if `name` isn't a column.

    Args:
        df (DataFrame): DataFrame to look up the column on.
        name (str): Column name to reference.

    Returns:
        Column: The real column, or `NULL` if Bronze's accumulated schema
            lacks it -- NOAA's column set has drifted across 1996-present
            (see docs/data_limitations.md), and Bronze's per-run
            `unionByName(allowMissingColumns=True)` only guarantees a
            column's presence within a single fetch, not across every
            historical append to the Delta table. Mirrors the same guard in
            `ingestion/features/noaa_storm_events/feature.py`.
    """
    return F.col(name) if name in df.columns else F.lit(None).cast("string")


def _null_if_blank(column: Column) -> Column:
    """Normalizes a blank/empty string to a true null.

    Args:
        column (Column): Raw string column.

    Returns:
        Column: `column` unchanged, or null if it was null or blank.
            Reason: NOAA's CSV reader parses an unset optional field (e.g.
            MAGNITUDE_TYPE on a non-wind/hail event, TOR_F_SCALE on a
            non-tornado event) as `""`, not a true null -- confirmed against
            live NOAA data, where this showed up as the majority of rows
            failing `accepted_values` checks despite `check_accepted_values`
            already excluding real nulls. Mirrors `_parse_damage_property`'s
            identical blank-string handling below.
    """
    return F.when(column.isNull() | (F.trim(column) == ""), F.lit(None).cast("string")).otherwise(
        column
    )


def _map_state_to_usps(column: Column) -> Column:
    """Maps a full state/territory name (e.g. "TEXAS") to its USPS code ("TX").

    Args:
        column (Column): Raw STATE name column (any case/whitespace).

    Returns:
        Column: 2-letter USPS code, or null if the name isn't a recognized
            US state/territory (e.g. one of NOAA's marine zones).
    """
    mapping_items = [item for pair in STATE_NAME_TO_USPS.items() for item in pair]
    mapping = F.create_map(*[F.lit(item) for item in mapping_items])
    return mapping[F.upper(F.trim(column))]


def _parse_damage_property(column: Column) -> Column:
    """Parses NOAA's "25.00K"/"1.50M"-style damage strings into USD.

    Args:
        column (Column): Raw DAMAGE_PROPERTY string column.

    Returns:
        Column: Parsed USD amount as a double, 0.0 for blank/null values.
    """
    numeric_part = F.regexp_extract(column, r"([0-9]*\.?[0-9]+)", 1).cast("double")
    suffix = F.upper(F.regexp_extract(column, r"([A-Za-z]+)$", 1))
    multiplier = (
        F.when(suffix == "K", F.lit(1_000.0))
        .when(suffix == "M", F.lit(1_000_000.0))
        .when(suffix == "B", F.lit(1_000_000_000.0))
        .otherwise(F.lit(1.0))
    )
    return F.when(column.isNull() | (F.trim(column) == ""), F.lit(0.0)).otherwise(
        numeric_part * multiplier
    )


def _severity_band(event_type: Column, damage_usd: Column) -> Column:
    """Derives a severe/moderate/minor band from event type and damage estimate.

    Args:
        event_type (Column): EVENT_TYPE column.
        damage_usd (Column): Parsed DAMAGE_PROPERTY_USD column.

    Returns:
        Column: "severe", "moderate", or "minor".
    """
    return (
        F.when(event_type.isin(*_ALWAYS_SEVERE_EVENT_TYPES), F.lit("severe"))
        .when(damage_usd >= 1_000_000, F.lit("severe"))
        .when(damage_usd >= 10_000, F.lit("moderate"))
        .otherwise(F.lit("minor"))
    )


class NoaaStormEventsTransformer(BaseSilverTransformer):
    """Standardizes Bronze NOAA Storm Events for downstream analytics.

    Maps NOAA's full state names to USPS codes (matching FEMA's and the
    Gazetteer's convention -- see `us_state_codes.py`), parses event dates
    and damage estimates to USD, derives a severity band, and dedupes on
    EVENT_ID (NOAA's true dedup key), falling back to a content hash for the
    rare blank-EVENT_ID record.
    """

    silver_table_name = "noaa_storm_events"
    drift_numeric_columns = ["DAMAGE_PROPERTY_USD"]

    def transform(self, bronze_df: DataFrame) -> DataFrame:
        """Normalizes fields, parses damage/severity, and dedupes events.

        Args:
            bronze_df (DataFrame): Raw NOAA storm events from Bronze.

        Returns:
            DataFrame: One row per distinct storm event, severity-banded.
        """
        current_bronze_df = self.filter_current(bronze_df)
        standardized = current_bronze_df.select(
            F.col("EVENT_ID"),
            _map_state_to_usps(F.col("STATE")).alias("STATE"),
            F.col("CZ_NAME"),
            F.col("EVENT_TYPE"),
            _null_if_blank(_col_or_null(current_bronze_df, "CZ_TYPE")).alias("CZ_TYPE"),
            _null_if_blank(_col_or_null(current_bronze_df, "MAGNITUDE_TYPE")).alias(
                "MAGNITUDE_TYPE"
            ),
            _null_if_blank(_col_or_null(current_bronze_df, "TOR_F_SCALE")).alias("TOR_F_SCALE"),
            _null_if_blank(_col_or_null(current_bronze_df, "FLOOD_CAUSE")).alias("FLOOD_CAUSE"),
            _null_if_blank(_col_or_null(current_bronze_df, "BEGIN_AZIMUTH")).alias("BEGIN_AZIMUTH"),
            _null_if_blank(_col_or_null(current_bronze_df, "END_AZIMUTH")).alias("END_AZIMUTH"),
            F.to_timestamp(F.col("BEGIN_DATE_TIME"), "dd-MMM-yy HH:mm:ss")
            .cast("date")
            .alias("EVENT_DATE"),
            _parse_damage_property(F.col("DAMAGE_PROPERTY")).alias("DAMAGE_PROPERTY_USD"),
            F.col("MAGNITUDE"),
            F.col("DATA_YEAR"),
        )
        dedup_key = noaa_event_key(
            F.col("EVENT_ID"),
            F.col("STATE"),
            F.col("EVENT_TYPE"),
            F.col("EVENT_DATE"),
            F.col("CZ_NAME"),
        )
        deduped = (
            standardized.withColumn("_DEDUP_KEY", dedup_key)
            .dropDuplicates(["_DEDUP_KEY"])
            .drop("_DEDUP_KEY")
        )
        return deduped.withColumn(
            "SEVERITY_BAND", _severity_band(F.col("EVENT_TYPE"), F.col("DAMAGE_PROPERTY_USD"))
        )

    def dq_checks(self, df: DataFrame) -> list[DQCheckResult]:
        """Validates the declared schema.

        Args:
            df (DataFrame): Transformed NOAA storm events.

        Returns:
            list[DQCheckResult]: Schema-derived checks (columns, types,
                not-null, damage-amount range, and SEVERITY_BAND's
                accepted-values check -- see `SCHEMA`).
        """
        return check_against_schema(df, SCHEMA)
