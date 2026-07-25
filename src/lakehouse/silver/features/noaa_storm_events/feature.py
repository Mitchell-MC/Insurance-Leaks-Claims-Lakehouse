"""Silver transformer for NOAA Storm Events."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from lakehouse.silver.base_transformer import BaseSilverTransformer
from lakehouse.silver.data_quality.checks import (
    DQCheckResult,
    check_accepted_values,
    check_expression,
    check_no_null_geography,
    check_schema_drift,
    check_valid_dates,
)
from lakehouse.silver.us_state_codes import STATE_NAME_TO_USPS

# Reason: the only values `_severity_band` ever derives -- see that function.
# A value outside this set means the banding logic changed without this
# check being updated, or a bug produced something else entirely.
_SEVERITY_BANDS = {"severe", "moderate", "minor"}

_EXPECTED_COLUMNS = {
    "EVENT_ID",
    "STATE",
    "CZ_NAME",
    "EVENT_TYPE",
    "EVENT_DATE",
    "DAMAGE_PROPERTY_USD",
    "MAGNITUDE",
    "DATA_YEAR",
    "SEVERITY_BAND",
}

# Reason: these event types carry outsized claims-leakage risk regardless of
# NOAA's self-reported dollar damage estimate (which is frequently a rough or
# missing figure for these types), so they're always banded "severe".
_ALWAYS_SEVERE_EVENT_TYPES = {
    "Hurricane (Typhoon)",
    "Tornado",
    "Flash Flood",
    "Storm Surge/Tide",
}


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

    def transform(self, bronze_df: DataFrame) -> DataFrame:
        """Normalizes fields, parses damage/severity, and dedupes events.

        Args:
            bronze_df (DataFrame): Raw NOAA storm events from Bronze.

        Returns:
            DataFrame: One row per distinct storm event, severity-banded.
        """
        standardized = bronze_df.select(
            F.col("EVENT_ID"),
            _map_state_to_usps(F.col("STATE")).alias("STATE"),
            F.col("CZ_NAME"),
            F.col("EVENT_TYPE"),
            F.to_timestamp(F.col("BEGIN_DATE_TIME"), "dd-MMM-yy HH:mm:ss")
            .cast("date")
            .alias("EVENT_DATE"),
            _parse_damage_property(F.col("DAMAGE_PROPERTY")).alias("DAMAGE_PROPERTY_USD"),
            F.col("MAGNITUDE"),
            F.col("DATA_YEAR"),
        )
        dedup_key = F.coalesce(
            F.col("EVENT_ID"),
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("STATE"),
                    F.col("EVENT_TYPE"),
                    F.col("EVENT_DATE").cast("string"),
                    F.col("CZ_NAME"),
                ),
                256,
            ),
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
        """Validates geography, event-date parsing, severity band, and schema drift.

        Args:
            df (DataFrame): Transformed NOAA storm events.

        Returns:
            list[DQCheckResult]: Results for all four checks.
        """
        return [
            check_no_null_geography(df, ["STATE"]),
            check_valid_dates(df, ["EVENT_DATE"]),
            check_accepted_values(df, "SEVERITY_BAND", _SEVERITY_BANDS),
            check_expression(
                df, F.col("DAMAGE_PROPERTY_USD") >= 0, "damage_property_usd_non_negative"
            ),
            check_schema_drift(df, _EXPECTED_COLUMNS),
        ]
