"""Tests for NoaaStormEventsTransformer's parsing, severity banding, and dedup."""

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

from lakehouse.config.config import LakehouseSettings
from lakehouse.silver.features.noaa_storm_events.feature import NoaaStormEventsTransformer


def _row(**overrides: object) -> Row:
    defaults: dict[str, object] = {
        "EVENT_ID": "1001",
        "STATE": "TEXAS",
        "CZ_NAME": "HARRIS",
        "EVENT_TYPE": "Thunderstorm Wind",
        "CZ_TYPE": "C",
        "MAGNITUDE_TYPE": "MG",
        # Reason: a column that's None in every row of the test DataFrame defeats
        # PySpark's schema inference (it can't determine a NullType column's real
        # type), so these defaults use real values even though the field is
        # nullable in production (null for non-tornado/non-flood events).
        "TOR_F_SCALE": "EF1",
        "FLOOD_CAUSE": "Heavy Rain",
        "BEGIN_AZIMUTH": "ENE",
        "END_AZIMUTH": "ENE",
        "BEGIN_DATE_TIME": "27-AUG-20 08:00:00",
        "DAMAGE_PROPERTY": "25.00K",
        "MAGNITUDE": "60",
        "DATA_YEAR": "2020",
    }
    defaults.update(overrides)
    return Row(**defaults)


def test_bronze_table_name() -> None:
    """The transformer targets the noaa_storm_events Silver table."""
    assert NoaaStormEventsTransformer.silver_table_name == "noaa_storm_events"


def test_drift_numeric_columns_includes_damage_property(spark: SparkSession) -> None:
    """DAMAGE_PROPERTY_USD is checked for silent aggregate drift run-over-run.

    Reason: a unit-conversion or parsing regression in _parse_damage_property
    would pass every per-row schema/range check while still silently shifting
    the table's total damage estimate -- see check_no_silent_drift.
    """
    transformer = NoaaStormEventsTransformer(LakehouseSettings(), spark)

    assert transformer.drift_numeric_columns == ["DAMAGE_PROPERTY_USD"]


def test_transform_maps_full_state_name_to_usps_and_parses_date(spark: SparkSession) -> None:
    """transform() maps NOAA's full state name to a USPS code and parses the date."""
    transformer = NoaaStormEventsTransformer(LakehouseSettings(), spark)
    bronze_df = spark.createDataFrame([_row(STATE=" texas ")])

    result = transformer.transform(bronze_df).collect()

    assert result[0]["STATE"] == "TX"
    assert str(result[0]["EVENT_DATE"]) == "2020-08-27"


def test_transform_parses_damage_property_suffixes(spark: SparkSession) -> None:
    """transform() converts K/M-suffixed damage strings into USD."""
    transformer = NoaaStormEventsTransformer(LakehouseSettings(), spark)
    bronze_df = spark.createDataFrame(
        [
            _row(EVENT_ID="1", DAMAGE_PROPERTY="25.00K"),
            _row(EVENT_ID="2", DAMAGE_PROPERTY="1.50M"),
            _row(EVENT_ID="3", DAMAGE_PROPERTY=""),
        ]
    )

    rows = {row["EVENT_ID"]: row for row in transformer.transform(bronze_df).collect()}

    assert rows["1"]["DAMAGE_PROPERTY_USD"] == 25_000.0
    assert rows["2"]["DAMAGE_PROPERTY_USD"] == 1_500_000.0
    assert rows["3"]["DAMAGE_PROPERTY_USD"] == 0.0


def test_transform_bands_severity(spark: SparkSession) -> None:
    """transform() bands tornadoes as severe regardless of damage, else by dollar amount."""
    transformer = NoaaStormEventsTransformer(LakehouseSettings(), spark)
    bronze_df = spark.createDataFrame(
        [
            _row(EVENT_ID="1", EVENT_TYPE="Tornado", DAMAGE_PROPERTY="0.00K"),
            _row(EVENT_ID="2", EVENT_TYPE="Thunderstorm Wind", DAMAGE_PROPERTY="5.00K"),
            _row(EVENT_ID="3", EVENT_TYPE="Thunderstorm Wind", DAMAGE_PROPERTY="500.00K"),
            _row(EVENT_ID="4", EVENT_TYPE="Thunderstorm Wind", DAMAGE_PROPERTY="2.00M"),
        ]
    )

    rows = {row["EVENT_ID"]: row for row in transformer.transform(bronze_df).collect()}

    assert rows["1"]["SEVERITY_BAND"] == "severe"
    assert rows["2"]["SEVERITY_BAND"] == "minor"
    assert rows["3"]["SEVERITY_BAND"] == "moderate"
    assert rows["4"]["SEVERITY_BAND"] == "severe"


def test_transform_nulls_drift_prone_columns_absent_from_an_older_years_schema(
    spark: SparkSession,
) -> None:
    """A Bronze batch lacking a drift-prone column (e.g. an early NOAA year) still transforms.

    Reason: NOAA's column set has drifted across 1996-present (some early
    years lack MAGNITUDE_TYPE and similar fields -- see
    docs/data_limitations.md), so these columns must degrade to null rather
    than raise when Bronze's schema doesn't include them.
    """
    transformer = NoaaStormEventsTransformer(LakehouseSettings(), spark)
    row = _row().asDict()
    for column in (
        "CZ_TYPE",
        "MAGNITUDE_TYPE",
        "TOR_F_SCALE",
        "FLOOD_CAUSE",
        "BEGIN_AZIMUTH",
        "END_AZIMUTH",
    ):
        del row[column]
    bronze_df = spark.createDataFrame([Row(**row)])

    result = transformer.transform(bronze_df).collect()

    assert result[0]["CZ_TYPE"] is None
    assert result[0]["MAGNITUDE_TYPE"] is None
    assert result[0]["TOR_F_SCALE"] is None
    assert result[0]["FLOOD_CAUSE"] is None
    assert result[0]["BEGIN_AZIMUTH"] is None
    assert result[0]["END_AZIMUTH"] is None


def test_transform_nulls_blank_string_drift_prone_columns(spark: SparkSession) -> None:
    """A Bronze row where a drift-prone column is `""` (not null) still nulls out in Silver.

    Reason: confirmed against live NOAA data -- CSV rows where these fields
    are legitimately unset (e.g. MAGNITUDE_TYPE on a non-wind/hail event)
    parse as an empty string, not a true null, which the accepted_values
    check does not exempt the way it exempts real nulls.
    """
    transformer = NoaaStormEventsTransformer(LakehouseSettings(), spark)
    bronze_df = spark.createDataFrame(
        [
            _row(
                MAGNITUDE_TYPE="",
                TOR_F_SCALE="",
                FLOOD_CAUSE="",
                BEGIN_AZIMUTH="",
                END_AZIMUTH="",
            )
        ]
    )

    result = transformer.transform(bronze_df).collect()

    assert result[0]["MAGNITUDE_TYPE"] is None
    assert result[0]["TOR_F_SCALE"] is None
    assert result[0]["FLOOD_CAUSE"] is None
    assert result[0]["BEGIN_AZIMUTH"] is None
    assert result[0]["END_AZIMUTH"] is None


def test_transform_dedupes_by_event_id(spark: SparkSession) -> None:
    """transform() collapses duplicate EVENT_ID rows to one."""
    transformer = NoaaStormEventsTransformer(LakehouseSettings(), spark)
    bronze_df = spark.createDataFrame([_row(EVENT_ID="1001"), _row(EVENT_ID="1001")])

    result = transformer.transform(bronze_df)

    assert result.count() == 1


def test_dq_checks_flag_null_state(spark: SparkSession) -> None:
    """dq_checks() surfaces a null-geography failure."""
    transformer = NoaaStormEventsTransformer(LakehouseSettings(), spark)
    bronze_df = spark.createDataFrame(
        [_row(EVENT_ID="1", STATE=None), _row(EVENT_ID="2", STATE="TEXAS")]
    )
    df = transformer.transform(bronze_df)

    results = {result.check_name: result for result in transformer.dq_checks(df)}

    assert not results["schema_not_null_STATE"].passed
    assert results["schema_columns"].passed


def test_dq_checks_flag_unknown_severity_band(spark: SparkSession) -> None:
    """dq_checks() flags a SEVERITY_BAND value outside severe/moderate/minor."""
    transformer = NoaaStormEventsTransformer(LakehouseSettings(), spark)
    df = transformer.transform(spark.createDataFrame([_row(EVENT_ID="1")])).withColumn(
        "SEVERITY_BAND", F.lit("catastrophic")
    )

    results = {result.check_name: result for result in transformer.dq_checks(df)}

    assert not results["accepted_values_SEVERITY_BAND"].passed


def test_dq_checks_flag_negative_damage(spark: SparkSession) -> None:
    """dq_checks() flags a negative DAMAGE_PROPERTY_USD value."""
    transformer = NoaaStormEventsTransformer(LakehouseSettings(), spark)
    df = transformer.transform(spark.createDataFrame([_row(EVENT_ID="1")])).withColumn(
        "DAMAGE_PROPERTY_USD", F.lit(-100.0)
    )

    results = {result.check_name: result for result in transformer.dq_checks(df)}

    assert not results["schema_range_DAMAGE_PROPERTY_USD"].passed
