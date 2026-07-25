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

    assert not results["no_null_geography"].passed
    assert results["schema_drift"].passed


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

    assert not results["damage_property_usd_non_negative"].passed
