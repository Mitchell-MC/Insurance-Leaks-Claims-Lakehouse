"""Tests for FemaDeclarationsTransformer's normalization and DQ checks."""

from datetime import date

from pyspark.sql import Row, SparkSession

from lakehouse.config.config import LakehouseSettings
from lakehouse.silver.features.fema_declarations.feature import FemaDeclarationsTransformer


def test_bronze_table_name() -> None:
    """The transformer targets the fema_declarations Silver table."""
    assert FemaDeclarationsTransformer.silver_table_name == "fema_declarations"


def test_transform_normalizes_state_and_parses_dates(spark: SparkSession) -> None:
    """transform() uppercases state and parses ISO datetime strings to dates."""
    transformer = FemaDeclarationsTransformer(LakehouseSettings(), spark)
    bronze_df = spark.createDataFrame(
        [
            Row(
                disasterNumber=4586,
                state=" tx ",
                incidentType="Hurricane",
                declarationDate="2020-08-27T00:00:00.000Z",
                incidentBeginDate="2020-08-23T00:00:00.000Z",
                designatedArea="Harris (County)",
            )
        ]
    )

    result = transformer.transform(bronze_df).collect()

    assert result[0]["state"] == "TX"
    assert str(result[0]["declarationDate"]) == "2020-08-27"
    assert str(result[0]["incidentBeginDate"]) == "2020-08-23"


def test_dq_checks_flag_null_state_and_duplicate_keys(spark: SparkSession) -> None:
    """dq_checks() surfaces a null-geography failure and a duplicate-key failure."""
    transformer = FemaDeclarationsTransformer(LakehouseSettings(), spark)
    df = spark.createDataFrame(
        [
            Row(
                disasterNumber=4586,
                state="TX",
                incidentType="Hurricane",
                declarationDate=date(2020, 8, 27),
                incidentBeginDate=date(2020, 8, 23),
                designatedArea="Harris (County)",
            ),
            Row(
                disasterNumber=4586,
                state=None,
                incidentType="Hurricane",
                declarationDate=None,
                incidentBeginDate=None,
                designatedArea="Harris (County)",
            ),
        ]
    )

    results = {result.check_name: result for result in transformer.dq_checks(df)}

    assert not results["no_null_geography"].passed
    assert not results["valid_dates"].passed
    assert not results["no_duplicate_keys"].passed
    assert results["schema_drift"].passed


def test_dq_checks_flag_declaration_before_incident(spark: SparkSession) -> None:
    """dq_checks() flags a declaration dated before its own incident began."""
    transformer = FemaDeclarationsTransformer(LakehouseSettings(), spark)
    df = spark.createDataFrame(
        [
            Row(
                disasterNumber=4586,
                state="TX",
                incidentType="Hurricane",
                declarationDate=date(2020, 8, 20),
                incidentBeginDate=date(2020, 8, 23),
                designatedArea="Harris (County)",
            )
        ]
    )

    results = {result.check_name: result for result in transformer.dq_checks(df)}

    assert not results["declaration_not_before_incident"].passed
    assert results["declaration_not_before_incident"].failed_count == 1
