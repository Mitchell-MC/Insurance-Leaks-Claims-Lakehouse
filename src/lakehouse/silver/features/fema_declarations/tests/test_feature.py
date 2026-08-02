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
                declarationType="DR",
                region=6,
                ihProgramDeclared=True,
                iaProgramDeclared=True,
                paProgramDeclared=True,
                hmProgramDeclared=True,
                tribalRequest=False,
                declarationDate="2020-08-27T00:00:00.000Z",
                incidentBeginDate="2020-08-23T00:00:00.000Z",
                designatedArea="Harris (County)",
            )
        ]
    )

    result = transformer.transform(bronze_df).collect()

    assert result[0]["state"] == "TX"
    assert result[0]["declarationType"] == "DR"
    assert result[0]["region"] == 6
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
                declarationType="DR",
                region=6,
                ihProgramDeclared=True,
                iaProgramDeclared=True,
                paProgramDeclared=True,
                hmProgramDeclared=True,
                tribalRequest=False,
                declarationDate=date(2020, 8, 27),
                incidentBeginDate=date(2020, 8, 23),
                designatedArea="Harris (County)",
            ),
            Row(
                disasterNumber=4586,
                state=None,
                incidentType="Hurricane",
                declarationType="DR",
                region=6,
                ihProgramDeclared=True,
                iaProgramDeclared=True,
                paProgramDeclared=True,
                hmProgramDeclared=True,
                tribalRequest=False,
                declarationDate=date(2020, 8, 27),
                incidentBeginDate=date(2020, 8, 23),
                designatedArea="Harris (County)",
            ),
        ]
    )

    results = {result.check_name: result for result in transformer.dq_checks(df)}

    assert not results["schema_not_null_state"].passed
    assert not results["schema_unique_key"].passed
    assert results["schema_columns"].passed


def test_dq_checks_flag_declaration_before_incident(spark: SparkSession) -> None:
    """dq_checks() flags a declaration dated before its own incident began."""
    transformer = FemaDeclarationsTransformer(LakehouseSettings(), spark)
    df = spark.createDataFrame(
        [
            Row(
                disasterNumber=4586,
                state="TX",
                incidentType="Hurricane",
                declarationType="DR",
                region=6,
                ihProgramDeclared=True,
                iaProgramDeclared=True,
                paProgramDeclared=True,
                hmProgramDeclared=True,
                tribalRequest=False,
                declarationDate=date(2020, 8, 20),
                incidentBeginDate=date(2020, 8, 23),
                designatedArea="Harris (County)",
            )
        ]
    )

    results = {result.check_name: result for result in transformer.dq_checks(df)}

    assert not results["declaration_not_before_incident"].passed
    assert results["declaration_not_before_incident"].failed_count == 1
