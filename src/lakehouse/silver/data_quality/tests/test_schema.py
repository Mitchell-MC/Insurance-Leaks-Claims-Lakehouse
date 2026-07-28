"""Tests for TableSchema/ColumnSpec's declarative contract and check_against_schema."""

from pyspark.sql import Row, SparkSession

from lakehouse.silver.data_quality.schema import ColumnSpec, TableSchema, check_against_schema

_SCHEMA = TableSchema(
    name="widgets",
    description="Test fixture table.",
    columns=[
        ColumnSpec(
            name="band",
            data_type="string",
            description="A closed-vocabulary category.",
            nullable=False,
            allowed_values={"low", "medium", "high"},
        ),
    ],
)


def test_check_against_schema_passes_for_known_allowed_value(spark: SparkSession) -> None:
    """check_against_schema passes when every value is in the column's allowed_values."""
    df = spark.createDataFrame([Row(band="low"), Row(band="high")])

    results = {result.check_name: result for result in check_against_schema(df, _SCHEMA)}

    assert results["accepted_values_band"].passed


def test_check_against_schema_flags_unknown_allowed_value(spark: SparkSession) -> None:
    """check_against_schema flags a value outside the column's declared allowed_values."""
    df = spark.createDataFrame([Row(band="low"), Row(band="extreme")])

    results = {result.check_name: result for result in check_against_schema(df, _SCHEMA)}

    assert not results["accepted_values_band"].passed
    assert results["accepted_values_band"].failed_count == 1


def test_check_against_schema_skips_accepted_values_when_unset(spark: SparkSession) -> None:
    """check_against_schema omits the accepted_values check for columns without allowed_values."""
    schema = TableSchema(
        name="widgets",
        description="Test fixture table.",
        columns=[ColumnSpec(name="band", data_type="string", description="Unconstrained.")],
    )
    df = spark.createDataFrame([Row(band="anything")])

    results = {result.check_name: result for result in check_against_schema(df, schema)}

    assert "accepted_values_band" not in results
