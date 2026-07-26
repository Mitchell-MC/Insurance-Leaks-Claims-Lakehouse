"""Tests for GeographyReferenceTransformer's typing and normalization."""

from pyspark.sql import Row, SparkSession

from lakehouse.config.config import LakehouseSettings
from lakehouse.silver.features.geography_reference.feature import GeographyReferenceTransformer


def test_bronze_table_name() -> None:
    """The transformer targets the geography_reference_counties Silver table."""
    assert GeographyReferenceTransformer.silver_table_name == "geography_reference_counties"


def test_transform_normalizes_usps_and_types_areas(spark: SparkSession) -> None:
    """transform() uppercases USPS and casts land/water area to double."""
    transformer = GeographyReferenceTransformer(LakehouseSettings(), spark)
    bronze_df = spark.createDataFrame(
        [
            Row(
                USPS=" tx ",
                GEOID="48201",
                NAME="Harris County",
                ALAND="4416420000",
                AWATER="309291000",
            )
        ]
    )

    result = transformer.transform(bronze_df).collect()

    assert result[0]["USPS"] == "TX"
    assert result[0]["ALAND"] == 4416420000.0
    assert result[0]["AWATER"] == 309291000.0


def test_dq_checks_flag_duplicate_geoid(spark: SparkSession) -> None:
    """dq_checks() surfaces a duplicate-key failure for repeated GEOID values."""
    transformer = GeographyReferenceTransformer(LakehouseSettings(), spark)
    df = spark.createDataFrame(
        [
            Row(USPS="TX", GEOID="48201", NAME="Harris County", ALAND=1.0, AWATER=1.0),
            Row(USPS="TX", GEOID="48201", NAME="Harris County", ALAND=1.0, AWATER=1.0),
        ]
    )

    results = {result.check_name: result for result in transformer.dq_checks(df)}

    assert not results["schema_unique_key"].passed
    assert results["schema_columns"].passed
