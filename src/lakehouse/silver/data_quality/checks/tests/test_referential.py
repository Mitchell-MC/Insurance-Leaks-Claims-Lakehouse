"""Tests for the fact-to-dimension referential-integrity check."""

from pyspark.sql import Row, SparkSession

from lakehouse.silver.data_quality.checks import check_referential_integrity


def test_check_referential_integrity_passes_when_all_keys_resolve(spark: SparkSession) -> None:
    """check_referential_integrity passes when every fact key exists in the dimension."""
    fact_df = spark.createDataFrame([Row(state_key=1), Row(state_key=2)])
    dim_df = spark.createDataFrame([Row(key=1), Row(key=2), Row(key=3)])

    result = check_referential_integrity(fact_df, dim_df, "state_key", "key")

    assert result.passed
    assert result.failed_count == 0


def test_check_referential_integrity_flags_orphaned_keys(spark: SparkSession) -> None:
    """check_referential_integrity counts fact rows whose key has no matching dimension row."""
    fact_df = spark.createDataFrame([Row(state_key=1), Row(state_key=99)])
    dim_df = spark.createDataFrame([Row(key=1), Row(key=2)])

    result = check_referential_integrity(fact_df, dim_df, "state_key", "key")

    assert not result.passed
    assert result.failed_count == 1
    assert result.check_name == "referential_integrity_state_key"


def test_check_referential_integrity_accepts_check_name_override(spark: SparkSession) -> None:
    """check_referential_integrity uses the given check_name instead of the default.

    Needed when the same fact_key/dim_key pair is checked against more than
    one fact table -- the default name alone wouldn't disambiguate them.
    """
    fact_df = spark.createDataFrame([Row(state_key=1)])
    dim_df = spark.createDataFrame([Row(key=1)])

    result = check_referential_integrity(
        fact_df, dim_df, "state_key", "key", check_name="my_fact_referential_integrity"
    )

    assert result.check_name == "my_fact_referential_integrity"


def test_check_referential_integrity_flags_null_fact_key(spark: SparkSession) -> None:
    """check_referential_integrity treats a null foreign key as unresolved, not exempt."""
    fact_df = spark.createDataFrame([Row(state_key=1), Row(state_key=None)])
    dim_df = spark.createDataFrame([Row(key=1)])

    result = check_referential_integrity(fact_df, dim_df, "state_key", "key")

    assert not result.passed
    assert result.failed_count == 1
