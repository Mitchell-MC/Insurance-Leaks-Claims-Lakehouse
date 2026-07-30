"""Tests for the aggregate silent-drift check."""

from pyspark.sql import Row, SparkSession

from lakehouse.silver.data_quality.checks import check_no_silent_drift


def test_check_no_silent_drift_passes_for_stable_row_count(spark: SparkSession) -> None:
    """check_no_silent_drift passes when row count barely changes."""
    new_df = spark.createDataFrame([Row(state="TX")] * 100)
    previous_df = spark.createDataFrame([Row(state="TX")] * 105)

    results = check_no_silent_drift(new_df, previous_df, numeric_columns=[])

    assert len(results) == 1
    assert results[0].check_name == "no_silent_drift_row_count"
    assert results[0].passed


def test_check_no_silent_drift_flags_row_count_collapse(spark: SparkSession) -> None:
    """check_no_silent_drift flags a row count that dropped far more than the threshold allows.

    Catches the "join fan-out" / "upstream feed went dark" failure mode: a
    change that passes every per-row check (schema, nulls, ranges) yet
    silently starves or multiplies the dataset in aggregate.
    """
    new_df = spark.createDataFrame([Row(state="TX")] * 5)
    previous_df = spark.createDataFrame([Row(state="TX")] * 100)

    results = check_no_silent_drift(new_df, previous_df, numeric_columns=[])

    assert not results[0].passed
    assert results[0].failed_count == 1
    assert results[0].severity == "warn"


def test_check_no_silent_drift_flags_row_count_spike(spark: SparkSession) -> None:
    """check_no_silent_drift flags a row count that grew far more than the threshold allows."""
    new_df = spark.createDataFrame([Row(state="TX")] * 500)
    previous_df = spark.createDataFrame([Row(state="TX")] * 100)

    results = check_no_silent_drift(new_df, previous_df, numeric_columns=[])

    assert not results[0].passed


def test_check_no_silent_drift_checks_numeric_column_sums(spark: SparkSession) -> None:
    """check_no_silent_drift adds a sum-comparison result per requested numeric column.

    Catches a unit-conversion or parsing bug that shifts an aggregate total
    without changing row count (e.g. damage estimates suddenly read as cents
    instead of dollars).
    """
    new_df = spark.createDataFrame([Row(amount=1_000.0), Row(amount=1_000.0)])
    previous_df = spark.createDataFrame([Row(amount=10.0), Row(amount=10.0)])

    results = {
        result.check_name: result
        for result in check_no_silent_drift(new_df, previous_df, numeric_columns=["amount"])
    }

    assert "no_silent_drift_sum_amount" in results
    assert not results["no_silent_drift_sum_amount"].passed


def test_check_no_silent_drift_passes_when_previous_metric_is_zero(spark: SparkSession) -> None:
    """check_no_silent_drift passes a metric with no prior baseline (e.g. the table's first run)."""
    new_df = spark.createDataFrame([Row(amount=1_000.0)])
    previous_df = spark.createDataFrame([Row(amount=0.0)])

    results = {
        result.check_name: result
        for result in check_no_silent_drift(new_df, previous_df, numeric_columns=["amount"])
    }

    assert results["no_silent_drift_sum_amount"].passed


def test_check_no_silent_drift_respects_custom_threshold(spark: SparkSession) -> None:
    """check_no_silent_drift's max_relative_change controls how much movement is tolerated."""
    new_df = spark.createDataFrame([Row(state="TX")] * 130)
    previous_df = spark.createDataFrame([Row(state="TX")] * 100)

    lenient = check_no_silent_drift(
        new_df, previous_df, numeric_columns=[], max_relative_change=0.5
    )
    strict = check_no_silent_drift(new_df, previous_df, numeric_columns=[], max_relative_change=0.1)

    assert lenient[0].passed
    assert not strict[0].passed
