"""Tests for reusable Silver- and Gold-layer data-quality checks."""

from datetime import UTC, date, datetime, timedelta

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

from lakehouse.silver.data_quality.checks import (
    DQCheckFailure,
    DQCheckResult,
    check_accepted_values,
    check_expression,
    check_freshness,
    check_no_duplicate_keys,
    check_no_null_geography,
    check_no_silent_drift,
    check_referential_integrity,
    check_schema_drift,
    check_valid_dates,
    raise_on_failures,
)


def test_check_no_null_geography_passes_when_no_nulls(spark: SparkSession) -> None:
    """check_no_null_geography passes when every geography column is populated."""
    df = spark.createDataFrame([Row(state="TX", county="Harris"), Row(state="FL", county="Dade")])

    result = check_no_null_geography(df, ["state", "county"])

    assert result.passed
    assert result.failed_count == 0


def test_check_no_null_geography_counts_null_rows(spark: SparkSession) -> None:
    """check_no_null_geography counts rows with a null in any listed column."""
    df = spark.createDataFrame([Row(state="TX", county="Harris"), Row(state=None, county="Dade")])

    result = check_no_null_geography(df, ["state", "county"])

    assert not result.passed
    assert result.failed_count == 1


def test_check_valid_dates_counts_null_dates(spark: SparkSession) -> None:
    """check_valid_dates counts rows where a date column failed to parse (is null)."""
    df = spark.createDataFrame([Row(event_date=date(2020, 1, 1)), Row(event_date=None)])

    result = check_valid_dates(df, ["event_date"])

    assert not result.passed
    assert result.failed_count == 1


def test_check_no_duplicate_keys_detects_duplicates(spark: SparkSession) -> None:
    """check_no_duplicate_keys counts rows beyond the distinct key count."""
    df = spark.createDataFrame([Row(id="a", value=1), Row(id="a", value=2), Row(id="b", value=3)])

    result = check_no_duplicate_keys(df, ["id"])

    assert not result.passed
    assert result.failed_count == 1


def test_check_schema_drift_flags_unexpected_columns(spark: SparkSession) -> None:
    """check_schema_drift flags columns outside the expected set."""
    df = spark.createDataFrame([Row(state="TX", unexpected_column="x")])

    result = check_schema_drift(df, expected_columns={"state"})

    assert not result.passed
    assert result.failed_count == 1


def test_check_result_defaults_to_error_severity() -> None:
    """DQCheckResult defaults to "error" severity when not specified."""
    result = DQCheckResult(check_name="x", passed=True, failed_count=0)

    assert result.severity == "error"


def test_check_accepted_values_passes_for_known_values(spark: SparkSession) -> None:
    """check_accepted_values passes when every value is in the allowed set."""
    df = spark.createDataFrame([Row(band="severe"), Row(band="minor")])

    result = check_accepted_values(df, "band", {"severe", "moderate", "minor"})

    assert result.passed
    assert result.failed_count == 0


def test_check_accepted_values_flags_unknown_values(spark: SparkSession) -> None:
    """check_accepted_values counts rows whose value isn't in the allowed set."""
    df = spark.createDataFrame([Row(band="severe"), Row(band="catastrophic")])

    result = check_accepted_values(df, "band", {"severe", "moderate", "minor"})

    assert not result.passed
    assert result.failed_count == 1
    assert result.check_name == "accepted_values_band"


def test_check_accepted_values_ignores_nulls(spark: SparkSession) -> None:
    """check_accepted_values does not flag nulls -- that's a separate not-null check's job."""
    df = spark.createDataFrame([Row(band="severe"), Row(band=None)])

    result = check_accepted_values(df, "band", {"severe"})

    assert result.passed


def test_check_expression_flags_false_rows(spark: SparkSession) -> None:
    """check_expression counts rows where the expression is false."""
    df = spark.createDataFrame([Row(amount=10.0), Row(amount=-5.0)])

    result = check_expression(df, F.col("amount") >= 0, "amount_non_negative")

    assert not result.passed
    assert result.failed_count == 1
    assert result.check_name == "amount_non_negative"


def test_check_expression_flags_null_as_failure(spark: SparkSession) -> None:
    """check_expression treats a null expression result as a failure (SQL "unknown" semantics)."""
    df = spark.createDataFrame([Row(amount=10.0), Row(amount=None)])

    result = check_expression(df, F.col("amount") >= 0, "amount_non_negative")

    assert not result.passed
    assert result.failed_count == 1


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


def test_check_freshness_passes_for_recent_timestamp(spark: SparkSession) -> None:
    """check_freshness passes when the newest timestamp is within max_age."""
    recent = datetime.now(UTC) - timedelta(minutes=5)
    df = spark.createDataFrame([Row(loaded_at=recent)])

    result = check_freshness(df, "loaded_at", timedelta(hours=1))

    assert result.passed
    assert result.failed_count == 0


def test_check_freshness_flags_stale_timestamp(spark: SparkSession) -> None:
    """check_freshness fails when the newest timestamp is older than max_age."""
    stale = datetime.now(UTC) - timedelta(hours=2)
    df = spark.createDataFrame([Row(loaded_at=stale)])

    result = check_freshness(df, "loaded_at", timedelta(hours=1))

    assert not result.passed
    assert result.failed_count == 1
    assert result.check_name == "freshness_loaded_at"


def test_check_freshness_flags_empty_dataframe_as_stale(spark: SparkSession) -> None:
    """check_freshness fails when there are no rows to derive a max timestamp from."""
    df = spark.createDataFrame([], schema="loaded_at timestamp")

    result = check_freshness(df, "loaded_at", timedelta(hours=1))

    assert not result.passed
    assert result.failed_count == 1


def test_check_freshness_defaults_to_warn_severity(spark: SparkSession) -> None:
    """check_freshness defaults to "warn" severity, unlike the other checks."""
    df = spark.createDataFrame([Row(loaded_at=datetime.now(UTC))])

    result = check_freshness(df, "loaded_at", timedelta(hours=1))

    assert result.severity == "warn"


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
    strict = check_no_silent_drift(
        new_df, previous_df, numeric_columns=[], max_relative_change=0.1
    )

    assert lenient[0].passed
    assert not strict[0].passed


def test_raise_on_failures_raises_for_error_severity_failure() -> None:
    """raise_on_failures raises DQCheckFailure when an error-severity check failed."""
    results = [DQCheckResult(check_name="x", passed=False, failed_count=1, severity="error")]

    with pytest.raises(DQCheckFailure, match="x"):
        raise_on_failures(results)


def test_raise_on_failures_ignores_warn_severity_failure() -> None:
    """raise_on_failures does not raise when only a warn-severity check failed."""
    results = [DQCheckResult(check_name="x", passed=False, failed_count=1, severity="warn")]

    raise_on_failures(results)


def test_raise_on_failures_does_not_raise_when_all_passed() -> None:
    """raise_on_failures does not raise when every check passed."""
    results = [DQCheckResult(check_name="x", passed=True, failed_count=0, severity="error")]

    raise_on_failures(results)
