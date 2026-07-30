"""Tests for the shared DQ check result model and failure handling."""

import pytest

from lakehouse.silver.data_quality.checks import DQCheckFailure, DQCheckResult, raise_on_failures


def test_check_result_defaults_to_error_severity() -> None:
    """DQCheckResult defaults to "error" severity when not specified."""
    result = DQCheckResult(check_name="x", passed=True, failed_count=0)

    assert result.severity == "error"


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
