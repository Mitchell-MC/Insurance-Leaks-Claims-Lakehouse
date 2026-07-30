"""Core result model and failure handling shared by every data-quality check."""

from typing import Literal

from pydantic import BaseModel


class DQCheckResult(BaseModel):
    """Outcome of a single data-quality check.

    Attributes:
        check_name (str): Human-readable name of the check.
        passed (bool): Whether the check found zero failing records/columns.
        failed_count (int): Number of records (or unexpected columns) that failed.
        severity (Literal["error", "warn"]): "error" (default) means a failure
            must block promotion of the data; "warn" means it's logged but
            non-blocking. Mirrors dbt's `severity` config.
    """

    check_name: str
    passed: bool
    failed_count: int
    severity: Literal["error", "warn"] = "error"


class DQCheckFailure(Exception):
    """Raised when one or more "error"-severity DQCheckResults failed."""

    def __init__(self, failures: list[DQCheckResult]) -> None:
        """Builds an error message summarizing every failed check.

        Args:
            failures (list[DQCheckResult]): The failed, error-severity results.
        """
        self.failures = failures
        summary = ", ".join(
            f"{result.check_name} ({result.failed_count} failing)" for result in failures
        )
        super().__init__(f"Data-quality checks failed: {summary}")


def raise_on_failures(results: list[DQCheckResult]) -> None:
    """Raises DQCheckFailure if any "error"-severity check in `results` failed.

    "warn"-severity failures are ignored here; callers that want to surface
    them can inspect `results` directly.

    Args:
        results (list[DQCheckResult]): Results to inspect.

    Raises:
        DQCheckFailure: If any error-severity result has `passed=False`.
    """
    blocking_failures = [
        result for result in results if not result.passed and result.severity == "error"
    ]
    if blocking_failures:
        raise DQCheckFailure(blocking_failures)
