"""Foreign-key / dimension-resolution checks between fact and dimension tables."""

from typing import Literal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse.silver.data_quality.checks.results import DQCheckResult


def check_referential_integrity(
    fact_df: DataFrame,
    dim_df: DataFrame,
    fact_key: str,
    dim_key: str,
    severity: Literal["error", "warn"] = "error",
    check_name: str | None = None,
) -> DQCheckResult:
    """Flags fact rows whose foreign key has no matching dimension row.

    Mirrors dbt's `relationships` test. Catches a left join that silently
    dropped the match (e.g. a state code that didn't reconcile against
    `dim_geography_state`) instead of surfacing it as a `NULL`/orphaned key.
    A null `fact_key` is treated as a failure -- a fact row should always
    resolve to a real dimension row.

    Args:
        fact_df (DataFrame): Fact table to check.
        dim_df (DataFrame): Dimension table `fact_key` should resolve against.
        fact_key (str): Foreign-key column on `fact_df`.
        dim_key (str): Primary-key column on `dim_df`.
        severity (Literal["error", "warn"]): Severity to attach to the result.
        check_name (str | None): Override for the result's name. Defaults to
            "referential_integrity_{fact_key}"; pass an explicit name when
            checking the same `fact_key`/`dim_key` pair against more than one
            fact table, since the default alone wouldn't disambiguate them.

    Returns:
        DQCheckResult: Result named `check_name`, or
            "referential_integrity_{fact_key}" if not given.
    """
    valid_keys = dim_df.select(F.col(dim_key).alias("_valid_key")).distinct()
    orphaned = fact_df.join(valid_keys, fact_df[fact_key] == valid_keys["_valid_key"], "left_anti")
    failed_count = orphaned.count()
    return DQCheckResult(
        check_name=check_name or f"referential_integrity_{fact_key}",
        passed=failed_count == 0,
        failed_count=failed_count,
        severity=severity,
    )
