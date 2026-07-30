"""Reusable data-quality checks for Silver- and Gold-layer transformers.

Split by concern: `results` (the shared result model/failure handling),
`structural` (per-row null/date/duplicate/schema/value/expression checks),
`referential` (fact-to-dimension key resolution), `drift` (aggregate
row-count/sum swings), and `freshness` (stale-source detection). Re-exported
here so existing `from lakehouse.silver.data_quality.checks import ...`
call sites don't need to know which submodule a check lives in.
"""

from lakehouse.silver.data_quality.checks.drift import check_no_silent_drift
from lakehouse.silver.data_quality.checks.freshness import check_freshness
from lakehouse.silver.data_quality.checks.referential import check_referential_integrity
from lakehouse.silver.data_quality.checks.results import (
    DQCheckFailure,
    DQCheckResult,
    raise_on_failures,
)
from lakehouse.silver.data_quality.checks.structural import (
    check_accepted_values,
    check_expression,
    check_no_duplicate_keys,
    check_no_null_geography,
    check_schema_drift,
    check_valid_dates,
)

__all__ = [
    "DQCheckFailure",
    "DQCheckResult",
    "check_accepted_values",
    "check_expression",
    "check_freshness",
    "check_no_duplicate_keys",
    "check_no_null_geography",
    "check_no_silent_drift",
    "check_referential_integrity",
    "check_schema_drift",
    "check_valid_dates",
    "raise_on_failures",
]
