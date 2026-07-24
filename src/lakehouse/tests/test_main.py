"""Tests for the lakehouse CLI entry point."""

import pytest

from lakehouse.main import build_parser, main


def test_build_parser_accepts_valid_stage() -> None:
    """Parser accepts each of the four defined pipeline stages."""
    parser = build_parser()
    for stage in ["ingest", "silver", "process", "gold"]:
        args = parser.parse_args([stage])
        assert args.stage == stage


def test_build_parser_rejects_invalid_stage() -> None:
    """Parser exits with an error on an unrecognized stage name."""
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["not-a-stage"])


def test_main_returns_zero_for_valid_stage() -> None:
    """main() returns exit code 0 for a valid stage argument."""
    assert main(["ingest"]) == 0
