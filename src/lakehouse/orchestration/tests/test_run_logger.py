"""Tests for log_stage_run's structured start/success/failure records."""

import json

import pytest

from lakehouse.orchestration.run_logger import log_stage_run


def test_log_stage_run_emits_started_then_succeeded(capsys: pytest.CaptureFixture[str]) -> None:
    """A normal run emits a started record followed by a succeeded record."""
    with log_stage_run("ingest.fema_declarations"):
        pass

    lines = [json.loads(line) for line in capsys.readouterr().out.strip().splitlines()]
    assert [record["status"] for record in lines] == ["started", "succeeded"]
    assert lines[0]["run_id"] == lines[1]["run_id"]
    assert all(record["stage"] == "ingest.fema_declarations" for record in lines)


def test_log_stage_run_emits_failed_and_reraises(capsys: pytest.CaptureFixture[str]) -> None:
    """A raising run emits a failed record with the error, then re-raises."""
    with pytest.raises(ValueError, match="boom"):
        with log_stage_run("silver.noaa_storm_events"):
            raise ValueError("boom")

    lines = [json.loads(line) for line in capsys.readouterr().out.strip().splitlines()]
    assert [record["status"] for record in lines] == ["started", "failed"]
    assert lines[1]["error"] == "boom"
