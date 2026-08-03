"""Tests for the lakehouse CLI entry point."""

from unittest.mock import Mock, patch

import pytest

from lakehouse.main import _run_ingest, build_parser, main


def test_build_parser_accepts_valid_stage() -> None:
    """Parser accepts each of the five defined pipeline stages."""
    parser = build_parser()
    for stage in ["ingest", "silver", "process", "gold", "export"]:
        args = parser.parse_args([stage])
        assert args.stage == stage


def test_build_parser_rejects_invalid_stage() -> None:
    """Parser exits with an error on an unrecognized stage name."""
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["not-a-stage"])


def test_build_parser_defaults_source_to_all() -> None:
    """The --source flag defaults to 'all' when omitted."""
    parser = build_parser()
    args = parser.parse_args(["ingest"])
    assert args.source == "all"


def test_main_dispatches_ingest_with_the_requested_source() -> None:
    """main() builds a SparkSession and calls _run_ingest with the requested --source."""
    with (
        patch("lakehouse.main.SparkSession") as mock_spark_session,
        patch("lakehouse.main._run_ingest") as mock_run_ingest,
    ):
        mock_spark_session.builder.appName.return_value.getOrCreate.return_value = Mock()

        exit_code = main(["ingest", "--source", "nws_alerts_snapshots"])

        assert exit_code == 0
        mock_run_ingest.assert_called_once()
        assert mock_run_ingest.call_args[0][2] == "nws_alerts_snapshots"


def _make_ingestor(table_name: str) -> Mock:
    return Mock(bronze_table_name=table_name)


def test_run_ingest_filters_to_historical_sources() -> None:
    """source='historical' excludes the nws_alerts_snapshots ingestor."""
    ingestors = [
        _make_ingestor("fema_declarations"),
        _make_ingestor("nws_alerts_snapshots"),
    ]
    with (
        patch("lakehouse.main.build_ingestors", return_value=ingestors),
        patch("lakehouse.main.run_ingest_stage") as mock_run_ingest_stage,
    ):
        _run_ingest(Mock(), Mock(), source="historical")

    run_names = {i.bronze_table_name for i in mock_run_ingest_stage.call_args[0][0]}
    assert run_names == {"fema_declarations"}


def test_run_ingest_filters_to_a_single_named_source() -> None:
    """A specific source name runs only that one ingestor."""
    ingestors = [
        _make_ingestor("fema_declarations"),
        _make_ingestor("nws_alerts_snapshots"),
    ]
    with (
        patch("lakehouse.main.build_ingestors", return_value=ingestors),
        patch("lakehouse.main.run_ingest_stage") as mock_run_ingest_stage,
    ):
        _run_ingest(Mock(), Mock(), source="nws_alerts_snapshots")

    run_names = {i.bronze_table_name for i in mock_run_ingest_stage.call_args[0][0]}
    assert run_names == {"nws_alerts_snapshots"}


def test_main_dispatches_non_ingest_stages_via_stage_runners() -> None:
    """main() looks up non-ingest stages in _STAGE_RUNNERS and calls them."""
    with (
        patch("lakehouse.main.SparkSession") as mock_spark_session,
        patch.dict("lakehouse.main._STAGE_RUNNERS", {"silver": Mock()}) as stage_runners,
    ):
        mock_spark_session.builder.appName.return_value.getOrCreate.return_value = Mock()

        exit_code = main(["silver"])

        assert exit_code == 0
        stage_runners["silver"].assert_called_once()
