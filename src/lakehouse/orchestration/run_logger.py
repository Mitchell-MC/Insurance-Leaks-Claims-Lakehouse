"""Structured run-status logging for orchestrated pipeline stages."""

import json
import sys
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime


@contextmanager
def log_stage_run(stage_name: str) -> Iterator[None]:
    """Logs a structured start/success/failure record for one pipeline stage run.

    Emits JSON lines to stdout. Databricks Workflows captures task stdout, so
    this gives queryable run history without a separate observability system;
    `infra/jobs.tf`'s email/webhook notifications handle alerting on failure
    independently of this.

    Args:
        stage_name (str): Name of the pipeline stage being run (e.g. "ingest.fema_declarations").

    Yields:
        None
    """
    run_id = str(uuid.uuid4())
    _emit(stage_name, run_id, "started")
    start_perf = time.perf_counter()
    try:
        yield
    except Exception as exc:
        _emit(
            stage_name,
            run_id,
            "failed",
            error=str(exc),
            duration_seconds=time.perf_counter() - start_perf,
        )
        raise
    else:
        _emit(stage_name, run_id, "succeeded", duration_seconds=time.perf_counter() - start_perf)


def _emit(stage_name: str, run_id: str, status: str, **extra: object) -> None:
    """Writes one JSON-line run-status record to stdout.

    Args:
        stage_name (str): Name of the pipeline stage.
        run_id (str): Unique identifier shared across a run's start/end records.
        status (str): One of "started", "succeeded", "failed".
        **extra (object): Additional fields to include (e.g. `duration_seconds`).
    """
    record = {
        "stage": stage_name,
        "run_id": run_id,
        "status": status,
        "at": datetime.now(UTC).isoformat(),
        **extra,
    }
    print(json.dumps(record), file=sys.stdout)
