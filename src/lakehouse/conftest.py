"""Shared pytest fixtures for the lakehouse test suite."""

import os
import sys
from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

# Reason: some dev machines have SPARK_HOME/PYSPARK_PYTHON pointed at an
# unrelated external Spark install or a stale interpreter path (often stuck
# in an already-running terminal host's cached environment, immune to
# fixing the registry). Force pyspark to use its own pip-installed jars and
# this venv's interpreter so tests are correct regardless of shell state.
os.environ.pop("SPARK_HOME", None)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """Provides a local single-node SparkSession shared across the test session.

    Yields:
        SparkSession: Local Spark session configured for fast unit-test startup.
    """
    session = (
        SparkSession.builder.master("local[1]")
        .appName("lakehouse-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
