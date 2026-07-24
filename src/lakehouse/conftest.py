"""Shared pytest fixtures for the lakehouse test suite."""

from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession


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
