"""Shared pytest fixtures for the lakehouse test suite."""

import os
import sys
from collections.abc import Iterator

import pytest
from delta import configure_spark_with_delta_pip
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
    """Provides a local single-node, Delta-enabled SparkSession shared across the test session.

    Reason: BaseSilverTransformer.run() reads/writes real Delta tables (to
    check for silent drift against the table's current contents), so the
    test session needs the same Delta catalog/extensions configuration
    production code relies on -- not just plain pyspark.

    Yields:
        SparkSession: Local Spark session configured for fast unit-test startup.
    """
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("lakehouse-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()
