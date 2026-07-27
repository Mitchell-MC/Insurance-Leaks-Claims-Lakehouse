"""Build-time only: warms the Ivy cache for Delta's jars, and appends the
resolved spark.jars.packages coordinate to spark-defaults.conf.

Run once during `docker build` so the resulting image layer bakes in the
resolved jars -- no container run ever needs network access to Maven
Central. Reuses the exact configure_spark_with_delta_pip mechanism already
proven in src/lakehouse/conftest.py, just invoked once at build time
instead of every test session, and additionally derives the exact Delta
artifact coordinate (matching whatever delta-spark version is actually
installed) instead of hardcoding it into spark-defaults.conf, where it
would silently drift out of sync with pyproject.toml/uv.lock.

Not part of the lakehouse package or wheel; deleted from the image after
running (the populated ~/.ivy2 cache and the appended conf line are kept).
"""

import importlib_metadata
import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

_SPARK_DEFAULTS_CONF = "/opt/spark-conf/spark-defaults.conf"

delta_version = importlib_metadata.version("delta_spark")
spark_major_minor = ".".join(pyspark.__version__.split(".")[:2])
delta_coordinate = f"io.delta:delta-spark_{spark_major_minor}_2.13:{delta_version}"

builder = SparkSession.builder.master("local[1]").appName("warm-delta-cache")
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.stop()

with open(_SPARK_DEFAULTS_CONF, "a") as f:
    f.write(f"spark.jars.packages\t{delta_coordinate}\n")
