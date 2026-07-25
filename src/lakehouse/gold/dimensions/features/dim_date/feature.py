"""Gold dim_date: one row per calendar day over a configured range."""

from datetime import date

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F


def build_dim_date(spark: SparkSession, start_date: date, end_date: date) -> DataFrame:
    """Builds a calendar-date dimension spanning `start_date` to `end_date` inclusive.

    Args:
        spark (SparkSession): Active Spark session.
        start_date (date): First date in the dimension (inclusive).
        end_date (date): Last date in the dimension (inclusive).

    Returns:
        DataFrame: One row per day with `date_key` (int, YYYYMMDD), `date`,
            `year`, `quarter`, `month`, `day`, `day_of_week`, `is_weekend`.
    """
    bounds = spark.createDataFrame([Row(start=start_date, end=end_date)])
    date_values = bounds.select(
        F.explode(F.sequence(F.col("start"), F.col("end"), F.expr("interval 1 day"))).alias("date")
    )
    return date_values.select(
        F.date_format(F.col("date"), "yyyyMMdd").cast("int").alias("date_key"),
        F.col("date"),
        F.year(F.col("date")).alias("year"),
        F.quarter(F.col("date")).alias("quarter"),
        F.month(F.col("date")).alias("month"),
        F.dayofmonth(F.col("date")).alias("day"),
        F.dayofweek(F.col("date")).alias("day_of_week"),
        F.dayofweek(F.col("date")).isin(1, 7).alias("is_weekend"),
    )
