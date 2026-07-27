"""Silver transformer for FEMA disaster declarations."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse.silver.base_transformer import BaseSilverTransformer
from lakehouse.silver.data_quality.checks import DQCheckResult, check_expression
from lakehouse.silver.data_quality.schema import check_against_schema
from lakehouse.silver.features.fema_declarations.schema import SCHEMA


class FemaDeclarationsTransformer(BaseSilverTransformer):
    """Standardizes Bronze FEMA disaster declarations for downstream analytics.

    Normalizes state codes, parses declaration/incident dates, and keeps only
    the columns kpi_definitions.md's KPI-3 (Average Days from Event to
    Declaration) traces to a real source column for.
    """

    silver_table_name = "fema_declarations"

    def transform(self, bronze_df: DataFrame) -> DataFrame:
        """Normalizes state codes and parses declaration/incident dates.

        Args:
            bronze_df (DataFrame): Raw FEMA declarations from Bronze.

        Returns:
            DataFrame: One row per disaster/area with typed date columns.
        """
        current_bronze_df = self.filter_current(bronze_df)
        return current_bronze_df.select(
            F.col("disasterNumber"),
            F.upper(F.trim(F.col("state"))).alias("state"),
            F.col("incidentType"),
            # Reason: OpenFEMA returns ISO 8601 datetimes (e.g.
            # "2020-08-27T00:00:00.000Z"); only the date portion is needed,
            # and slicing it avoids brittle datetime-format parsing.
            F.to_date(F.substring(F.col("declarationDate"), 1, 10)).alias("declarationDate"),
            F.to_date(F.substring(F.col("incidentBeginDate"), 1, 10)).alias("incidentBeginDate"),
            F.col("designatedArea"),
        )

    def dq_checks(self, df: DataFrame) -> list[DQCheckResult]:
        """Validates the declared schema plus the declaration/incident date ordering rule.

        Args:
            df (DataFrame): Transformed FEMA declarations.

        Returns:
            list[DQCheckResult]: Schema-derived checks (columns, types,
                not-null, uniqueness) plus the date-ordering business rule.
        """
        return [
            *check_against_schema(df, SCHEMA),
            check_expression(
                df,
                F.col("declarationDate") >= F.col("incidentBeginDate"),
                "declaration_not_before_incident",
            ),
        ]
