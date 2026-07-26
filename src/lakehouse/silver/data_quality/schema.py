"""Declared table/column schemas -- the single source of truth per table.

Mirrors the schema-file idea from Stint's data-pipeline testing write-up:
every table declares its columns, types, and constraints once; that
declaration drives both the checks run against it and its documentation,
instead of a set of column names and ad hoc dq_checks() calls duplicating
the same knowledge in two places.
"""

from pydantic import BaseModel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DataType

from lakehouse.silver.data_quality.checks import DQCheckResult, check_expression


class ColumnSpec(BaseModel):
    """Declares one column's type and constraints.

    Attributes:
        name (str): Column name.
        data_type (str): Expected Spark SQL type name (e.g. "string", "int",
            "double", "date", "timestamp"), compared against
            `DataFrame.schema[name].dataType.typeName()`.
        description (str): What the column holds and where it comes from.
        nullable (bool): Whether null values are allowed. Defaults to True.
        unique (bool): Whether values must be unique across the table
            (checked jointly with any other `unique=True` columns as a
            composite key). Defaults to False.
        minimum (float | None): Minimum allowed value, inclusive, for
            numeric columns. None means unconstrained.
        maximum (float | None): Maximum allowed value, inclusive, for
            numeric columns. None means unconstrained.
    """

    name: str
    data_type: str
    description: str
    nullable: bool = True
    unique: bool = False
    minimum: float | None = None
    maximum: float | None = None


class TableSchema(BaseModel):
    """Declares a table's full column contract.

    Attributes:
        name (str): Unqualified table name (matches `silver_table_name`/
            `bronze_table_name`/the Gold table name).
        description (str): What the table represents.
        columns (list[ColumnSpec]): Every column the table must have.
    """

    name: str
    description: str
    columns: list[ColumnSpec]

    @property
    def column_names(self) -> set[str]:
        """set[str]: Every declared column name."""
        return {column.name for column in self.columns}

    @property
    def unique_key_columns(self) -> list[str]:
        """list[str]: Names of columns marked `unique=True`, in declaration order."""
        return [column.name for column in self.columns if column.unique]


def check_against_schema(df: DataFrame, schema: TableSchema) -> list[DQCheckResult]:
    """Runs every check implied by a TableSchema's column declarations.

    Args:
        df (DataFrame): DataFrame to validate against `schema`.
        schema (TableSchema): The declared contract for this table.

    Returns:
        list[DQCheckResult]: One "schema_columns" result (missing or
            unexpected columns), one "schema_type_{column}" result per
            declared column present in `df`, one "schema_not_null_{column}"
            per non-nullable column, one "schema_unique_key" result if any
            column is marked unique, and one "schema_range_{column}" result
            per column with a minimum/maximum.
    """
    results = [_check_columns_match(df, schema)]
    for column in schema.columns:
        if column.name not in df.columns:
            continue
        results.append(_check_column_type(df, column))
        if not column.nullable:
            results.append(_check_column_not_null(df, column.name))
        if column.minimum is not None or column.maximum is not None:
            results.append(_check_column_range(df, column))
    if schema.unique_key_columns:
        results.append(_check_unique_key(df, schema.unique_key_columns))
    return results


def _check_columns_match(df: DataFrame, schema: TableSchema) -> DQCheckResult:
    """Flags any mismatch between `df`'s columns and `schema`'s declared columns."""
    actual = set(df.columns)
    expected = schema.column_names
    mismatched = actual.symmetric_difference(expected)
    return DQCheckResult(
        check_name="schema_columns",
        passed=len(mismatched) == 0,
        failed_count=len(mismatched),
    )


def _check_column_type(df: DataFrame, column: ColumnSpec) -> DQCheckResult:
    """Flags a declared column whose actual Spark type doesn't match `column.data_type`."""
    actual_type: DataType = df.schema[column.name].dataType
    matches = actual_type.typeName() == column.data_type
    return DQCheckResult(
        check_name=f"schema_type_{column.name}",
        passed=matches,
        failed_count=0 if matches else 1,
    )


def _check_column_not_null(df: DataFrame, column_name: str) -> DQCheckResult:
    """Flags rows where a non-nullable declared column is null."""
    failed_count = df.filter(F.col(column_name).isNull()).count()
    return DQCheckResult(
        check_name=f"schema_not_null_{column_name}",
        passed=failed_count == 0,
        failed_count=failed_count,
    )


def _check_column_range(df: DataFrame, column: ColumnSpec) -> DQCheckResult:
    """Flags rows where a declared column falls outside its min/max range."""
    in_range = F.lit(True)
    if column.minimum is not None:
        in_range = in_range & (F.col(column.name) >= column.minimum)
    if column.maximum is not None:
        in_range = in_range & (F.col(column.name) <= column.maximum)
    return check_expression(
        df.filter(F.col(column.name).isNotNull()),
        in_range,
        check_name=f"schema_range_{column.name}",
    )


def _check_unique_key(df: DataFrame, key_columns: list[str]) -> DQCheckResult:
    """Flags duplicate rows across the declared unique-key column(s)."""
    total = df.count()
    distinct = df.select(*key_columns).distinct().count()
    failed_count = total - distinct
    return DQCheckResult(
        check_name="schema_unique_key",
        passed=failed_count == 0,
        failed_count=failed_count,
    )
