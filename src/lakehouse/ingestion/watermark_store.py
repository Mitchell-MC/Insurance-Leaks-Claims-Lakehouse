"""Delta-backed watermark state store for incremental Bronze ingestion."""

from datetime import datetime

from delta.tables import DeltaTable
from pydantic import BaseModel
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from lakehouse.config.config import LakehouseSettings

DEFAULT_WATERMARK_KEY = "_default"

_SCHEMA = StructType(
    [
        StructField("source_name", StringType(), nullable=False),
        StructField("watermark_key", StringType(), nullable=False),
        StructField("watermark_value", StringType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False),
        StructField("extra", StringType(), nullable=True),
    ]
)


class WatermarkRecord(BaseModel):
    """One source's (or one source-partition's) incremental-fetch checkpoint.

    Attributes:
        source_name (str): Ingestor's `bronze_table_name` (e.g. "fema_declarations").
        watermark_key (str): Sub-key within the source. `DEFAULT_WATERMARK_KEY`
            for sources with a single scalar watermark (FEMA, geography, NWS);
            a year string (e.g. "2024") for NOAA's per-year watermarks.
        watermark_value (str): Opaque string value -- an ISO timestamp for
            FEMA's `lastRefresh`, an 8-digit creation-date for NOAA, or the
            configured gazetteer year for geography. Stored as a string so one
            table schema covers every source's native watermark type.
        updated_at (datetime): UTC timestamp this row was last written.
        extra (str | None): Optional free-form JSON string for source-specific
            bookkeeping (e.g. a previous run's key-set, for tombstone
            detection). None if unused.
    """

    source_name: str
    watermark_key: str = DEFAULT_WATERMARK_KEY
    watermark_value: str
    updated_at: datetime
    extra: str | None = None


class WatermarkStore:
    """Reads and writes WatermarkRecords in the bronze `_ingestion_state` Delta table.

    Bronze tables elsewhere in this codebase are append-only (see
    `BaseIngestor.write_bronze`); this table is the one deliberate exception,
    since watermark state is inherently a "current value per key" store, not
    a history. Backed by Delta `MERGE` (upsert) rather than append, the same
    category of exception `BaseSilverTransformer.write_silver` already makes
    for the same reason.
    """

    def __init__(self, settings: LakehouseSettings, spark: SparkSession) -> None:
        """Initializes the store with typed settings and a Spark session.

        Args:
            settings (LakehouseSettings): Provides the storage path this
                store's table lives under.
            spark (SparkSession): Active Spark session used to read/write Delta.

        Note:
            Performs no I/O -- the underlying Delta table is created lazily on
            the first `set()`/`set_many()` call, so constructing a
            `WatermarkStore` is always safe even before any watermark exists.
        """
        self._settings = settings
        self._spark = spark
        self._path = settings.bronze_table_path(settings.ingestion_state_table_name)

    def get(
        self, source_name: str, watermark_key: str = DEFAULT_WATERMARK_KEY
    ) -> WatermarkRecord | None:
        """Returns the current watermark row, or None if never set.

        Args:
            source_name (str): Ingestor's `bronze_table_name`.
            watermark_key (str): Sub-key within the source. Defaults to the
                single-scalar-watermark key.

        Returns:
            WatermarkRecord | None: The stored record, or None on a first run
                (or if the state table doesn't exist yet).
        """
        return self.get_all(source_name).get(watermark_key)

    def get_all(self, source_name: str) -> dict[str, WatermarkRecord]:
        """Returns every watermark row for a source, keyed by `watermark_key`.

        Args:
            source_name (str): Ingestor's `bronze_table_name`.

        Returns:
            dict[str, WatermarkRecord]: Empty if the state table doesn't exist
                yet or no rows exist for this source (both are first-run cases).
        """
        if not DeltaTable.isDeltaTable(self._spark, self._path):
            return {}
        rows = (
            self._spark.read.format("delta")
            .load(self._path)
            .filter(F.col("source_name") == source_name)
            .collect()
        )
        return {row["watermark_key"]: WatermarkRecord(**row.asDict()) for row in rows}

    def set(self, record: WatermarkRecord) -> None:
        """Upserts one watermark row, keyed on `(source_name, watermark_key)`.

        Args:
            record (WatermarkRecord): The watermark state to persist.
        """
        self.set_many([record])

    def set_many(self, records: list[WatermarkRecord]) -> None:
        """Upserts multiple watermark rows in a single Delta MERGE.

        Args:
            records (list[WatermarkRecord]): Watermark states to persist, e.g.
                NOAA's per-year batch after one ingestion run. No-op if empty.
        """
        if not records:
            return

        updates_df = self._spark.createDataFrame(
            [
                Row(
                    source_name=r.source_name,
                    watermark_key=r.watermark_key,
                    watermark_value=r.watermark_value,
                    updated_at=r.updated_at,
                    extra=r.extra,
                )
                for r in records
            ],
            schema=_SCHEMA,
        )

        if not DeltaTable.isDeltaTable(self._spark, self._path):
            updates_df.write.format("delta").save(self._path)
            return

        target = DeltaTable.forPath(self._spark, self._path)
        (
            target.alias("t")
            .merge(
                updates_df.alias("s"),
                "t.source_name = s.source_name AND t.watermark_key = s.watermark_key",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
