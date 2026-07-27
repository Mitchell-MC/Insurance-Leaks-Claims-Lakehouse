"""Bronze ingestor for OpenFEMA Disaster Declarations Summaries."""

import json
from datetime import UTC, datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse.ingestion.base_ingestor import BaseIngestor
from lakehouse.ingestion.watermark_store import WatermarkRecord

_ENDPOINT = "DisasterDeclarationsSummaries"
_LAST_REFRESH_FIELD = "lastRefresh"
_KEY_FIELD = "id"


class FemaDeclarationsIngestor(BaseIngestor):
    """Ingests FEMA disaster declarations as a historical batch source.

    Paginates the OpenFEMA API and captures each record's native JSON shape
    so Bronze reflects the source schema exactly, without manual column
    mapping. Incremental after the first run: only records with a
    `lastRefresh` newer than the stored watermark are requested.

    Tombstone detection (flagging a record whose `id` disappeared upstream
    via `_is_current = false`) only runs on the first, unfiltered fetch,
    when the response is the source's complete current state. A filtered
    incremental page only contains new/changed records, so it structurally
    cannot reveal a deletion -- see `docs/data_limitations.md` for this
    trade-off.
    """

    bronze_table_name = "fema_declarations"

    def fetch(self) -> DataFrame:
        """Fetches disaster declarations new/changed since the last run.

        Returns:
            DataFrame: One row per declaration record (plus tombstone rows,
                first run only), columns inferred from the source JSON.
        """
        watermark = self._watermark_store.get(self.bronze_table_name)
        records = self._fetch_pages(watermark)

        json_lines = [json.dumps(record) for record in records]
        df = self._spark.read.json(self._spark.sparkContext.parallelize(json_lines))

        if watermark is None:
            result_df = self._detect_tombstones(df, key_column=_KEY_FIELD)
        else:
            result_df = df.withColumn("_is_current", F.lit(True))

        if records:
            self._watermark_store.set(
                WatermarkRecord(
                    source_name=self.bronze_table_name,
                    watermark_value=max(str(record[_LAST_REFRESH_FIELD]) for record in records),
                    updated_at=datetime.now(UTC),
                    extra=self._key_set_json(df, key_column=_KEY_FIELD)
                    if watermark is None
                    else None,
                )
            )
        return result_df

    def _fetch_pages(self, watermark: WatermarkRecord | None) -> list[dict[str, object]]:
        """Paginates the OpenFEMA API, filtering by watermark when one exists.

        Args:
            watermark (WatermarkRecord | None): The stored checkpoint, or
                None on a first/full run.

        Returns:
            list[dict[str, object]]: All matching records across every page.
        """
        page_size = self._settings.fema_page_size
        records: list[dict[str, object]] = []
        skip = 0
        while True:
            params: dict[str, object] = {"$top": page_size, "$skip": skip}
            if watermark is not None:
                params["$filter"] = f"{_LAST_REFRESH_FIELD} gt '{watermark.watermark_value}'"
            response = self._make_request(
                f"{self._settings.fema_api_base_url}/{_ENDPOINT}",
                params=params,
            )
            page = response.json()[_ENDPOINT]
            records.extend(page)
            if len(page) < page_size:
                break
            skip += page_size
        return records
