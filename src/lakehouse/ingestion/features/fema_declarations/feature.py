"""Bronze ingestor for OpenFEMA Disaster Declarations Summaries."""

import json

from pyspark.sql import DataFrame

from lakehouse.ingestion.base_ingestor import BaseIngestor

_ENDPOINT = "DisasterDeclarationsSummaries"


class FemaDeclarationsIngestor(BaseIngestor):
    """Ingests FEMA disaster declarations as a historical batch source.

    Paginates the OpenFEMA API and captures each record's native JSON shape
    so Bronze reflects the source schema exactly, without manual column
    mapping.
    """

    bronze_table_name = "fema_declarations"

    def fetch(self) -> DataFrame:
        """Fetches all disaster declaration records via paginated API calls.

        Returns:
            DataFrame: One row per declaration record, columns inferred from
                the source JSON.
        """
        page_size = self._settings.fema_page_size
        records: list[dict[str, object]] = []
        skip = 0
        while True:
            response = self._make_request(
                f"{self._settings.fema_api_base_url}/{_ENDPOINT}",
                params={"$top": page_size, "$skip": skip},
            )
            page = response.json()[_ENDPOINT]
            records.extend(page)
            if len(page) < page_size:
                break
            skip += page_size

        json_lines = [json.dumps(record) for record in records]
        return self._spark.read.json(self._spark.sparkContext.parallelize(json_lines))
