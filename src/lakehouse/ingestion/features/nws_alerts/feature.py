"""Bronze ingestor for National Weather Service active alert snapshots."""

import json

from pyspark.sql import DataFrame

from lakehouse.ingestion.base_ingestor import BaseIngestor


class NwsAlertsIngestor(BaseIngestor):
    """Ingests a snapshot of active NWS alerts to simulate near-real-time feeds.

    Each call captures the alerts active at that moment; scheduling repeated
    snapshots (Phase 6) is what approximates near-real-time ingestion for
    this batch-oriented source.
    """

    bronze_table_name = "nws_alerts_snapshots"

    def fetch(self) -> DataFrame:
        """Fetches the current active-alerts GeoJSON feed.

        Returns:
            DataFrame: One row per active alert, with alert properties and
                geometry columns inferred from the source JSON.
        """
        response = self._make_request(
            f"{self._settings.nws_api_base_url}/alerts/active",
            headers={
                "User-Agent": self._settings.nws_user_agent,
                "Accept": "application/geo+json",
            },
        )
        features = response.json()["features"]
        json_lines = [
            json.dumps({**feature["properties"], "geometry": feature.get("geometry")})
            for feature in features
        ]
        return self._spark.read.json(self._spark.sparkContext.parallelize(json_lines))
