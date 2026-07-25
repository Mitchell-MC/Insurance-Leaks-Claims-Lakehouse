"""Bronze ingestor for National Weather Service active alert snapshots."""

import json
from datetime import timedelta

from pyspark.sql import DataFrame

from lakehouse.ingestion.base_ingestor import BaseIngestor
from lakehouse.silver.data_quality.checks import DQCheckResult, check_freshness

# Reason: this ingestor runs on a schedule (infra/jobs.tf) to approximate a
# near-real-time feed -- if the newest `_ingestion_metadata.loaded_at` is
# older than this, the scheduled job has likely stopped running, not that
# the alerts themselves are stale (NWS `effective`/`expires` are separate,
# per-alert fields this check doesn't inspect).
_MAX_SNAPSHOT_AGE = timedelta(hours=6)


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

    def dq_checks(self, df: DataFrame) -> list[DQCheckResult]:
        """Flags a snapshot whose `_ingestion_metadata.loaded_at` is stale.

        Args:
            df (DataFrame): Fetched alerts, with `_ingestion_metadata` attached.

        Returns:
            list[DQCheckResult]: Result for the freshness check ("warn"
                severity -- a stale snapshot shouldn't block ingestion of
                whatever data was actually fetched, only get flagged).
        """
        return [
            check_freshness(
                df,
                "_ingestion_metadata.loaded_at",
                _MAX_SNAPSHOT_AGE,
                severity="warn",
            )
        ]
