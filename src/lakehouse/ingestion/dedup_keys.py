"""Shared natural-key expressions, used by both Bronze tombstone detection and Silver dedup."""

from pyspark.sql import Column
from pyspark.sql import functions as F


def noaa_event_key(
    event_id: Column, state: Column, event_type: Column, event_date: Column, cz_name: Column
) -> Column:
    """Derives NOAA's true dedup/identity key: EVENT_ID, or a content hash if blank.

    Args:
        event_id (Column): Raw EVENT_ID column (NOAA's own identifier; blank
            for a rare subset of records).
        state (Column): STATE column (raw or already-normalized).
        event_type (Column): EVENT_TYPE column.
        event_date (Column): Parsed event date column.
        cz_name (Column): CZ_NAME column.

    Returns:
        Column: EVENT_ID when present, else a stable sha2 hash of the other
            fields -- used both by `NoaaStormEventsTransformer` to dedupe
            within a run and by `NoaaStormEventsIngestor` to detect a
            previously-seen event disappearing between runs.

    Note:
        Bronze calls this on raw string columns (e.g. `BEGIN_DATE_TIME` as
        NOAA's native "01-JAN-19 00:00:00" text) since Bronze has no typed
        date yet; Silver calls it after parsing to a real date. Both are
        stable identity keys for the *same* underlying event as long as the
        raw string doesn't change between runs, but the two layers' computed
        hash values are not expected to be byte-identical to each other.
    """
    return F.coalesce(
        event_id,
        F.sha2(
            F.concat_ws("|", state, event_type, event_date.cast("string"), cz_name),
            256,
        ),
    )
