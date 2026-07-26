"""Declared schema contract for Silver noaa_storm_events."""

from lakehouse.silver.data_quality.schema import ColumnSpec, TableSchema

SCHEMA = TableSchema(
    name="noaa_storm_events",
    description=(
        "NOAA Storm Events, standardized from the bulk CSV source and "
        "deduplicated on EVENT_ID (falling back to a content hash for "
        "blank-EVENT_ID records -- see feature.py). One row per distinct "
        "storm event."
    ),
    columns=[
        ColumnSpec(
            name="EVENT_ID",
            data_type="string",
            description=(
                "NOAA's event identifier. Not guaranteed unique on its own: "
                "the rare blank-EVENT_ID record is deduplicated against a "
                "content hash instead (see feature.py's _DEDUP_KEY)."
            ),
        ),
        ColumnSpec(
            name="STATE",
            data_type="string",
            description="2-letter USPS state code, mapped from NOAA's full state name.",
            nullable=False,
        ),
        ColumnSpec(
            name="CZ_NAME",
            data_type="string",
            description="NOAA's county/zone name for the event.",
        ),
        ColumnSpec(
            name="EVENT_TYPE",
            data_type="string",
            description="NOAA's event type (e.g. \"Tornado\", \"Flash Flood\").",
        ),
        ColumnSpec(
            name="EVENT_DATE",
            data_type="date",
            description="Parsed event begin date.",
            nullable=False,
        ),
        ColumnSpec(
            name="DAMAGE_PROPERTY_USD",
            data_type="double",
            description="Parsed property damage estimate in USD (0.0 for blank/null source values).",
            nullable=False,
            minimum=0.0,
        ),
        ColumnSpec(
            name="MAGNITUDE",
            data_type="string",
            description="NOAA's raw magnitude field (units vary by event type; left unparsed).",
        ),
        ColumnSpec(
            name="DATA_YEAR",
            data_type="string",
            description="Source file year, attached during Bronze ingestion.",
        ),
        ColumnSpec(
            name="SEVERITY_BAND",
            data_type="string",
            description="Derived severe/moderate/minor band (see feature.py's _severity_band).",
            nullable=False,
        ),
    ],
)
