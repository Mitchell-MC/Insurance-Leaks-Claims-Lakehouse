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
            description='NOAA\'s event type (e.g. "Tornado", "Flash Flood").',
        ),
        ColumnSpec(
            name="CZ_TYPE",
            data_type="string",
            description=(
                "Whether the event was recorded against a (C)ounty/parish, "
                "(Z)one, or (M)arine area."
            ),
            allowed_values={"C", "Z", "M"},
        ),
        ColumnSpec(
            name="MAGNITUDE_TYPE",
            data_type="string",
            description=(
                "How MAGNITUDE was derived: EG (estimated gust), ES (estimated "
                "sustained), MS (measured sustained), MG (measured gust). Null for "
                "hail events, which carry no magnitude type."
            ),
            allowed_values={"EG", "ES", "MS", "MG"},
        ),
        ColumnSpec(
            name="TOR_F_SCALE",
            data_type="string",
            description=(
                "Tornado intensity rating: legacy Fujita scale (F0-F5, used through "
                "January 2007), Enhanced Fujita scale (EF0-EF5, used from February "
                "2007 onward), or EFU ('EF-Unknown', in official use since 2016 for "
                "a tornado with no assignable damage rating -- e.g. an inaccessible "
                "track or no surveyable damage). Null for non-tornado events."
            ),
            allowed_values={
                "F0",
                "F1",
                "F2",
                "F3",
                "F4",
                "F5",
                "EF0",
                "EF1",
                "EF2",
                "EF3",
                "EF4",
                "EF5",
                "EFU",
            },
        ),
        ColumnSpec(
            name="FLOOD_CAUSE",
            data_type="string",
            description=(
                "NOAA's reported/estimated cause of a flood event (e.g. \"Heavy "
                'Rain", "Dam/Levee Break"). Not a documented closed vocabulary, '
                "so left unconstrained."
            ),
        ),
        ColumnSpec(
            name="BEGIN_AZIMUTH",
            data_type="string",
            description="16-point compass direction from the event's begin reference location.",
            allowed_values={
                "N",
                "NNE",
                "NE",
                "ENE",
                "E",
                "ESE",
                "SE",
                "SSE",
                "S",
                "SSW",
                "SW",
                "WSW",
                "W",
                "WNW",
                "NW",
                "NNW",
            },
        ),
        ColumnSpec(
            name="END_AZIMUTH",
            data_type="string",
            description="16-point compass direction from the event's end reference location.",
            allowed_values={
                "N",
                "NNE",
                "NE",
                "ENE",
                "E",
                "ESE",
                "SE",
                "SSE",
                "S",
                "SSW",
                "SW",
                "WSW",
                "W",
                "WNW",
                "NW",
                "NNW",
            },
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
            description="Parsed property damage estimate in USD (0.0 for blank/null source rows).",
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
            allowed_values={"severe", "moderate", "minor"},
        ),
    ],
)
