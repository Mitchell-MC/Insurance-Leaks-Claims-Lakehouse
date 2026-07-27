"""Declared schema contract for Silver fema_declarations."""

from lakehouse.silver.data_quality.schema import ColumnSpec, TableSchema

SCHEMA = TableSchema(
    name="fema_declarations",
    description=(
        "FEMA disaster declarations, standardized from OpenFEMA's "
        "DisasterDeclarationsSummaries endpoint. One row per "
        "(disasterNumber, designatedArea)."
    ),
    columns=[
        ColumnSpec(
            name="disasterNumber",
            data_type="long",
            description="FEMA's unique identifier for the disaster declaration.",
            nullable=False,
            unique=True,
        ),
        ColumnSpec(
            name="state",
            data_type="string",
            description="2-letter USPS state/territory code, uppercased and trimmed.",
            nullable=False,
        ),
        ColumnSpec(
            name="incidentType",
            data_type="string",
            description='FEMA\'s incident type (e.g. "Hurricane", "Flood").',
        ),
        ColumnSpec(
            name="declarationDate",
            data_type="date",
            description="Date FEMA issued the disaster declaration.",
            nullable=False,
        ),
        ColumnSpec(
            name="incidentBeginDate",
            data_type="date",
            description="Date the underlying incident began.",
            nullable=False,
        ),
        ColumnSpec(
            name="designatedArea",
            data_type="string",
            description="County/area FEMA designated within the declaration.",
            unique=True,
        ),
    ],
)
