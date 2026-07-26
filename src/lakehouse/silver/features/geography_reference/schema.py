"""Declared schema contract for Silver geography_reference_counties."""

from lakehouse.silver.data_quality.schema import ColumnSpec, TableSchema

SCHEMA = TableSchema(
    name="geography_reference_counties",
    description=(
        "Census Gazetteer county reference data, standardized for join "
        "compatibility with FEMA's and NOAA's state columns. One row per county."
    ),
    columns=[
        ColumnSpec(
            name="USPS",
            data_type="string",
            description="2-letter USPS state code, uppercased and trimmed.",
            nullable=False,
        ),
        ColumnSpec(
            name="GEOID",
            data_type="string",
            description="Census county GEOID -- the table's unique key.",
            nullable=False,
            unique=True,
        ),
        ColumnSpec(
            name="NAME",
            data_type="string",
            description="County name.",
        ),
        ColumnSpec(
            name="ALAND",
            data_type="double",
            description="Land area in square meters.",
        ),
        ColumnSpec(
            name="AWATER",
            data_type="double",
            description="Water area in square meters.",
        ),
    ],
)
