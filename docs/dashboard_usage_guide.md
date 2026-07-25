# Power BI Dashboard Usage Guide

For claims operations, catastrophe response, and customer experience
stakeholders (the personas defined in [charter.md](charter.md)). This guide
describes the dashboard's layout and how to read it; it is the spec a
dashboard builder works from in Power BI Desktop — this project does not
ship a `.pbix` file (a binary format outside what this repo can author),
only the Gold-layer views (`reporting_views.sql`) and this guide.

## Connecting Power BI to the data

1. Power BI Desktop → Get Data → Databricks.
2. Server hostname / HTTP path: from the SQL warehouse provisioned by
   `infra/sql_warehouse.tf` (Databricks workspace → SQL Warehouses → Connection details).
3. Connect to the `gold` schema and import the six views in
   `docs/reporting_views.sql` (`v_elevated_pressure_regions`,
   `v_complaint_trend_acceleration`, `v_catastrophe_pressure_map`,
   `v_activity_trend_over_time`, `v_roi_summary`), plus `gold.dim_geography`
   and `gold.dim_date` for slicers.
4. Use DirectQuery (not Import) so the dashboard reflects the alerts job's
   15-minute Bronze refresh without a separate Power BI refresh schedule —
   see `docs/batch_vs_streaming_memo.md` for why alert freshness matters here.

## Page 1: Regional Risk Overview

- **Cards** (top row), sourced from `v_elevated_pressure_regions`:
  - Count of regions at elevated catastrophe pressure (`claims_surge_risk >= 0.7`)
  - Count of regions with accelerating complaint trend, from
    `v_complaint_trend_acceleration` — **note:** until a structured
    complaint source is connected, this reads zero (see
    `docs/data_limitations.md`); don't present it as live in a demo without
    that caveat.
  - Estimated avoidable backlog exposure, from `v_roi_summary` — see
    `docs/roi_assumptions.md` for exactly how this number is computed.
- **Map**, sourced from `v_catastrophe_pressure_map`: state/county filled
  map colored by `claims_surge_risk`, sized by `total_damage_property_usd`.
  Click-through filters the page to that region.
- **Drill-through page**: matched storm events and declaration history for
  the clicked region (`gold.fact_catastrophe_event` filtered by
  `geography_key`).

## Page 2: Trends Over Time

- Sourced from `v_activity_trend_over_time`: a line chart with
  `avg_claims_surge_risk`, `declaration_count`, and `complaint_count` over
  `date`, one line per selected state (slicer on `dim_geography.state`).
- Read this page as: does complaint volume follow catastrophe pressure with
  a lag, or has it decoupled? A widening gap between the pressure line and
  the complaint line (pressure rising, complaints flat) is the leading-
  indicator signal this whole project exists to surface early.

## Page 3: ROI

- Sourced from `v_roi_summary`, laid out per `docs/roi_assumptions.md`'s
  formula: `avoided_backlog_days * cost_per_adjuster_day_of_delay`, broken
  out by state.
- Always show the assumptions table (baseline days, cost per day) alongside
  the dollar figure — per `roi_assumptions.md`, `cost_per_adjuster_day_of_delay`
  is an illustrative placeholder for this fictitious-client portfolio
  project, not sourced from real financials, and should be labeled as such
  on the page itself.

## Refresh cadence

Matches `docs/batch_vs_streaming_memo.md`: Bronze NWS alerts refresh every
15 minutes, everything else (and therefore every Gold table these views
read) refreshes daily at 06:00 UTC. If a stakeholder asks why a just-issued
alert isn't reflected in `claims_surge_risk` yet, that's the answer — see
that memo's "What this means for `claims_surge_risk` freshness" section for
the planned fix.
