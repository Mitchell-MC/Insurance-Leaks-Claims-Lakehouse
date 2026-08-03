# Power BI Dashboard Usage Guide

For claims operations, catastrophe response, and customer experience
stakeholders (the personas defined in [charter.md](charter.md)). This guide
describes the dashboard's layout and how to read it; it is the spec a
dashboard builder works from in Power BI Desktop — this project does not
ship a `.pbix` file (a binary format outside what this repo can author),
only the Gold-layer views (`reporting_views.sql`) and this guide.

There are two ways to get this into Power BI Desktop, and they trade off
very differently:

| | Local export | Production Azure |
|---|---|---|
| Cost | Free | Real Azure billing (serverless SQL warehouse) |
| Setup | `make run-pipeline` (already Dockerized) | `terraform apply` against a real subscription |
| Refresh | Manual: rerun pipeline, click Refresh in Power BI | Automatic: DirectQuery reflects each Gold run |
| What it proves | The pipeline logic and Gold output are correct | The actual production serving path works end-to-end |

## Local Power BI Desktop (Docker Compose export)

The fastest way to see real numbers in Power BI Desktop today, no Azure
subscription required. The `export` stage (`orchestration/export_stage.py`)
writes every Gold Delta table to Parquet, and `docker-compose.yml` bind-mounts
that output to `./powerbi-export/` on the host — a plain Docker volume
(like `lakehouse-data`) isn't visible outside the container, so this stage
specifically needs a bind mount instead.

1. Run the full pipeline, including the new `export` step:
   ```bash
   make run-pipeline
   ```
   Or, if Gold is already built, just the export step on its own:
   ```bash
   docker compose run --rm lakehouse export
   ```
2. In Power BI Desktop: **Get Data → Folder**, browse to
   `<repo>/powerbi-export/<table_name>/` (one call per table — each Gold
   table is its own subfolder, e.g. `powerbi-export/fact_catastrophe_event/`,
   containing one or more Spark-written `part-*.parquet` files, not a single
   file Power BI's plain Parquet connector expects).
3. In the Folder dialog, **Combine & Transform** and filter to `.parquet`
   extension files — this merges the part-files into one table, the same
   pattern used for any Spark output folder.
4. Repeat per table: `dim_date`, `dim_geography`, `dim_geography_state`,
   `dim_event_type`, `dim_alert_status`, `fact_catastrophe_event`,
   `fact_regional_alert_activity`, `fact_complaint_trend`,
   `leakage_risk_metric`. Build the same three pages described below from
   these tables directly, joining on the same keys `reporting_views.sql`
   uses (`state_geography_key`, `date_key`).

This is **Import, not DirectQuery** — Power BI loads a static snapshot.
Rerunning `make run-pipeline` does not update the report automatically;
re-run the `export` step, then click **Refresh** in Power BI Desktop.

## Production Azure/Databricks path

The architecture `docs/architecture_ideal_vs_actual.md` section 7 calls
"ideal": Power BI connects live via DirectQuery, so a Gold refresh shows up
without any manual export/refresh step. This is what `infra/*.tf` actually
provisions, and none of it is invented for this doc — every resource named
below exists in this repo today, just not deployed by default:

1. **Resource group + Databricks workspace** (`infra/main.tf`,
   `azurerm_databricks_workspace.this`) — Premium SKU, required for Unity
   Catalog.
2. **ADLS Gen2 storage** (`azurerm_storage_account.lakehouse`,
   `is_hns_enabled = true`) + a private container
   (`azurerm_storage_container.lakehouse`) as the actual Delta table storage.
3. **Managed-identity access** (`azurerm_databricks_access_connector.this` +
   a `Storage Blob Data Contributor` role assignment) — lets Unity Catalog
   read/write the storage account with no stored secret.
4. **Unity Catalog** (`infra/catalog.tf`): a storage credential and external
   location pointing at that storage account, then `databricks_catalog
   .lakehouse` with `bronze`/`silver`/`gold` schemas — this is what
   `LakehouseSettings.catalog_name`/`bronze_schema`/`silver_schema`/
   `gold_schema` actually target once `storage_root` points at `abfss://`
   instead of the local `file:///data/lakehouse` Docker uses.
5. **The Power BI SQL warehouse** (`infra/sql_warehouse.tf`,
   `databricks_sql_endpoint.power_bi`) — serverless, `2X-Small`, auto-stops
   after 10 minutes idle, so cost tracks actual dashboard usage rather than
   uptime. Gated behind `var.enable_sql_warehouse` (**default `false`**) —
   this resource, and the scheduled jobs in `infra/jobs.tf`
   (`var.enable_scheduled_jobs`, also default `false`), are the only
   continuously-billing pieces here, so a plain `terraform apply` provisions
   everything except them.

**Prerequisites this repo assumes but doesn't provision:** a Unity Catalog
metastore already assigned to the workspace's region at the Databricks
account level (`infra/catalog.tf`'s own comment), and a Databricks personal
access token supplied as `TF_VAR_databricks_pat` (`providers.tf` uses this
instead of an Azure CLI session).

**To actually stand this up:** `terraform apply` from `infra/` with
`storage_account_name`, `cluster_single_user_name`, and
`TF_VAR_databricks_pat` set, plus `-var enable_sql_warehouse=true` once
Gold has real data to query. This provisions real, billed Azure/Databricks
resources against your subscription — not something to run without deciding
to take on that cost deliberately.

## Connecting Power BI to the data (once deployed)

1. Power BI Desktop → Get Data → Databricks.
2. Server hostname / HTTP path: from the SQL warehouse provisioned by
   `infra/sql_warehouse.tf` (Databricks workspace → SQL Warehouses → Connection details).
3. Connect to the `gold` schema and import the five views in
   `docs/reporting_views.sql` (`v_elevated_pressure_regions`,
   `v_complaint_trend_acceleration`, `v_catastrophe_pressure_map`,
   `v_activity_trend_over_time`, `v_roi_summary`), plus
   `gold.dim_geography_state` and `gold.dim_date` for slicers.
4. Use DirectQuery (not Import) so the dashboard picks up each Gold refresh
   without a separate Power BI refresh schedule. Note this does *not* make
   the dashboard 15-minute-fresh: these views read Gold tables, which
   recompute daily — see the "Refresh cadence" section below and
   `docs/batch_vs_streaming_memo.md`.

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
- **Map**, sourced from `v_catastrophe_pressure_map`: state filled map
  colored by `claims_surge_risk`, sized by `total_damage_property_usd`.
  Click-through filters the page to that region. Note this is a state-level
  map: every KPI is defined at region (state) grain, so there is no
  county-level pressure score to shade — see `docs/interview_walkthrough.md`'s
  "Modeling approach".
- **Drill-through page**: matched storm events and declaration history for
  the clicked region (`gold.fact_catastrophe_event` filtered by
  `state_geography_key`).

## Page 2: Trends Over Time

- Sourced from `v_activity_trend_over_time`: a line chart with
  `avg_claims_surge_risk`, `declaration_count`, and `complaint_count` over
  `date`, one line per selected state (slicer on `dim_geography_state.state`).
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
