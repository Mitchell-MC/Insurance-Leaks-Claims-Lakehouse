# Productionization Next Steps

What this portfolio project intentionally left out of scope
([charter.md](charter.md)'s Scope section), and what real Meridian systems
each would require. Ordered by impact.

## 1. Real complaint data (highest impact)

**Gap:** `docs/data_limitations.md` — no structured Texas DOI/Florida OIR
complaint source exists publicly. `complaint_rate_trend` runs on a
documented proxy; `gold.fact_complaint_trend` is a correctly-shaped, empty
table.

**What it takes:** A data-sharing agreement with a state DOI/OIR, or a
commercial complaint-data vendor (e.g. NAIC member-company complaint
extracts, if Meridian has an NAIC data-sharing relationship). Once a
structured source lands, only the ingestion layer changes — a new
`ingestion/features/complaint_data/` slice following the existing
`BaseIngestor` pattern — everything downstream (`fact_complaint_trend`'s
schema, `docs/reporting_views.sql`'s views, the leakage z-score formula)
already expects this shape and needs no changes.

## 2. Decouple Gold pressure-score refresh from the daily batch

**Gap:** `docs/batch_vs_streaming_memo.md` — `bronze.nws_alerts_snapshots`
refreshes every 15 minutes, but `claims_surge_risk` (Gold) only recomputes
once daily alongside FEMA/NOAA.

**What it takes:** Add a third, lightweight Databricks Job task (or extend
`lakehouse-alerts-snapshot` in `infra/jobs.tf`) that re-runs just
`processing/features/catastrophe_pressure_score` and
`gold/facts/features/fact_regional_alert_activity` on the 15-minute
schedule, reading the previous run's Silver/Process outputs rather than
recomputing them. Requires `orchestration/gold_stage.py`'s dimension-
building calls to be split from its fact-building calls, since dimensions
don't need to rebuild every 15 minutes.

## 3. Real period-over-period complaint trend

**Gap:** `orchestration/gold_stage.py`'s `_build_complaint_trend_proxy`
defaults to 0.0 (no trend signal) because computing a real trailing-30-day
trend needs yesterday's persisted `gold.fact_regional_alert_activity`
snapshot, not just today's.

**What it takes:** Once item 2 above gives regular Gold snapshots, read the
prior run's persisted table (a self-join on run date) instead of
short-circuiting to zero.

## 4. Policy exposure and vendor/claims assignment data

**Gap:** Everything in this project is catastrophe/weather signal —
there's no join to which policies are actually exposed in a pressured
region, or which vendors/adjusters are already assigned.

**What it takes:** Real Meridian policy administration and claims/vendor
management system extracts. This is the biggest lift: it requires internal
system access this portfolio project's public-data scope was never meant
to have (per `charter.md`'s Scope section).

## 5. SLA monitoring for alert freshness

**Gap:** No automated check that the 15-minute alerts job is actually
keeping up (vs. silently falling behind under NWS API rate limiting or an
extended outage).

**What it takes:** A Databricks Workflow SQL alert (or a scheduled query)
comparing `MAX(_ingestion_metadata.loaded_at)` in `bronze.nws_alerts_snapshots`
against `now()`, paging on-call if the gap exceeds ~30 minutes (2x the
expected poll interval).

## 6. Geography reference re-ingestion cadence

**Gap:** Partially closed. `ingestion/features/geography_reference/feature.py`
now skips the actual download once the configured `census_gazetteer_year` has
already been ingested (see `docs/data_limitations.md`), so the wasted-bandwidth
part of this gap is fixed. What remains: the ingestor is still *invoked* daily
alongside FEMA/NOAA on the same batch schedule — it just no-ops most days —
rather than having its own low-frequency schedule, purely because the
pipeline doesn't yet distinguish "reference data" from "operational data" as
separate schedules.

**What it takes:** A third, low-frequency (monthly or on-demand) Databricks
Job for `ingestion/features/geography_reference/`, decoupled from the daily
batch pipeline, so the stage doesn't need to run (and immediately skip) daily.

## 7. Watermark key-set storage doesn't scale past portfolio volumes

**Gap:** `docs/data_limitations.md` — FEMA and NOAA's tombstone detection
stores the previous run's full natural-key set as a JSON blob in one Delta
cell (`_ingestion_state.extra`). Proportionate for FEMA's total record count
and for NOAA's per-year, recent-years-only scope, but would not scale to a
source with a much larger single-comparison key-set.

**What it takes:** A dedicated `_ingestion_keys` table (one row per key,
not one JSON blob per source) if a source's key-set ever grows past what's
comfortable in a single cell — allows a real anti-join for tombstone
detection instead of a Python-side set difference.

## 8. FEMA/NOAA Silver DQ gates fail against real full-dataset data

**Gap:** Discovered via the Docker Compose local-validation work (see
`architecture_ideal_vs_actual.md`'s section 8), not previously exercised:
`silver.fema_declarations` fails its `schema_unique_key` check against the
full live OpenFEMA dataset (24 real duplicate-key groups), and
`silver.noaa_storm_events` fails `schema_not_null_STATE` against any full
real year (NOAA's marine-zone rows always map to null `STATE`). Both are
detailed in `docs/data_limitations.md`.

**What it takes:** A FEMA dedup rule mirroring NOAA's `dropDuplicates`
pattern, and a decision on NOAA marine zones (drop them in Silver, or make
`STATE` nullable) — both small, targeted fixes once prioritized.

## Note: what's now verifiable locally vs. still requires real Azure

**Verifiable in Docker Compose today** (see `architecture_ideal_vs_actual.md`'s
section 8 and the README's "Running the pipeline locally with Docker
Compose" section): ingestion writing/reading real Delta tables, Silver
standardization/DQ logic, Processing joins/window functions, and Gold
star-schema builds — all against real (if locally-stored) Delta tables,
using the real `lakehouse` CLI entry point.

**Still requires a real Azure/Databricks apply:** Unity Catalog governance,
true ADLS Gen2 storage semantics, the wheel-based deploy path, scheduled-job
execution/monitoring, and the serverless SQL warehouse serving Power BI.
