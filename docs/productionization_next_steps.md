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

**Gap:** `docs/batch_vs_streaming_memo.md` notes the Census Gazetteer
county reference (a yearly file) re-ingests daily alongside FEMA/NOAA,
purely because the pipeline doesn't yet distinguish "reference data" from
"operational data" as separate schedules.

**What it takes:** A third, low-frequency (monthly or on-demand) Databricks
Job for `ingestion/features/geography_reference/`, decoupled from the daily
batch pipeline.
