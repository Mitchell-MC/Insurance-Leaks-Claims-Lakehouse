# Architecture Memo: Batch vs. Near-Real-Time Ingestion

**To:** Engineering review
**From:** Data Engineering
**Re:** Why NWS alerts run on a 15-minute schedule and FEMA/NOAA run daily

## The decision

`infra/jobs.tf` defines two separate Databricks Jobs instead of one:

- **`lakehouse-batch-pipeline`** — FEMA declarations, NOAA storm events, and
  Census geography, chained Ingest → Silver → Process → Gold, on a **daily**
  schedule (06:00 UTC).
- **`lakehouse-alerts-snapshot`** — NWS active-alert snapshots only, on a
  **15-minute** schedule, ingesting to Bronze and stopping there.

## Why alerts need a tight schedule

NWS active alerts are the only source in this project that represents
*currently unfolding* risk — an alert issued this hour is operationally
relevant this hour, not tomorrow. The scoping memo's whole premise
([docs/scoping_memo.md](scoping_memo.md)) is giving the Catastrophe Response
Manager lead time to pre-position adjusters *before* FEMA formalizes a
declaration; a daily alert refresh would reintroduce exactly the lag this
project exists to remove. 15 minutes is tight enough to catch a fast-moving
severe-weather episode without over-polling a public API that itself only
updates on the order of minutes, not seconds (true streaming/websocket
ingestion would add operational complexity — a long-running job, checkpoint
management — for a source that doesn't push updates faster than this poll
interval would catch anyway).

## Why FEMA/NOAA/geography stay batch

- **FEMA declarations** update when a declaration is signed — a discrete,
  infrequent event (days between updates even during active hurricane
  season), never sub-daily.
- **NOAA storm events** are published as versioned yearly bulk files with a
  creation-date suffix (`ingestion/features/noaa_storm_events/feature.py`)
  that itself updates on a monthly-or-slower cadence as NOAA finalizes
  event records.
- **Census geography** is a yearly reference file. It doesn't need
  re-ingesting more than once a year, let alone daily — it's re-ingested
  daily here only because the pipeline doesn't yet distinguish
  "reference data" from "slowly-changing operational data" as separate
  schedules; a natural next refinement once this project scales past
  portfolio scope.

Re-running the full Silver → Process → Gold chain every 15 minutes alongside
alerts would burn compute reprocessing FEMA/NOAA data that hasn't changed,
for no signal gain — the alerts job deliberately stops at Bronze and lets
the next daily batch run pick up whatever snapshots accumulated.

## What this means for `claims_surge_risk` freshness

`processing/features/catastrophe_pressure_score/feature.py`'s
`active_alert_severity_score` term reads directly from
`bronze.nws_alerts_snapshots` — the alerts job keeps that Bronze table
current to within 15 minutes, but `claims_surge_risk` itself (a Gold-layer
value) only recomputes once daily. A production-grade version of this
pipeline would decouple that: run the Gold-layer pressure-score recompute
on the alerts job's schedule too (it's a cheap aggregation, unlike the full
Silver/Process chain), so the dashboard's leading-indicator score is as
fresh as the alerts feeding it. Tracked in
[productionization_next_steps.md](productionization_next_steps.md).

## Operational visibility

Both jobs run via `docs/batch_vs_streaming_memo.md`'s sibling deliverable —
`orchestration/run_logger.py`'s structured JSON-line stage logs (captured in
Databricks Workflows' task stdout) plus `email_notifications`/
`webhook_notifications` on task failure (`infra/jobs.tf`), so a failed alert
poll or a failed batch stage pages the on-call path without needing a
separate observability stack.
