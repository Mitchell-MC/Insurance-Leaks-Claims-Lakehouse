# Interview Walkthrough

A 5-minute walkthrough of this project, grounded in what's actually in this
repo (not a generic template) — file paths included so claims made here can
be checked against real code.

## The 30-second pitch

I built an insurance catastrophe-response lakehouse that helps claims
leaders anticipate where severe weather and disaster activity are likely to
create service bottlenecks and claims leakage. It ingests FEMA disaster
declarations, NOAA storm events, and live NWS weather alerts into a
Bronze/Silver/Gold Databricks lakehouse, runs distributed PySpark
transformations to score regional catastrophe pressure, and models a Gold
star schema an executive Power BI dashboard queries directly.

## Why Data Engineering Core was the focus

This project was scoped against a documented skills gap (Distributed
Computing, Data Modeling, Batch vs. Streaming decision-making all at 3/5),
not picked arbitrarily. Concretely, that shows up in:
- `src/lakehouse/processing/features/rolling_event_intensity/feature.py` —
  a range-based `Window` (not row-based), because a trailing-30-day window
  needs to reflect actual elapsed days, not just "the last N rows."
- `src/lakehouse/gold/` — a real dimensional model (5 dimensions, 3 facts,
  one derived metric), not a flat denormalized table.
- `docs/batch_vs_streaming_memo.md` — an explicit, justified architecture
  decision, not "everything runs on the same schedule because that's simpler."

## How I handled heterogeneous public data

FEMA returns state as a 2-letter USPS code ("TX"); NOAA returns the full
state name ("TEXAS"). I caught this mismatch mid-build — my first pass at
the Phase 4 region join assumed they'd already match, and the bug hid
behind self-consistent test fixtures until I traced it back while writing
the Gold layer. The fix lives in `src/lakehouse/silver/us_state_codes.py`:
a single source-of-truth mapping every Silver transformer uses, so the
reconciliation happens once, in the standardization layer, not scattered
across every downstream join. That's the real story of what "different
geographic conventions" means in practice, not just a bullet point.

## Distributed computing decisions

`docs/partitioning_benchmark_memo.md` compares two real physical query
plans (captured via `.explain(mode="formatted")` against synthetic
production-scale data — 2M NOAA rows, 2K FEMA rows): a shuffle-heavy
`SortMergeJoin` when auto-broadcast is disabled, versus a `BroadcastHashJoin`
when the small FEMA side is explicitly broadcast
(`processing/features/regional_event_join/feature.py`). The large NOAA side
gets scanned once with zero shuffle in the broadcast plan — that's the
concrete tradeoff, not a hand-wave about "using Spark efficiently."

## Modeling approach

The Gold star schema's grain decisions were deliberate, not default.
`fact_catastrophe_event` is one row per FEMA declaration — matching exactly
what KPI 3, Average Days from Event to Declaration, needs — and
`fact_regional_alert_activity` is one row per region per snapshot date.
Both join `dim_date` and `dim_geography_state`.

The geography split is the grain decision worth talking about: the Census
Gazetteer is county-grain, but every KPI is defined at *region* (state)
level. Joining a state-grain fact to a county-grain dimension on `state`
alone fans one declaration out into one row per county — 254x for Texas —
silently inflating every count-based KPI. So `dim_geography_state`
(state-grain) keys the facts, while county-grain `dim_geography` stays
available for future county-level facts. The regression tests for both facts
deliberately use multi-county fixtures, since a one-county-per-state fixture
cannot detect this class of bug.

`dim_event_type` and `dim_alert_status` are built and published but not yet
referenced by any fact — they're conformed dimensions staged for the
event-grain fact described in `productionization_next_steps.md`, not live
star-schema edges today.

`fact_complaint_trend` exists with the correct schema but zero rows — see
the leakage-data story below.

## Business ROI framing

`docs/roi_assumptions.md` turns "earlier detection saves money" into a
named formula (`avoided_backlog_days * cost_per_adjuster_day_of_delay`)
with every constant labeled — including which one is a real KPI-3-derived
number and which is an illustrative placeholder for this fictitious client.
I'd rather say "this number is illustrative, here's what would make it
real" in an interview than let a made-up number pass as fact.

## Architecture judgment

`infra/jobs.tf` runs two separate Databricks Jobs on different schedules
(15-minute NWS alerts, daily FEMA/NOAA/Gold rebuild) instead of one job on
one schedule. `docs/batch_vs_streaming_memo.md` explains why, including the
honest gap this creates: `claims_surge_risk` only recomputes daily even
though its alert input refreshes every 15 minutes — flagged as a concrete
next step, not glossed over.

## Data quality strategy

`src/lakehouse/silver/data_quality/checks.py` has four reusable checks
(null geography, invalid dates, duplicate keys, schema drift), and every
Silver transformer declares which ones apply to it and attaches the result
as `_dq_metadata` on every row (`silver/base_transformer.py`) — so a bad
run is visible in the data itself, not just in a log someone has to go find.

## The leakage-data gap — the most honest part of this project

`docs/data_limitations.md` documents that Texas DOI/Florida OIR complaint
data doesn't have a structured API or bulk extract — only a consumer-facing
lookup form. I could have built a fragile scraper to check a box, or
fabricated placeholder complaint numbers. I did neither: `complaint_rate_trend`
resolves to a documented fallback proxy (catastrophe-pressure acceleration),
and `fact_complaint_trend` ships as a correctly-shaped, zero-row table with
the real gap written down in `productionization_next_steps.md`. I'd rather
walk an interviewer through a documented, principled scope cut than pretend
a data source exists that doesn't.

## What I'd productionize next

See [productionization_next_steps.md](productionization_next_steps.md) in
full; top three: a real complaint-data source (data-sharing agreement or
vendor, not a scraper), decoupling the Gold pressure-score recompute from
the daily batch schedule so it's as fresh as the alerts feeding it, and
joining real policy-exposure/vendor-assignment data once it exists.
