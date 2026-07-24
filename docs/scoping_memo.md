# Scoping Memo: Why Earlier Catastrophe Signal Detection Matters

**To:** Claims Operations Leadership
**From:** Data Engineering
**Re:** Business case for a unified catastrophe/complaint risk signal

## The problem in one sentence

Meridian's three earliest warning signals for claims surges — active
weather alerts, FEMA disaster declarations, and complaint trends — live in
three different places and are reviewed on three different cadences, so no
one sees the full picture until a backlog has already formed.

## Why earlier detection reduces cost

**Staffing.** Adjuster deployment decisions today are reactive: staffing
follows the backlog rather than anticipating it. A regional risk score that
updates as alerts and storm events accumulate — before FEMA formalizes a
declaration — gives the Catastrophe Response Manager days of lead time to
pre-position adjusters instead of scrambling after FNOL volume spikes.

**Cycle time.** [KPI: Average Days from Event to Declaration] shows how much
lag already exists between an event and its formal federal recognition.
Regions with an elevated `claims_surge_risk` score before that declaration
lands are exactly the regions where early triage shortens the eventual
claim-to-close cycle time the most.

**Customer retention.** Complaint volume acceleration is a lagging
indicator — by the time it's visible, dissatisfaction has already happened.
Pairing it with the leading catastrophe-pressure signal lets the Customer
Experience Lead flag regions at risk of complaint escalation *before* the
complaints arrive, not after.

## What this project delivers

A single regional view — the `leakage_exposure_proxy` — that combines
catastrophe pressure, complaint trend, and declaration lag into one number
per region, refreshed on the same cadence as the underlying alerts (near
real-time) rather than the current monthly complaint-review cycle.

## Bottom line

The cost of reacting late is staffing scrambles, longer cycle times, and
avoidable complaint escalation. The cost of building this signal is one
lakehouse pipeline and one dashboard. This memo is the source material for
the Phase 8 interview walkthrough's business-impact narrative.
