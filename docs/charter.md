# Project Charter — Insurance Claims Leakage & Catastrophe Response Analytics Lakehouse

## Fictitious client profile

**Meridian Mutual Insurance** — a regional property & casualty (P&C) insurer
writing homeowners, dwelling, and commercial-property policies across
hurricane-, flood-, and severe-storm-exposed states (initial focus: Texas
and Florida, per the Phase 2 complaint-data sources). Meridian's claims
organization is regionally structured: each region owns adjuster staffing,
vendor (contractor/appraiser) assignment, and first-notice-of-loss (FNOL)
response for its territory.

## Business problem

Meridian struggles to anticipate where catastrophe events will create claims
backlogs, customer dissatisfaction, and avoidable claims leakage. Claims
operations, vendor management, and regional leaders receive fragmented
signals — weather alerts, disaster declarations, and complaint trends — too
late to proactively allocate adjusters, triage high-risk regions, or reduce
cycle time and indemnity leakage. Each signal currently lives in a different
system (NWS alerts are watched informally, FEMA declarations arrive via news
coverage, complaint trends are reviewed monthly in arrears), so no one
function sees pressure building across all three before it shows up as a
backlog.

## Stakeholder personas

| Persona | Owns | Cares about | Primary KPI(s) |
|---|---|---|---|
| **Claims Operations Leader** | Adjuster staffing & workload balancing across regions | Cycle time, backlog risk, staffing lead time | Estimated Claims Surge Risk, Leakage Exposure Proxy |
| **Catastrophe Response Manager** | CAT event triage, vendor mobilization | How fast declared/undeclared events translate to operational load | Estimated Claims Surge Risk, Avg. Days Event→Declaration |
| **Regional VP** | Regional P&L, service-level commitments | Which territories are under elevated risk this week/month | Estimated Claims Surge Risk, Leakage Exposure Proxy (by region) |
| **Customer Experience Lead** | Complaint escalation, NPS/retention | Complaint volume acceleration tied to service delays | Complaint Rate Trend |

## Scope

**In scope:** FEMA disaster declarations, NOAA storm events, NWS active
alerts, Census geography reference, and publicly available state insurance
complaint data (Texas DOI, with Florida OIR as optional enrichment) — ingested,
modeled, and published as a regional risk/leakage dashboard.

**Out of scope (see [productionization_next_steps.md](productionization_next_steps.md), written in Phase 8):**
actual policy exposure data, real claims/vendor assignment systems, and SLA
monitoring integrations. These require internal Meridian systems that don't
exist in this portfolio project's public-data scope.

## Success criteria

Every KPI in [kpi_definitions.md](kpi_definitions.md) is computable from a
single SQL query against Gold tables ([kpi_validation_queries.sql](kpi_validation_queries.sql),
written in Phase 5), and each traces to a real column in a Phase 2 source
dataset — not a placeholder.
