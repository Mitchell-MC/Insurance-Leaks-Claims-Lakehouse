# Data Limitations

Known gaps and assumptions in the Silver-layer data, and how they affect the
KPIs defined in [kpi_definitions.md](kpi_definitions.md).

## Complaint data is not available in structured form

**Status: closed, with a fallback proxy — not planned for this phase.**

`docs/charter.md` scoped Texas DOI (with Florida OIR as optional enrichment)
as the complaint-data source for `complaint_rate_trend` (KPI 2) and
`leakage_exposure_proxy` (KPI 4). Investigating the source confirms neither
publishes a structured bulk extract or API: Texas DOI's complaint tool
(`tdi.texas.gov/consumer/complfrm.html`) is a consumer-facing case-lookup
form, and Florida OIR's catastrophe/market pages are report-oriented, not
record-level downloads. Notably, the project's own Phase 2 source-acquisition
task list (the mentoring brief this project was scoped from) never included
complaint data in Bronze ingestion — only FEMA, NOAA, NWS alerts, and
Census geography — which lines up with this finding.

Building a scraper against a consumer web form to backfill this is fragile
(no stable schema, no versioning, likely to break or require CAPTCHA
handling) and disproportionate to a portfolio project's scope. Rather than
fabricate complaint rows or silently drop the KPIs that depend on them, this
project uses a **documented fallback proxy**:

### Fallback: `complaint_rate_trend` (KPI 2)

Redefined as the rolling 30-day rate of *change* in a region's catastrophe
pressure score (from `gold.fact_regional_alert_activity`) rather than actual
complaint volume — the reasoning being that complaint volume is itself
mostly *driven by* catastrophe pressure with a lag (per the charter's own
"complaint trends... reviewed monthly in arrears" framing), so its
acceleration is a reasonable leading proxy until real complaint data is
available:

```
complaint_rate_trend_proxy(region, as_of_date) =
    (pressure_score(region, last_30d) - pressure_score(region, prior_30d))
    / NULLIF(pressure_score(region, prior_30d), 0)
```

### Fallback: `leakage_exposure_proxy` (KPI 4)

Unchanged formula, but its `complaint_rate_trend` term now resolves to the
proxy above instead of real complaint data. `fact_complaint_trend` (Gold,
Phase 5) is built as an explicitly empty table with the correct grain and
schema — not populated with synthetic data — so anyone querying it sees
zero rows and the documented reason why, rather than fabricated numbers.

### What closes this gap for real

Tracked in [productionization_next_steps.md](productionization_next_steps.md)
(Phase 8): a real complaint-data source needs either a data-sharing
agreement with a state DOI/OIR, or a commercial data vendor — not a scraper.

## NOAA Storm Events schema drift (1996-present)

NOAA's column set has changed across the ~30 yearly bulk files (e.g. some
early years lack `MAGNITUDE_TYPE` or `CATEGORY`). Bronze ingestion
(`ingestion/features/noaa_storm_events/feature.py`) already tolerates this
via `unionByName(allowMissingColumns=True)`. The Silver transformer
(`silver/features/noaa_storm_events/feature.py`) only selects the small
column subset the KPIs actually need (`STATE`, `CZ_NAME`, `EVENT_TYPE`,
`BEGIN_DATE_TIME`, `DAMAGE_PROPERTY`, `MAGNITUDE`, `EVENT_ID`), all of which
are present across the full 1996-present range, so drift in less-used
columns doesn't propagate past Bronze.

## `DAMAGE_PROPERTY` is a rough estimate, not an audited figure

NOAA's damage dollar figures are self-reported estimates from local weather
offices at time of event, frequently rounded or "0.00K" for events where no
estimate was made. The Silver severity-banding logic
(`_severity_band` in `silver/features/noaa_storm_events/feature.py`)
compensates by always banding a fixed list of historically high-leakage
event types (Hurricane, Tornado, Flash Flood, Storm Surge/Tide) as
"severe" regardless of the reported dollar figure — see the `# Reason:`
comment at that function for the specific list and rationale.
