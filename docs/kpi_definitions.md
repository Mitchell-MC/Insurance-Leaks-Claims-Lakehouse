# KPI Definitions

Formal definitions for the four business KPIs named in the project brief.
Each entry states the calculation, the Phase 2 source columns it depends on
(field names per the FEMA OpenFEMA Disaster Declarations Summaries v2 API,
the NOAA Storm Events Database CSV format, and the NWS `api.weather.gov`
alerts schema — all public, versioned external contracts), and the Gold
table/column that will serve it once Phase 5 lands. This doc is the contract
Phase 5's fact/mart design must satisfy.

---

## 1. Estimated Claims Surge Risk

**Definition:** A composite, region-level score estimating how much
above-normal claims volume a region is likely to see in the near term,
combining active weather severity, recent storm intensity, and historical
declaration frequency for that region.

**Formula:**
```
claims_surge_risk(region, as_of_date) =
    w1 * normalized(active_alert_severity_score(region, as_of_date))
  + w2 * normalized(rolling_30d_storm_intensity(region, as_of_date))
  + w3 * normalized(historical_declaration_frequency(region))
```
where weights `w1..w3` are named constants defined alongside
`processing/features/catastrophe_pressure_score/` (Phase 4) and documented
with their justification in that feature's docstring.

**Source columns (Phase 2 bronze/silver):**
- `active_alert_severity_score`: NWS Alerts API `severity`, `urgency`,
  `certainty`, `areaDesc`, `effective`, `expires` (`bronze.nws_alerts_snapshots`)
- `rolling_30d_storm_intensity`: NOAA Storm Events `EVENT_TYPE`, `MAGNITUDE`,
  `DAMAGE_PROPERTY`, `BEGIN_DATE_TIME`, `STATE`, `CZ_NAME`
  (`silver.noaa_storm_events`)
- `historical_declaration_frequency`: FEMA `incidentType`, `declarationDate`,
  `state`, `designatedArea` (`silver.fema_declarations`)

**Gold source:** `gold.fact_regional_alert_activity` joined to
`gold.fact_catastrophe_event`, surfaced via the
`processing/features/catastrophe_pressure_score` output column in the
regional mart (Phase 5).

---

## 2. Complaint Rate Trend

**Definition:** Percent change in insurance complaint volume for a region
over a rolling window (e.g., current 30 days vs. trailing 30-day baseline).

**Formula:**
```
complaint_rate_trend(region, as_of_date) =
    (complaints(region, last_30d) - complaints(region, prior_30d))
    / complaints(region, prior_30d)
```

**Source columns:** complaint date, region/county, and complaint type/reason
fields from the Texas DOI complaint data (optionally supplemented by
Florida OIR). **Open dependency:** structured, column-level availability of
this data is not yet confirmed — see `docs/data_limitations.md` (Phase 3),
which must document the actual extract schema before this formula is
finalized, and any fallback proxy if a structured extract isn't available.

**Gold source:** `gold.fact_complaint_trend` (Phase 5).

---

## 3. Average Days from Event to Declaration

**Definition:** Mean number of days between a storm event's onset and its
associated FEMA disaster declaration, grouped by state and incident type —
the clearest lagging-signal KPI, since both dates come from FEMA's own
declaration record.

**Formula:**
```
avg_days_event_to_declaration(state, incidentType) =
    AVG(declarationDate - incidentBeginDate)
    WHERE state = :state AND incidentType = :incidentType
```

**Source columns:** FEMA `incidentBeginDate`, `declarationDate`, `state`,
`incidentType`, `disasterNumber` (`silver.fema_declarations`). Fully
traceable to real, existing columns in the Phase 2 FEMA source — no
complaint-data dependency.

**Gold source:** `gold.fact_catastrophe_event` (grain: one row per FEMA
disaster declaration), Phase 5.

---

## 4. Leakage Exposure Proxy

**Definition:** A regional flag/score identifying the highest-risk
combination for claims leakage: elevated catastrophe pressure, a rising
complaint trend, and slower-than-average declarations — i.e., operational
stress building across all three signals at once.

**Formula:**
```
leakage_exposure_proxy(region, as_of_date) =
    z(claims_surge_risk(region, as_of_date))
  + z(complaint_rate_trend(region, as_of_date))
  + z(avg_days_event_to_declaration(region) - regional_baseline_days)
```
`z()` denotes a z-score normalized across regions for the given `as_of_date`,
so the three differently-scaled signals combine on a comparable basis.
Named constants for any thresholding (e.g., "high leakage exposure" cutoff)
are defined in `gold/features/leakage_risk_metric/` (Phase 5) with docstring
justification.

**Source:** derived entirely from KPIs 1–3 above — no new source columns,
but inherits KPI 2's open complaint-data dependency.

**Gold source:** `gold.fact_regional_alert_activity` +
`gold.fact_catastrophe_event` + `gold.fact_complaint_trend`, combined in
`gold/features/leakage_risk_metric/` (Phase 5).

---

## Verification status

KPIs 1 and 3 are fully traceable to real, confirmed source columns today.
KPIs 2 and 4 depend on confirming the structured schema of the Texas DOI /
Florida OIR complaint extracts during Phase 2 ingestion — tracked as the
open item Phase 3's `data_limitations.md` must close out before Phase 5
gold modeling finalizes `fact_complaint_trend`.
