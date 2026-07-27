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

## Silver DQ checks are observational, not gating

**Status: open — accepted for this phase.**

`BaseSilverTransformer.run()` (`silver/base_transformer.py`) runs each
transformer's `dq_checks()` and records the pass/fail counts in the
`_dq_metadata` struct it attaches to every Silver row, but it always writes
Silver regardless of the result, and nothing in `orchestration/` reads
`checks_failed` to block, quarantine, or alert. A failed null-geography or
duplicate-key check is therefore *visible after the fact* (queryable from
`_dq_metadata`) but has **zero effect on the pipeline** — bad rows still
reach Gold.

This is a deliberate portfolio-scope tradeoff, not an oversight: gating
requires deciding quarantine-vs-fail-the-run semantics per check and a place
to route rejected rows. Closing it means having the stage runners inspect
`_dq_metadata` and fail (or divert to a `silver_rejects` table) on
critical-check failure.

## FEMA and geography Silver transformers detect duplicates but don't drop them

**Status: open — accepted for this phase.**

`silver/features/noaa_storm_events/feature.py` both checks for and drops
duplicates (`dropDuplicates(["_DEDUP_KEY"])`). The FEMA and geography
transformers only *check*:
`check_no_duplicate_keys(df, ["disasterNumber", "designatedArea"])` and
`check_no_duplicate_keys(df, ["GEOID"])` respectively. Combined with the
non-gating behavior above, a duplicate from (for example) an OpenFEMA
pagination retry would pass through to Silver and Gold, inflating
`historical_declaration_frequency` and KPI-3's declaration counts, with only
a `_dq_metadata` counter recording that it happened.

Geography is expected to be naturally unique on `GEOID`. **FEMA is not** —
this was assumed, not verified, until the Docker Compose local-validation
work (see `architecture_ideal_vs_actual.md`'s section 8) actually ran
`schema_unique_key` against the full live OpenFEMA dataset and found 24 real
`(disasterNumber, designatedArea)` duplicate groups (31 failing rows), which
now blocks `silver.fema_declarations` outright since this check is
error-severity by default. This is exactly the failure mode this section
already warned about — it just hadn't been exercised against full real data
before. Closing it needs an explicit "keep the newest" or "keep the
DQ-flagged records" dedup rule added to `FemaDeclarationsTransformer`,
mirroring NOAA's `dropDuplicates` pattern.

## Watermark state and tombstone detection

**Status: implemented, with scale trade-offs accepted for this project's
data volumes.**

`ingestion/watermark_store.py` backs all four sources' incremental fetch
logic with one Delta table, `bronze._ingestion_state`. Two related trade-offs
are worth being explicit about:

**Tombstone detection is not the same guarantee for every source.** FEMA and
NOAA flag a record whose natural key disappeared from the source via an
`_is_current` column (Silver filters these out before its overwrite), but:

- FEMA's tombstone comparison only runs on an *unfiltered* fetch (no watermark
  yet, or one explicitly reset) — a normal incremental run only sees
  new/changed records, so it structurally cannot tell whether a record that
  simply didn't change is still present upstream or was quietly retracted. A
  retracted declaration is only caught the next time a full/unfiltered
  re-sync happens, not on every incremental run.
- NOAA's tombstone comparison is scoped to only the years actually re-fetched
  this run (recent years, per `noaa_recheck_recent_years`), not the full
  1996-present history — key-set-diffing three decades of events on every run
  would be both slow and store an unbounded key-set in a single Delta cell.
- Geography and NWS alerts have no tombstone logic at all: a Gazetteer vintage
  year is a complete county list (no per-row deletes distinct from a new
  vintage), and an NWS alert not appearing in the next snapshot is the normal,
  expected case (expired/replaced), not a deletion.

**The previous-run key-set is stored as a JSON blob**, in the watermark row's
`extra` column. This is proportionate for FEMA's total record count (tens of
thousands) but would not scale to key-set-diffing NOAA's full multi-million-row
history in one Delta cell — which is exactly why NOAA's tombstone scope is
narrowed to recent years above, rather than a general-purpose mechanism.

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

## NOAA `STATE` is declared non-nullable, but real data always has nulls there

**Status: open — discovered via the Docker Compose local-validation work,
not yet fixed.**

`silver/features/noaa_storm_events/schema.py` declares `STATE` `nullable=False`.
`_map_state_to_usps` (`silver/features/noaa_storm_events/feature.py`) maps a
raw state/territory name to its USPS code via a fixed lookup table
(`us_state_codes.py`), returning `null` for anything not in that table.

NOAA's real `STATE` column is not limited to the 50 states plus territories
— it also includes marine zones (`"GULF OF MEXICO"`, `"LAKE MICHIGAN"`,
`"ATLANTIC SOUTH"`, and similar) for offshore storm events, none of which are
in `STATE_NAME_TO_USPS`. Every full year of real NOAA data checked so far
(2023, 2024) contains rows with one of these marine-zone values, meaning
`schema_not_null_STATE` — an error-severity check — fails for any full,
real year, not as a rare edge case but structurally, every time. This
appears to have gone unnoticed because prior verification relied on small,
hand-built test fixtures that never included a marine-zone row.

Closing this needs an explicit decision: either drop marine-zone events
during Silver standardization (they're not attributable to a U.S. state's
claims-leakage risk, this project's actual analytical unit), or relax
`STATE` to nullable and let downstream aggregations filter/`COALESCE`
around the null rather than gate the whole run on it.

## `DAMAGE_PROPERTY` is a rough estimate, not an audited figure

NOAA's damage dollar figures are self-reported estimates from local weather
offices at time of event, frequently rounded or "0.00K" for events where no
estimate was made. The Silver severity-banding logic
(`_severity_band` in `silver/features/noaa_storm_events/feature.py`)
compensates by always banding a fixed list of historically high-leakage
event types (Hurricane, Tornado, Flash Flood, Storm Surge/Tide) as
"severe" regardless of the reported dollar figure — see the `# Reason:`
comment at that function for the specific list and rationale.
