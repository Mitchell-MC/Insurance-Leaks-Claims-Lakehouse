# Ideal Architecture vs. What Was Actually Implemented

Written for interview walkthroughs. The main section describes the architecture
this project *should* have in a real Meridian Mutual deployment. Each "Aside"
records what actually exists in this repo and why it diverges — the divergences
are the interesting part, since most were forced by real constraints discovered
at apply time rather than chosen up front.

---

## 1. Ingestion (Bronze)

**Ideal.** Each source lands raw and immutable, append-only, with provenance
attached to every row (`source_file_date`, `loaded_at`, `run_id`,
`record_count`). Bronze is never edited — reprocessing means replaying from
Bronze, not re-fetching from a source that may have changed underneath you.
Retries use exponential backoff, and schema drift is tolerated rather than
fatal, because a 30-year archive like NOAA's *will* change shape mid-history.

Ingestion should be incremental: FEMA and NOAA get watermarked so a daily run
pulls only what changed, and NWS alert snapshots land continuously since they
represent a point-in-time view that cannot be reconstructed later.

**Aside — what's implemented.** All four ingestors
(`src/lakehouse/ingestion/features/`) work as described, sharing a `BaseIngestor`
that attaches `_ingestion_metadata` and implements the retry/backoff from
`LakehouseSettings`. NOAA schema drift is handled with
`unionByName(allowMissingColumns=True)`.

Incrementality is now implemented, per-source, backed by a shared
`_ingestion_state` Delta table (`ingestion/watermark_store.py`):

- **FEMA** filters the OpenFEMA API with `$filter=lastRefresh gt '<watermark>'`
  after the first run, advancing the watermark to the newest `lastRefresh` seen.
- **NOAA** always re-fetches the directory listing (required to discover
  filenames) but only downloads a year's file if it's new, or within
  `noaa_recheck_recent_years` (default 2) of the present *and* its
  creation-date suffix is newer than the one already ingested — older years
  are treated as frozen once ingested once.
- **Geography** skips the download entirely once the configured
  `census_gazetteer_year` has already been ingested — this file only changes
  on a new Census vintage, so re-fetching it daily was pure waste.
- **NWS alerts** still fetches the full active-alerts snapshot every call —
  there is no "since" filter for "currently active alerts," so a watermark
  here is only a last-polled-at marker, not a fetch filter. This is the
  correct outcome for this source, not a remaining gap.

A related, narrower gap closed alongside watermarking: FEMA and NOAA now flag
a record whose natural key disappeared from the source between runs via an
`_is_current` column, and Silver filters those rows out before its full
overwrite — see `docs/data_limitations.md` for the real trade-off this makes
(tombstone detection only runs on unfiltered/full fetches, and NOAA's is
scoped to recently re-fetched years, not the full 30-year history).

---

## 2. Standardization (Silver)

**Ideal.** Silver reconciles the sources into a shape that can actually be
joined: FEMA's state names and NOAA's state codes resolved to one convention,
duplicates removed on each source's true business key, damage figures parsed to
numerics, and severity banded consistently. Every table carries data-quality
results, and **failed critical checks gate promotion** — bad rows are quarantined
to a rejects table and the run fails loudly rather than silently poisoning Gold.

**Aside — what's implemented.** The reconciliation, dedup, banding, and
`_dq_metadata` all exist (`src/lakehouse/silver/`). Two honest gaps, both now
documented in `docs/data_limitations.md`:

- **DQ checks are observational, not gating.** `BaseSilverTransformer.run()`
  records pass/fail counts but always writes Silver regardless. Nothing reads
  `checks_failed` to block or quarantine. A failed duplicate-key check is
  visible after the fact and has zero pipeline effect.
- **Only NOAA actually dedupes.** FEMA and geography *check* for duplicates
  without dropping them, so an OpenFEMA pagination retry would flow through to
  Gold and inflate declaration counts.

Both are accepted scope tradeoffs, not oversights — but "expected unique" plus
"a check that doesn't gate" is not the same as enforced.

There is also no Silver slice for NWS alerts: they are consumed directly from
Bronze by the processing layer. That is deliberate (a point-in-time snapshot
needs no standardization), and the architecture diagram shows it as a dotted
edge.

---

## 3. Distributed Processing

**Ideal.** The expensive step is joining ~30 years of storm events to disaster
declarations. Done naively this shuffles both sides; done well, the small side
(declarations) is broadcast and the large side is scanned once. Rolling
intensity windows use `rangeBetween` over an event-time column so a "30-day
window" means 30 elapsed days, not 30 preceding rows. Partitioning is chosen
against a measured benchmark, not a guess.

**Aside — what's implemented.** This is the strongest part of the codebase.
`regional_event_join` broadcasts FEMA and joins on state + a 7-day window; I
confirmed with `.explain()` that Catalyst produces a `BroadcastHashJoin`
matching the plan shape claimed in `docs/partitioning_benchmark_memo.md`.
`rolling_event_intensity` correctly uses `rangeBetween` over epoch seconds, not
`rowsBetween`. No stray `repartition`/`coalesce` calls contradict the memo.

---

## 4. Dimensional Model (Gold)

**Ideal.** A star schema whose **fact grain matches the KPI grain**. Every KPI
here is defined per region (state) per date, so the facts are state-grain and
join a state-grain conformed geography dimension. Dimensions that no fact
references should not be published as though they were live star-schema edges.

This is where grain discipline earns its keep: joining a state-grain fact to a
county-grain dimension on `state` alone silently multiplies every fact row by
that state's county count. Nothing errors. The KPIs are simply wrong.

**Aside — what's implemented, and the bug this caught.** The model is real
(`src/lakehouse/gold/`), but it shipped with exactly the defect above. Both
`fact_catastrophe_event` and `fact_regional_alert_activity` joined the
county-grain `dim_geography` on `state`. I reproduced it: **one Texas
declaration became 254 fact rows**, inflating KPI-3's declaration count and
KPI-1's region counts by each state's county count.

The existing tests missed it because every fixture used exactly one county per
state — a fixture that structurally cannot detect fan-out.

Fixed by adding state-grain `dim_geography_state`, repointing all three facts,
and rewriting every join in `reporting_views.sql` and
`kpi_validation_queries.sql` — the SQL would otherwise have re-introduced the
same fan-out at query time. Regression tests now use multi-county fixtures.

`dim_event_type` and `dim_alert_status` are built and published but referenced
by no fact. They are staged conformed dimensions, and the docs now say so
rather than claiming all four dimensions join the facts.

---

## 5. Orchestration

**Ideal.** Two schedules, because the sources have two different cadences:
a daily batch (FEMA/NOAA publish daily-or-slower, so 15-minute runs would
re-read unchanged files) and a tight loop for NWS alerts, which are volatile and
unreconstructable. The alerts job should *not* chain into Silver→Gold; the daily
batch picks up whatever snapshots landed.

Jobs run a versioned wheel, not workspace-synced source, so a deploy is a
promotable artifact.

**Aside — what's implemented.** `infra/jobs.tf` defines exactly these two jobs,
and `main.py`'s `--source historical|nws_alerts_snapshots` split implements the
separation. Stage runners wrap work in `log_stage_run(...)` for structured run
logging.

The known weakness, tracked in `productionization_next_steps.md`: alerts refresh
every 15 minutes but `claims_surge_risk` only recomputes daily, so the
highest-weighted input to the headline KPI is fresher than the KPI itself.

**Both jobs are currently disabled** (`enable_scheduled_jobs = false`). The
alerts job's 15-minute schedule would preempt the cluster's 30-minute
autotermination, pinning compute ~24/7 — a real cost trap that is easy to miss
reading `jobs.tf` and `compute.tf` separately.

---

## 6. Compute

**Ideal.** A small autoscaling job cluster per workload, sized to the data, with
autotermination. Interactive/BI queries go to a serverless SQL warehouse that
auto-stops, so cost tracks usage rather than uptime.

**Aside — what's implemented, and the constraint that forced it.** This is the
largest divergence, and it was not a design choice — it was discovered by
applying.

The config asked for a single-node `Standard_DS3_v2` cluster. Three failures, in
order:

1. **`NO_ISOLATION or custom access modes are not allowed`** — the
   `singleNode` Spark profile implies the legacy `NO_ISOLATION` access mode,
   which a Unity Catalog workspace rejects. Fixed with
   `data_security_mode = "SINGLE_USER"`.
2. **"The VM size you are specifying is not available"** — `Standard_DS3_v2`
   is no longer offered in eastus2 at all; the DS/Dv3 families have aged out in
   favour of v6/v7.
3. **`Node type ... is not supported`** — Databricks maintains its own
   allowlist, separate from what Azure offers. The v7 node I picked was
   available in Azure and rejected by Databricks.

The intersection turned out to be empty: every Databricks-supported node type is
either absent from eastus2 or returns `NotAvailableForSubscription` on a Free
Trial. That reason code is a **subscription-tier capacity restriction, not a
quota ceiling** — the vCPU limit was 4 with 0 in use, so a quota increase would
not have helped.

**The workaround is serverless.** Serverless SQL runs on Databricks-managed
capacity rather than VMs allocated to the subscription, so it bypasses the
restriction entirely. `enable_verification_cluster = false` skips the VM cluster;
the serverless warehouse provides Unity Catalog query access instead.

The lesson worth stating in an interview: `terraform validate` passed cleanly
through all three of these. Validation checks syntax and references; it cannot
tell you a VM family was retired from a region or that your subscription tier
cannot allocate it. Only an apply finds that.

---

## 7. Serving

**Ideal.** Power BI connects through version-controlled SQL views rather than
queries embedded in the `.pbix`, so dashboard logic is reviewable and diffable.
DirectQuery against a serverless warehouse keeps the dashboard current without a
separate refresh schedule.

**Aside — what's implemented.** `docs/reporting_views.sql` defines five views,
all now joining `dim_geography_state` on `state_geography_key`. Two corrections
made during this audit: the usage guide claimed six views, and claimed
DirectQuery made the dashboard reflect the 15-minute alert refresh — it does not,
because the views read Gold tables that recompute daily. The same file's own
"Refresh cadence" section had it right.

The `.pbix` itself does not exist. It is a manual Power BI Desktop step against
these views, which this repo cannot author.

---

## 8. Local validation (Docker Compose)

**Ideal.** Pipeline logic should be validatable end-to-end — real Delta reads
and writes across every stage, not just mocked-Spark unit tests — without
requiring a provisioned Azure subscription. Useful both for a contributor
without Azure access and as a fast CI check that catches drift in the
Databricks-vs-local assumptions `main.py` makes.

**Aside — what's implemented, and what was tried first.** A Docker Compose
stack (`Dockerfile`, `docker-compose.yml`) runs the exact same `lakehouse`
console script against a local `SparkSession`, with Delta's catalog/extension
config supplied via a mounted `docker/spark-defaults.conf` rather than
Databricks' ambient cluster config — `main.py` itself needed zero code
changes; Spark reads that file via `SPARK_CONF_DIR` regardless of how the
Python builder is constructed.

Storage is a **plain Docker volume** (`file://`), not an Azure emulator. That
wasn't the first thing tried. Azurite (Microsoft's official Azure Storage
emulator) was set up and tested directly, on the reasoning that a real
emulator in the stack would be a more honest "simulated Azure" than a bare
filesystem path:

- **Blob protocol (`wasbs://`, `hadoop-azure`'s WASB driver):** connects,
  authenticates, and creates containers successfully, but hangs indefinitely
  on Delta's own container-metadata check (`DeltaTable.isDeltaTable` →
  `checkContainer` → `downloadAttributes`) — a thread dump showed the Azure
  Storage SDK (7.0.1, bundled with this `hadoop-azure` version) stuck in its
  own internal retry-backoff loop *before* ever issuing the HTTP request
  (confirmed by Azurite's own access log showing zero incoming requests
  during the hang). This reproduced consistently across fixes to DNS
  aliasing, HTTP-vs-HTTPS mode, and JVM entropy configuration, and reads as a
  genuine compatibility gap between this old SDK and this Azurite version,
  not a configuration mistake.
- **ABFS protocol (`abfs://`, the modern Data Lake Gen2 driver):** fails
  fast and cleanly instead of hanging — with `"This endpoint does not
  support BlobStorageEvents or SoftDelete"` (HTTP 409). Checking Azurite
  3.36.0's own source directly (not just its error message) confirmed there
  is no hierarchical-namespace/Gen2 toggle anywhere in it: ABFS is
  architecturally unsupported in this Azurite version, not a config gap.

Since production's real storage account is ADLS Gen2 (`is_hns_enabled =
true`, `abfss://` addressing — see `infra/main.tf`), and Azurite can't
faithfully emulate that regardless of which driver is used, a plain local
volume was the more honest choice: it claims only "storage root is
swappable via `LAKEHOUSE_STORAGE_ROOT`," not "this behaves like ADLS Gen2."

This does **not** validate: Unity Catalog access control, ADLS Gen2
hierarchical-namespace semantics, the wheel-build-and-upload deploy path
(`infra/README.md`), real cluster autoscaling/cost behavior, or the two
scheduled jobs' cron-triggered execution.

**A real, separate finding surfaced along the way.** Running the full
pipeline against live data in this local stack (not a Docker problem —
identical behavior confirmed outside Docker too) exercises the existing DQ
gates for real, and both trip: FEMA has genuine duplicate
`(disasterNumber, designatedArea)` keys in the live OpenFEMA data (24 groups
found), and NOAA's `STATE` column always includes non-US marine zones (e.g.
`"GULF OF MEXICO"`, `"LAKE MICHIGAN"`) that `_map_state_to_usps` maps to
`null` against a Silver schema that declares `STATE` `nullable=False` — so a
full year of real NOAA data has apparently never actually passed this gate.
See `docs/data_limitations.md` for both, and the CI `docker-smoke` job's
scope note in `.github/workflows/ci.yml` for why the smoke test avoids them
rather than treating them as Docker bugs.

---

## Summary: divergences and why

| Area | Ideal | Actual | Why |
|---|---|---|---|
| Ingestion | Incremental, watermarked | **Implemented** — per-source watermarks + tombstone detection | Closed during this audit; see `data_limitations.md` for the remaining scale trade-offs |
| Silver DQ | Gates promotion, quarantines | Records results only | Scope; documented |
| Dedup | All sources | NOAA only | FEMA/geography assumed unique |
| Gold grain | State-grain facts + conformed dims | **Fixed during this audit** | Was a real 254x fan-out bug |
| Complaint data | Real DOI/OIR feed | Empty table + documented proxy | No structured public source exists |
| Jobs | Two schedules, running | Defined, disabled | 15-min job would pin compute 24/7 |
| Compute | Job cluster + serverless SQL | **Serverless only** | Free Trial cannot allocate any supported node type |
| Serving | Views + `.pbix` | Views only | `.pbix` is a manual desktop step |
| Local dev/CI validation | Full Azure apply for every validation | Docker Compose + local volume (Azurite tried, rejected — storage protocol differs from ADLS Gen2) | Enables validation without an Azure subscription; Azurite's WASB driver hangs against this SDK, its ABFS driver has no Gen2 support at all |

The pattern worth noticing: the gaps that are *documented* were scope decisions,
and the one that was *undocumented* — the geography fan-out — was the actual bug.
That is usually how it goes.
