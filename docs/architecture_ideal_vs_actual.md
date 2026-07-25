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

The gap is incrementality: every run is a full re-fetch appended to Bronze.
At this data volume that is fine and simpler (KISS), but on a real FEMA history
it would waste bandwidth and grow Bronze without bound. Watermarking is the
first thing I would add.

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

## Summary: divergences and why

| Area | Ideal | Actual | Why |
|---|---|---|---|
| Ingestion | Incremental, watermarked | Full re-fetch each run | Scope; fine at this volume |
| Silver DQ | Gates promotion, quarantines | Records results only | Scope; documented |
| Dedup | All sources | NOAA only | FEMA/geography assumed unique |
| Gold grain | State-grain facts + conformed dims | **Fixed during this audit** | Was a real 254x fan-out bug |
| Complaint data | Real DOI/OIR feed | Empty table + documented proxy | No structured public source exists |
| Jobs | Two schedules, running | Defined, disabled | 15-min job would pin compute 24/7 |
| Compute | Job cluster + serverless SQL | **Serverless only** | Free Trial cannot allocate any supported node type |
| Serving | Views + `.pbix` | Views only | `.pbix` is a manual desktop step |

The pattern worth noticing: the gaps that are *documented* were scope decisions,
and the one that was *undocumented* — the geography fan-out — was the actual bug.
That is usually how it goes.
