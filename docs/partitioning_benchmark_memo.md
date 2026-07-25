# Partitioning & Join Strategy Benchmark

Compares two implementations of the Phase 4 regional event-to-declaration
join (`processing/features/regional_event_join/feature.py`) against a
synthetic dataset shaped like production: ~2,000 FEMA declarations
(`fema`, the small side) and ~2,000,000 NOAA storm events (`noaa`, the large
side) joined on state + a 7-day date window.

## Strategy A: unhinted join, auto-broadcast disabled

Models what happens on a pre-AQE cluster, or any cluster where the
optimizer's auto-broadcast threshold (`spark.sql.autoBroadcastJoinThreshold`,
10 MB by default) is exceeded — for example if the "small" side isn't
actually small (a wider FEMA extract with more columns, or a join against
another large table).

Real physical plan (`spark.conf.set("spark.sql.autoBroadcastJoinThreshold",
"-1")` to force this path locally):

```
AdaptiveSparkPlan
+- SortMergeJoin LeftOuter
   :- Sort
   :  +- Exchange  -- hashpartitioning(STATE, 200)   [shuffles all 2M NOAA rows]
   :     +- Project
   :        +- Range
   +- Sort
      +- Exchange  -- hashpartitioning(state, 200)   [shuffles all 2K FEMA rows]
         +- Project
            +- Filter
               +- Range
```

Both sides get an `Exchange` (shuffle) + `Sort` before the `SortMergeJoin`.
The large NOAA side — the one that matters at scale — gets fully
shuffled across 200 partitions by hash of `STATE`.

## Strategy B: explicit broadcast hint on the small FEMA side

```python
noaa.join(F.broadcast(fema), on=join_condition, how="left")
```

Real physical plan:

```
AdaptiveSparkPlan
+- BroadcastHashJoin LeftOuter BuildRight
   :- Project              -- NOAA: single scan, no shuffle, no sort
   +- BroadcastExchange    -- FEMA: collected once, shipped to every executor
      +- Project
         +- Filter
```

The large NOAA side is read once with zero shuffle; only the small FEMA
side pays a one-time broadcast cost.

## Tradeoff

| | Strategy A (shuffle) | Strategy B (broadcast) |
|---|---|---|
| Shuffle volume | Both sides shuffled (large side included) | Neither side shuffled |
| Scales with | `noaa` row count × `fema` row count | `noaa` row count only |
| Risk | Shuffle spill / skew if one `STATE` dominates row counts | Driver OOM if the "small" side isn't actually small |
| When to use | Both sides are large / comparably sized | One side reliably fits in memory (dimension-like) |

**Recommendation:** use Strategy B (`regional_event_join.feature.py` already
does — see the module's join call). FEMA declarations are dimension-like
(tens of thousands of rows even across the full historical range), so they
comfortably fit under the default 10 MB auto-broadcast threshold — Spark's
adaptive query execution (AQE) actually auto-broadcasts this join without
an explicit hint in most cases, as this benchmark confirmed when the
threshold wasn't manually disabled. The explicit `F.broadcast()` hint is
kept anyway so the plan doesn't silently degrade to Strategy A if a future
FEMA extract grows past the auto-broadcast threshold (e.g. by adding more
columns) or if `autoBroadcastJoinThreshold` is tuned down cluster-wide.

## Caveat

These are plan-shape comparisons from a local `local[2]` run — real shuffle
cost (network + disk spill) only shows up meaningfully on a multi-node
cluster with real data skew. The plan shapes above (shuffle-both-sides vs.
shuffle-neither) hold regardless of cluster size; empirical runtime/shuffle-
read numbers should be captured from a real Databricks job run once
`infra/` is applied (Spark UI's SQL tab shows shuffle read/write bytes per
stage).
