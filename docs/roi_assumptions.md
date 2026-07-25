# ROI Assumptions

The Power BI dashboard's ROI section ("what does earlier catastrophe signal
detection save?") is built entirely from named, adjustable constants — no
number on the dashboard should be unexplainable. This doc is the source of
truth those constants trace back to; update it, not the dashboard, when
assumptions change.

## The formula

```
estimated_roi(region, period) =
    avoided_backlog_days(region, period) * cost_per_adjuster_day_of_delay
```

Where:

```
avoided_backlog_days(region, period) =
    baseline_days_to_declaration - actual_avg_days_to_declaration(region, period)
```

`actual_avg_days_to_declaration` comes directly from
`gold.v_roi_summary.avg_days_to_declaration` (KPI 3). `baseline_days_to_declaration`
and `cost_per_adjuster_day_of_delay` are the two named assumptions below.

## Named assumptions

| Constant | Value | Justification |
|---|---|---|
| `baseline_days_to_declaration` | 14 days | Meridian's *pre-pipeline* reactive baseline, per `docs/scoping_memo.md`: "staffing follows the backlog rather than anticipating it." Set to a round two-week figure representing fully-reactive staffing with no early-warning signal; replace with Meridian's actual historical average once available. |
| `cost_per_adjuster_day_of_delay` | $2,500/day | Illustrative placeholder combining (a) overtime/rush-staffing premiums for late-mobilized adjusters and (b) estimated incremental claims-handling cost from cycle-time extension (each day of delay compounds backlog for *every* claim in the region, not just one). **Not sourced from real Meridian financials** — this is a fictitious client (`docs/charter.md`), so this number is a stand-in a real engagement would replace with the insurer's actual loss-adjustment-expense data. |

## Why this is deliberately conservative

`avoided_backlog_days` only credits days saved *relative to a fixed
14-day baseline* — a region that was already fast (say, 5-day average
declarations) shows a smaller ROI than one that improved from 20 days to 10,
even though both saved 10 days of *relative* delay. This avoids double-
counting: the dashboard is meant to show where earlier signal detection
*changes behavior*, not to inflate ROI for regions that were already well-run.

## What would make this real

Per `docs/productionization_next_steps.md`, replacing
`cost_per_adjuster_day_of_delay` with Meridian's actual loss-adjustment
expense (LAE) data is the single highest-value fix to this section — it's
currently the one dashboard number that isn't traceable to a real Phase 2
source column, and should be labeled as illustrative in any interview
walkthrough of the dashboard until that's fixed.
