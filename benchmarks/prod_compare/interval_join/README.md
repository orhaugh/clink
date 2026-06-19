# interval-join bench

Exercises two-input streams via `connect_process` + `KeyedCoProcessFunction`.

## Status (v1, 2026-06-01)

**Scaffolded but not yet functional end-to-end.** Both the clink-side
.so and the Flink-side jar build cleanly; the Flink side runs to
completion. The clink-side hits a planner gap when JM tries to
validate a plan with two source operators feeding into a `connect_process`
operator: the JM-side plan_job lookup returns "no source factory
registered" for the first source even though `define_job` registered
both sources successfully.

Two suspected fix paths, both deferred:

1. JM-side dlopen of the .so populates JM's RunnerRegistry; for
   single-source pipelines this works, but a two-source connect_process
   chain may not be re-running define_job on the JM side, or the
   second source registration is overwriting the first in JM's view.

2. The planner's chain-fusion logic at par=1 (`CLINK_PLAN_FUSE_PAR1=1`)
   refuses chains where a sink follows a multi-input op, throwing
   "chained ops must all be Operator kind". Disabling fusion gets
   further but hits the source-factory lookup gap above.

For v2: either pivot to a single-source emit-tagged-union pattern
(loses the literal two-stream test) or fix the planner gap.

## Workload (intended)

Two synthetic event streams keyed by user_id:

- **Order**: 5M `Order` events, 1000 keys.
- **Payment**: 5M `Payment` events, same keys, ts offset +50ms.

A `KeyedCoProcessFunction` maintains `ValueState<Order>` per key; on
Payment arrival it looks up the latest Order and emits a Joined
record. v1 has no time-window enforcement; v2 will layer event-time
bounds and broadcast UserProfile enrichment.

## Why it matters

Two-input + cross-stream state lookup is the canonical CDC-into-fact-
table pattern in stream analytics. The bench is the smallest artifact
that exercises that surface end-to-end.

## What it would verify (once fixed)

- Two-input operator wiring (Order ⊕ Payment).
- ValueState read+update from both sides of a CoProcessFunction.
- Cross-stream emit logic and downstream sink fan-in.
- Watermark merging across the two inputs.
