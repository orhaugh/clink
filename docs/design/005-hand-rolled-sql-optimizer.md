# 005: The SQL optimizer is hand-rolled, not adopted

**Status:** adopted; the optimizer is cost-based and deliberately
conservative.

## Context

When the SQL frontend grew multi-way joins, the planner needed real
optimisation: join reordering, predicate pushdown, projection pushdown.
The build-versus-adopt question was researched properly before writing one.

The adoption survey found no good fit. General-purpose optimizer frameworks
are built around batch-relational assumptions and bring a large dependency
surface; no standalone C++ optimizer designed for streaming plans - where
operators are stateful, unbounded, and windowed, and where a "cheaper" plan
must also respect watermark and state semantics - existed to adopt. Bridging
one across a language boundary would have put a foreign framework in the
middle of every query compile.

## Decision

Write the optimizer in-tree, scoped to what streaming plans actually need:

- Predicate pushdown and projection pushdown into the source's column hints.
- Statistics via `ANALYZE TABLE`: exact row counts, per-column NDV,
  histograms, and most-common values written into the catalog.
- Cost-based multi-way join reordering with dynamic-programming order
  selection, grounded in those statistics.
- A conservatism rule: a rewrite is applied only when the cost model says it
  is cheaper; a scan with no declared statistics is flagged in `EXPLAIN`
  rather than silently guessed at. `EXPLAIN` always prints the plan that
  would actually run, with per-node row estimates.

## Consequences

- The optimizer's assumptions match the engine's: it reorders within the
  semantics the streaming operators guarantee, and it can grow
  streaming-specific rules (state-size awareness, disaggregation decisions)
  without fighting a framework built for batch.
- No new heavyweight dependency in the compile path; query compilation stays
  a small, inspectable pipeline (parse, bind, optimise, lower).
- The trade-off accepted: every future optimisation is ours to build and
  test - there is no upstream rule library to inherit. The conservatism rule
  bounds the blast radius of mistakes: a missing statistic degrades to the
  unoptimised plan, not to a wrong or pathological one.

See [SQL frontend](../internals/sql-frontend.md) for the full
parse-to-operators pipeline.
