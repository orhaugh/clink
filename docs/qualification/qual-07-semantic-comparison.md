# QUAL-07: semantic comparison - the same answers as a reference engine

Clink computes the same answers as an independent, widely deployed
open-source stream-processing engine: **19 of 19 queries agree at the
content level** - not row counts, the rows themselves - over one
deterministic dataset both engines consumed from the same Kafka topics.
Append-only queries compare as byte-exact multisets after minimal
normalisation; changelog queries compare as the final state their update
streams converge to, paired by declared key; every query's judgement
class was declared, with its reason, before any run.

| Provenance | |
|---|---|
| Campaign run | `qual07-runf`: all 19 queries, one 460,000-bid dataset (plus auction and person streams), parallelism 4 on both engines |
| Engine | clink revision `998e0ff` (host build); the reference engine containerised at its pinned release |
| Query set | the nexmark-derived cross-engine suite: projections, filters, string functions, tumble/hop/session/cumulate windows, COUNT DISTINCT, stream-stream joins, dedup-latest, top-N per key, unbounded GROUP BY |
| Judgement | per-query declared classes: 15 append (sorted multiset, byte-exact), 4 materialised (upsert changelog reduced to final state, compared by key); float tolerance 1e-6 declared on 4 queries for rendering only |
| Scale | from 49 window rows to 1,493,218 hop panes per query; 455,895 final images on the dedup query; 460,000-row per-record queries byte-exact |

## What "agree" means here

Row counts can agree while every value is wrong, so this campaign
compares content under the strictest class each query's semantics allow:

- **Append** queries (windows that fire once, per-record
  transformations, insert-only joins): both drained sinks are
  normalised (JSON field order, integer-valued float collapse - nothing
  else), sorted, and compared as multisets. Duplicates count; a corrupt
  line fails the run rather than being skipped.
- **Materialised** queries (unbounded GROUP BY, dedup-latest, top-N -
  where emit cadence is legitimately implementation-defined): each
  engine's upsert topic is reduced to its final state (last write per
  key, tombstones resolved) and the states compare by the declared key.
  The two engines encode broker keys incompatibly, so rows pair by key
  columns extracted from the values.
- **Tolerance** never hides a numeric difference: every epsilon is 1e-6
  and covers only decimal rendering of IEEE-identical doubles (the
  declared fields are per-record products or means over integer-valued
  doubles far below 2^53, so the values are order-independent).
- A submit, settle or drain failure is **not gated** - never a pass. An
  empty side agrees with nothing. A judgement class downgraded after
  seeing a diff would make the whole run inconclusive by rule.

## What the campaign forced out

The comparison paid for itself before it went green - four engine fixes
and two premise reconciliations, every one invisible to row-count gating:

1. **Sinks wrote the row's internal schema, not the table's declared
   one.** Windowed aggregates leaked their synthetic window bounds into
   the sink JSON (seven queries); an unaliased SELECT expression reached
   the payload under a binder-synthesised column name. Fixed: every
   Row-channel sink binds the SELECT's outputs positionally to the
   declared columns.
2. **Cancel could fire open windows.** A cancelled job appended a
   nondeterministic partial tail of open window panes - correct-valued,
   premature, exactly the kind of plausible-looking wrong output an
   exactly-once consumer would trust. An independent oracle computed
   from the raw dataset pinned it: the reference equalled
   oracle-truncated-at-watermark exactly; clink matched no truncation.
   Fixed in three layers, each caught by a rerun: channel closes carry
   a Finished/Cancelled reason end to end (including across the wire);
   relay sources forward their feed's cancellation; and every runner's
   end-of-stream ceremony (timer fire + flush) obeys the input's close
   reason, not just its own stop token.
3. **Two dialect premises were untotal or lossy** and were reconciled in
   the shared query definitions before publication: the reference's
   integer AVG truncates (cast added, matching the intent), and a
   dedup-latest ORDER BY left same-millisecond ties with no defined
   winner (tie-breakers added).
4. **A 1-millisecond watermark-convention difference**: the reference's
   bounded-out-of-orderness watermark is `maxTimestamp - lag - 1ms`,
   clink's is `maxTimestamp - lag`. Aligned windows never see it; one
   singleton session's fire point landed exactly in the gap. Both
   engines had fired an exact oracle prefix by fire point - the session
   logic agreed perfectly - so this is declared and reconciled as a
   premise, not a defect.

## What this qualifies

- **Demonstrated:** content-level agreement with an independent
  reference implementation across 19 queries covering every window
  kind, joins, dedup, top-N, DISTINCT aggregation and per-record
  transformations, at revision `998e0ff`, judged under pre-declared
  per-query classes with zero not-gated queries.
- **Tested but bounded:** one dataset shape (460k bids, 4 partitions,
  4s watermark lag), parallelism 4, JSON-over-Kafka sinks; the two
  official nexmark queries with tiny join inputs run in the row-count
  gate harness, not here.
- **Architecturally supported but not qualified:** the same comparison
  under fault injection (the exactly-once campaigns cover faults; this
  campaign ran fault-free), and at higher parallelisms.
- **Unknown:** agreement on SQL surfaces outside this query set
  (interval joins, MATCH_RECOGNIZE, OVER aggregates).

## Caveats

- Agreement is asserted for the declared judgement classes on this
  dataset; the reference engine is one pinned release of one
  implementation, and a reference-engine defect surfacing as a
  divergence would be reported as such, not silently absorbed.
- Deliberate dialect differences (argument order of windowing
  functions, timestamp representations, the reconciled premises above)
  live in one generated query definition, so any drift between the two
  engines' SQL is visible in a single file.

Raw evidence (all 38 drained outputs, per-query verdicts with sample
divergences from the failing runs, the campaign logs, and the oracle
scripts used in diagnosis) is retained for the green run and for the
five preceding runs whose findings drove the fixes; every number above
is taken from it.
