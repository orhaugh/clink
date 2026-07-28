# Comprehensive window test plan

Windows are where clink's correctness is most exposed and was least tested. This
plan enumerates what "comprehensively tested" means for them, why each item earns
its place, and the order to build it in.

## Why this exists

A hopping window shipped with **no test anywhere**. Nexmark q5 was its only user,
q5 was new, and a query written with Flink's argument order reached deployment,
where the operator's constructor refused it. Because a task that fails to build
never closes its output channel, every downstream stage blocked, and the job
reported nothing until a watchdog fired. A rejected parameter presented as a
deadlock and cost a long investigation.

The test suite did not miss this for want of volume. It missed it along specific
axes, and each one generalises:

| axis | what was missed |
|---|---|
| feature | HOP had zero tests; SESSION and CUMULATE had almost none |
| execution path | the row path was covered, the **columnar** path was not - and columnar is what a Kafka/JSON source runs |
| parallelism | the unkeyed-aggregate bug is invisible at parallelism 1, and nearly everything ran at 1 |
| test emulating production | a harness re-implemented the fan-out loop, so 15 of 16 tests passed with **both** shipped bugs reverted |
| vacuous assertion | a gate compared 0 rows against 0 rows and passed |
| expectation provenance | expectations captured from the implementation would have locked the bugs in |
| rejection paths | invalid parameters were never asserted to be rejected |

The plan below is organised by those axes, because that is where the bugs were.

## The matrix

Four kinds, and they are not variations on one implementation: `WindowRowOp`
serves TUMBLE / HOP / CUMULATE, and SESSION is a separate class with its own
state and its own gap logic.

| kind | arguments | panes per record | state shape |
|---|---|---|---|
| TUMBLE | size | exactly 1 | one bucket per (key, window) |
| HOP | size, slide | `ceil(size / slide)` | overlapping buckets |
| CUMULATE | size, step | `size / step` | nested, sharing a start |
| SESSION | gap | 1, but buckets MERGE | mutable extent per key |

Every row below should hold for all four unless noted.

## Phase 1 - semantics, per kind, on both paths (largely DONE)

Driven directly against the operator built from the registry, no DAG involved, so
a failure is milliseconds and unambiguous. Both the row path and
`process_columnar` are exercised, because they are separate folds.

- [x] TUMBLE: one pane per (window, key)
- [x] TUMBLE: `window_end` is EXCLUSIVE - a record at the boundary lands in the next window
- [x] HOP: a record appears in every overlapping pane
- [x] HOP: only panes the watermark has passed fire
- [x] SESSION: merges within the gap, splits at or beyond it
- [x] CUMULATE: one cumulative pane per step
- [x] no window fires before the watermark passes its end
- [x] HOP where `slide == size` behaves exactly like TUMBLE (the degenerate case binds; assert it also *computes* the same)
- [x] CUMULATE where `step == size` behaves exactly like TUMBLE
- [x] SESSION: three records where the middle one bridges two that would otherwise split - the merge must be transitive
- [x] SESSION: a record arriving between two existing sessions must merge BOTH into one
- [x] every kind: two keys interleaved, so per-key state cannot leak between them
- [x] every kind: a key whose records all fall in one window, and a key spread across many

## Phase 2 - the argument surface (largely DONE)

Every invariant a constructor enforces must be refused at bind time, or a bad
query becomes a deployment failure instead of a compile error.

- [x] TUMBLE size = 0 rejected
- [x] SESSION gap = 0 rejected
- [x] HOP slide = 0 rejected
- [x] HOP slide > size rejected, with a message naming the argument order
- [x] CUMULATE size not divisible by step rejected
- [x] valid forms still bind (including `slide == size`)
- [x] negative values for every parameter
- [x] size or slide large enough to overflow when added to a timestamp near `int64` max
- [x] non-integer / non-literal arguments (an expression where a literal is required)
- [x] INTERVAL units: SECOND, MINUTE, HOUR all convert to the same milliseconds
- [x] a property test: for every kind, a randomly generated invalid parameter set is EITHER rejected at bind time OR builds successfully - never accepted at bind and refused at deploy

That last item is the one that would have caught this class outright, and it is
worth more than the individual cases above it.

## Phase 3 - the differential axes (NOT STARTED, highest remaining yield)

These are generated comparisons rather than hand-written expectations, which is
where coverage multiplies without the test count exploding.

- [x] **Path parity.** For each kind and a generated input, row path output ==
      columnar path output. No oracle needed: the two must agree.
- [x] **Parallelism parity.** Covered by the in-suite nexmark queries, which run at
      parallelism 4 through `cluster::apply_job_parallelism` against the oracle. This
      is the axis that hid an unpartitioned aggregate returning one answer per
      subtask.
- [x] **Batch-boundary invariance.** The same records delivered as one batch, as
      many single-record batches, and split randomly must give identical output.
      Window state is per record but the columnar fold groups per batch, so a
      batch-dependent result is a real bug this would catch.
- [x] **Watermark-granularity invariance.** One terminal watermark versus a
      watermark after every record must give the same final output - only the
      emission timing may differ.
- [x] **Degenerate-equivalence.** HOP with `slide == size`, CUMULATE with
      `step == size`, and TUMBLE of the same size must agree exactly.

## Phase 4 - time and lateness

- [x] a record later than the watermark by less than the allowed lateness is folded in
- [x] a record later than the allowed lateness is dropped, and counted as dropped
- [x] a window fires exactly ONCE even when late records keep arriving in band
- [x] timestamps at and around the epoch: negative window starts are clamped to 0
      today, which silently drops pre-epoch panes. Decide whether that is the
      intended contract, then pin it - my own oracle assumed the same clamp, so it
      has never actually been checked against another engine
- [x] timestamps near `int64` max: window_end arithmetic must not overflow
- [x] an out-of-order stream within the watermark bound produces the same result as
      a sorted one

## Phase 5 - state, scale and recovery

- [x] a window's state is purged after it fires (no unbounded growth) - assert
      state size returns to its floor after the terminal watermark
- [x] snapshot and restore mid-window: panes not yet fired must survive and fire
      after restore
- [x] a large keyspace fires without an O(groups) scan per watermark - the
      `earliest_win_end_` fast path has a debug assertion, so a test that exercises
      a window-creation site which forgets to maintain it must fail loudly
- [x] memory per (key, window) has a documented bound and a test that notices a
      regression, since a per-key state defect has shipped twice here. Two
      instruments, because the two costs differ: `static_assert`s on
      `sizeof(AggState/WindowBucket/Session)`, written in members so they hold on
      both standard libraries, forbid a second container going inline; and a test
      gates the SERIALISED bytes per pane, which a snapshot holds exactly. Resident
      memory is not deterministic enough for the suite and stays with
      `benchmarks/clink_window_state_bench`. Numbers in
      `docs/internals/time-and-windowing.md`
- [x] rescale: a windowed query changing parallelism across a restore. Scale-up
      filters one parent snapshot per new subtask, scale-down merges several
      through `combine_snapshots`; the union over the new subtasks must equal what
      the old parallelism fired. Covered for all four kinds at 1 -> 2, 1 -> 4,
      4 -> 2 and 4 -> 1

## Phase 6 - end to end and cross engine

- [x] one nexmark query per kind, in-suite, at parallelism > 1 against an
      independent oracle: q12 (TUMBLE), q5 (HOP), q11 (SESSION) exist; CUMULATE has
      no nexmark query and needs its own
- [x] the cross-engine gate covers each kind, with the argument-order difference
      handled per dialect rather than assumed. This entry originally read "TUMBLE and
      HOP are gated; SESSION and CUMULATE are not", and two of the four were wrong:
      SESSION was already gated by q11, while HOP's only query is q5, whose top-1
      rank makes it a changelog, and whose last upsert-gate run emitted zero rows on
      both engines. HOP and CUMULATE now have their own bare windowed aggregates,
      `qhop` and `qcum`, generated for both dialects. Run at 500k events,
      parallelism 4:

      | kind | query | clink rows | flink rows |
      |---|---|---|---|
      | TUMBLE | q12 | 184,767 | 184,767 |
      | SESSION | q11 | 73,468 | 73,468 |
      | CUMULATE | `qcum` | 678,007 | 678,007 |
      | HOP | `qhop` | 1,493,218 | 1,493,218 |
- [x] a job whose window cannot build fails FAST with the constructor's message
      (done - `TaskThatFailsToBuildFailsTheJobRatherThanHanging`)

## Order of work

1. **Phase 3** first, despite being last-written. Path parity and parallelism
   parity are ~100 lines of harness that generate dozens of comparisons and target
   the two axes that hid the worst bugs.
2. **Phase 2's property test**, which closes the bind-vs-deploy gap as a class
   rather than case by case.
3. **Phase 4**, because lateness is the least-tested behaviour that users will hit
   in production.
4. **Phase 1's remaining items** and **Phase 5**, which are valuable but narrower.
5. **Phase 6** last: it is the slowest to run and the most environment-dependent,
   and by then the fast tests should already have caught what it would.

## What this found

Working the plan surfaced eight defects, each in the phase written to look for it.

| defect | phase | severity |
|---|---|---|
| `HOP` discarded every pre-epoch record, and clamped away leading panes near the epoch | 3 | wrong answers |
| `TUMBLE` and `CUMULATE` shifted every pre-epoch record one window toward zero | 3 | wrong answers |
| the event-time column was read through a `double`, rounding any timestamp above 2^53 | 4 | wrong answers on nanosecond timestamps |
| window time arithmetic overflowed at the `int64` extremes | 4 | undefined behaviour on corrupt input |
| an open window did not survive a checkpoint restore | 5 | silent data loss on failover |
| an open SESSION did not survive one either - the fix above was written against the fixed-window class, and sessions are a separate one | 5 | silent data loss on failover |
| the nexmark oracle's q5 carried the engine's own clamp, so it agreed with the defect | 3 | the oracle was not independent |
| the cross-engine gate counted 0 rows against 0 rows as a MATCH, so a query that emitted nothing on both engines reported as gated | 6 | a gate that cannot fail |

Five of the eight were invisible to output tests over realistic data: epoch
milliseconds are around 1.7e12, comfortably inside both the `double` and the
`int64` safe ranges, and no benchmark checkpoints. They were reachable only by
asking what happens at the edges of the representation, which is what the
differential and extreme-value phases are for.

The session one is the clearest case for the "feature" axis at the top of this
page. The fix for it existed, in the class next door, with a test; sessions were
simply not enumerated when it was applied. A per-kind loop over
`all_window_kinds()` costs nothing and would have caught it the same day, which is
why the Phase 5 and rescale tests are written that way rather than against TUMBLE.

The gate one is the "vacuous assertion" axis again, at the other end of the plan
from where it was first written down. It is worth noting that the plan's own
account of what was gated was wrong in both directions until it was checked
against the recorded results, which is the general lesson: a claim about coverage
is not coverage.

The ninth finding is not a defect but a decision: `HOP(time, SIZE, SLIDE)` puts
the larger value first and `CUMULATE(time, STEP, SIZE)` puts the smaller first, so
the two multi-argument window kinds disagree with each other, and `HOP` also
disagrees with the same-named function in Flink. Both messages now name their order,
and bind tests pin both, but the orders themselves are unchanged.

## What "done" looks like

Not a test count. Three properties:

- Every window kind is exercised on every execution path it can take, at more than
  one parallelism, with expectations that do not come from the implementation.
- Every argument a user can get wrong is refused before a task is deployed, and
  the message says what to write instead.
- A failure anywhere in the window path surfaces as a failed job with a real
  message, never as a hang.
