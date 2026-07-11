# The replay determinism contract

> What `clink replay` guarantees, what it approximates, and why - the contract behind "any moment of a production job is reproducible".

## The model

The flight recorder captures, per operator per checkpoint epoch, the ordered EVENT stream the runner handed the operator: data records, watermarks (with idleness), and the clock positions at which due processing-time timers fired (`.cap` format v2 - see [fault-tolerance-and-rescale.md](./fault-tolerance-and-rescale.md)). Replay rebuilds the operator from its `op.json` sidecar, restores its keyed state and timers from checkpoint N-1, puts its `TimerService` on a manual clock, and feeds the events back through the production paths: data and watermarks via `process()`, captured clock positions via `fire_due_timers`.

Determinism therefore rests on three pillars:

1. **The input is total.** Everything that influences a single-input operator between two checkpoints - records, watermarks, timer-fire positions - is in the epoch file, in observed order.
2. **The clock is owned.** The operator's `TimerService` `NowFn` is the one sanctioned processing-time source. Its production default is the wall clock; under replay (and the `clink::test` harnesses) a manual clock governs it. The window operators and the watermark assigner read their processing time through it when a runtime is attached, so no operator on the replay path consults the wall clock directly.
3. **The state is exact.** Checkpoint N-1 restores through the same snapshot/restore cycle a recovering job uses, timers included.

## Guarantees

**Replay-replay determinism (the `--verify` gate).** Two replays of the same epoch from the same snapshot are byte-identical: same emissions, same order. `clink replay --verify` runs the epoch twice and byte-compares; exit 0 is the pass. This holds for every supported operator, windowed and timer-driven included, because the feed is single-threaded and the clock is manual.

**Live-replay equivalence.** For an untruncated v2 epoch, the replayed emissions equal what the operator emitted in production between checkpoints N-1 and N:

- Event-time behaviour (window fires, event-time timers) is exact - watermarks replay at their true stream positions.
- Processing-time timer FIRES are exact in stream position: the captured clock event moves the manual clock to the production firing time before firing, so timestamp-carrying fire output matches too.

## Approximations (documented, deliberate)

- **Registration-time clock reads.** An operator that reads `now_ms()` while PROCESSING a record (e.g. to register a timer at `now + delay`) sees the replay clock, which sits at the last captured fire position, not the production ingest time. Fires still happen at the captured positions - the semantics reproduce - but an operator that EMBEDS a raw clock reading in its output will not byte-match live-vs-replay (replay-vs-replay remains identical). None of the SQL frontend's operators do this.
- **Truncated epochs** replay the stored prefix (data past `capture_records` is dropped; the watermark spine survives its own 4x budget). The header records truncation and the true count, and the tool says so - a sampled replay is never silently presented as complete.

## Out of scope (rejected loudly, not silently wrong)

- **Multi-input operators (joins).** The runner's two-input interleaving is not captured today; `clink replay` rejects them with a clear message. Their inputs' captures upstream and their output's capture downstream still bracket them for forensics.
- **Non-Row channels.** Replay's operator rebuilding is wired for the SQL frontend's row-channel factories.
- **v1 captures** (records only, written by older builds) replay data-only: per-record operators replay exactly; watermark-driven fires do not occur. The tool prints a note.

## Cross-version A/B

Determinism is what makes version comparison meaningful: if replay were noisy, a diff between two builds would be noise too. `clink replay --out=<file>` dumps a run's emissions; `--plugin=<so>` rebuilds the operator from a candidate build first (ABI-gated like a cluster deploy); `clink replay-diff <a> <b>` then reports `identical` or the exact differing emissions. Same epoch, same state, two builds - the behavioural delta of a change on real production bytes, before it deploys.

## Incident to regression test

`clink replay --emit-test=<dir>` freezes an epoch into a self-contained bundle (capture + starting state + golden emissions + manifest) with a generated gtest whose body is one call to `clink::sql::run_replay_regression` - determinism is what makes the golden meaningful: the bundle passes forever on a correct build and locates the first divergence on a regressing one.

## The executable form

`tests/test_replay_cli.cpp` runs a windowed SQL job with capture armed, replays the tumbling-window operator's epoch through the CLI, and requires: the replayed emissions to equal the live sink output; `--verify` to pass (single-op and whole-job); two `--out` dumps to `replay-diff` as identical; a doctored dump to diff as different with the emission located; and an emitted regression bundle to pass through `run_replay_regression` (and fail, divergence located, once its golden is doctored) - the contract, enforced in CI.

## Related

- [fault-tolerance-and-rescale.md](./fault-tolerance-and-rescale.md) - the flight recorder and `clink replay` mechanics
- [state-snapshot-format.md](./state-snapshot-format.md) - the open snapshot format replay restores from
- [testing-framework.md](./testing-framework.md) - the manual-clock substrate shared with the harnesses
