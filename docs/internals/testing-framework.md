# The testing framework (clink::test)

> The public, supported API for unit-testing user code built on clink - stateless functions, stateful and time-aware operators, and (as the framework grows) complete pipelines - deterministically, in-process, in milliseconds.

## Overview

`clink::test` lets library consumers test their functions and operators without a cluster, without threads, and without the wall clock. The design principle is **fidelity by construction**: the harness does not mock the engine - it composes the same production pieces an operator runner would (a real `RuntimeContext` over an in-memory state backend, the operator's own `TimerService` with an injected manual clock, the engine's `Emitter`) and drives the operator through its real hooks. Event-time timer firing *is* the operator's own `on_watermark` path; processing-time firing replicates the runner's between-pops `poll_due`. What a test observes is what production does.

Link the `clink::test_support` CMake target from test executables only; production binaries must not depend on it. Everything lives in `namespace clink::test` under `include/clink/test/`.

## Where it lives

| Path | What |
| --- | --- |
| `include/clink/test/output_capture.hpp` | `OutputCapture<T>`: the typed emission log + a real `Emitter<T>`; doubles as the stateless-function collector |
| `include/clink/test/one_input_harness.hpp` | `OneInputOperatorHarness<In, Out>`: lifecycle-enforced single-input operator harness with manual time, snapshots and failure injection |
| `include/clink/test/keyed_harness.hpp` | `KeyedOneInputOperatorHarness<In, Out, K>` (typed state inspection, key-scoped timers), `default_codec<T>`, and the `ProcessFunction` factories |
| `include/clink/test/two_input_harness.hpp` | `TwoInputOperatorHarness<In1, In2, Out>` and its keyed variant: `CoOperator` testing with the engine's real watermark combination |
| `include/clink/test/failure_injection.hpp` | `FailurePlan`, `FailurePoint`, `InjectedFailure`: deterministic failure injection at the harness's mediation points |
| `include/clink/test/sources_and_sinks.hpp` | `TestSource<T>` (scripted, checkpointable), `CollectSink<T>`, `FailingSink<T>`, `TransactionalTestSink<T>` (2PC lifecycle recorder) |
| `tests/test_harness_framework.cpp` | The framework's own contract tests |

## Testing a stateless function

Anything that emits through an `Emitter<T>` is testable with just a capture - no harness, no runtime:

```cpp
clink::test::OutputCapture<std::int64_t> cap;
my_flatmap.process(element, cap.emitter());

EXPECT_EQ(cap.values(), (std::vector<std::int64_t>{1, 10}));
```

`OutputCapture` stores the engine's own `StreamElement<T>` events in emission order and projects them:
`values()` (data flattened across batches), `records()` (values with event times), `watermarks()`, `barriers()`, `value_count()`, `empty()`, `any_value(pred)`, `count_values(pred)`, `clear()`, `take_events()` (drain for phase-by-phase assertions).

## Testing an operator

```cpp
auto h = clink::test::OneInputOperatorHarness<In, Out>::create(MyOperator{});
h.open();

h.process_element(in);              // un-timestamped
h.process_element(in, 1000);        // event time (ms)
h.process_batch(batch);             // batch-first, like the engine
h.process_watermark(2000);          // production path: fires due event-time timers, then forwards
h.advance_processing_time_to(5000); // manual clock + due processing-time timers
h.flush();                          // end-of-input residuals (windows, sorts)

EXPECT_EQ(h.output_values(), expected);
h.close();                          // or let the destructor close
```

- **Lifecycle is enforced**: processing before `open()` or after `close()` throws `std::logic_error` with a clear message; double open/close throws; the destructor closes an open operator (best-effort - assert teardown explicitly via `close()` when it matters).
- **Time is manual**: `processing_time_ms()`, `set_processing_time()` (position without firing), `advance_processing_time_to/by()` (fire due timers). Time never moves backwards. Nothing in the framework reads the wall clock.
- **Timer determinism**: due timers fire in `(timestamp, key)` order - lexicographic key order breaks timestamp ties. A timer registered during a fire is deferred to the next advance (production `poll_due` semantics, preventing starvation).
- **Inspection**: `output()` (the capture), `current_watermark_ms()`, `processing_time_timers()` / `event_time_timers()` (non-destructive, in firing order), and escape hatches to the real pieces - `runtime()`, `state_backend()`, `metrics()`, `op()`.

`Options` configures the operator name/id, the clock's starting point, and an overriding state backend.

## Testing keyed, stateful functions

`make_keyed_process_function_harness` builds a harness over a `KeyedProcessFunction` through the same adapter the production fluent API uses; `make_process_function_harness` does the non-keyed equivalent. The keyed harness adds typed state inspection over the production read/write paths:

```cpp
auto h = clink::test::make_keyed_process_function_harness(
    CountPerUser{},
    [](const Purchase& p) { return p.user; },                 // key selector
    [](const std::string& timer_key) { return timer_key; });  // timer key -> K (needed iff timers)
h.open();
h.process_element(Purchase{"alice", 10}, 1000);

EXPECT_EQ(h.state_value<std::int64_t>("alice", "count"), 1);  // production read path
h.seed_state<std::int64_t>("bob", "count", 41);               // arrange-phase setup
EXPECT_TRUE(h.has_event_time_timer(2000, "alice"));           // key-scoped timer query
```

Codecs resolve through `clink::test::default_codec<T>` (`std::string` and `std::int64_t` are pre-wired; specialise it for your own types, or pass codecs explicitly). `known_keys<V>(slot)` lists a slot's keys in the backend's key encoding order (key-group first) - sort before comparing. Raw operators keyed some other way get the same surface via `KeyedOneInputOperatorHarness<In, Out, K>::create`.

## Testing two-input operators

`TwoInputOperatorHarness<In1, In2, Out>` drives a `CoOperator` with the engine's real two-input watermark semantics: each per-input watermark feeds the production `MultiInputAlignment`, and the operator only sees the combined watermark - the running minimum over both inputs - when the aligner says it advanced. `process_left/right(v[, ts])`, `process_left/right_watermark(ts)` (returns the combined watermark delivered, or `nullopt` when the minimum did not move), `mark_left/right_idle()` (an idle input stops constraining the minimum; a rejoining one clamps to the emitted global watermark). `KeyedTwoInputOperatorHarness` adds the keyed-state inspection surface.

## Snapshots, restore and recovery testing

`h.snapshot(checkpoint_id)` captures the operator's state AND timers as a self-contained `HarnessSnapshot`: timers are serialised into the backend by the operator's own `snapshot_timers` (what the runner does at a barrier), then the backend snapshots. Restore either statically (`OneInputOperatorHarness::restore(op, snapshot)`) or on any created-but-not-opened harness - including the keyed subclasses and factories - via `restore_from`:

```cpp
auto checkpoint = h.snapshot(1);
// ... h diverges or "crashes" ...
auto h2 = clink::test::make_keyed_process_function_harness(CountPerUser{}, key_fn);
h2.restore_from(checkpoint);
h2.open();  // timers replay through restore_timers BEFORE open(), the runner's ordering
```

The restored harness has the snapshot's state behind the production read path and its timers registered and firing. The canonical recovery test is: process, snapshot, fail, restore into a fresh harness, replay the post-checkpoint input, assert the same result.

## Failure injection

Failures are injected at the harness's mediation points - deterministic, explicit and observable, with no hooks in production code. Arm a `FailurePlan` and the harness throws `InjectedFailure` at the armed point:

```cpp
h.failures().fail_once(clink::test::FailurePoint::BeforeProcessElement);
EXPECT_THROW(h.process_element(x), clink::test::InjectedFailure);
EXPECT_TRUE(h.output().empty());               // the operator never saw it
EXPECT_EQ(h.failures().injected_count(), 1);
```

Points: `BeforeProcessElement`, `AfterProcessElement` (the "crash after the effect" shape for replay/idempotence tests), `OnEventTimeTimer` / `OnProcessingTimeTimer` (before the fire - the timer stays registered, so a retry fires it), `DuringSnapshot`. Rules: `fail_once(point)`, `fail_on_nth(point, n)`, `fail_when(point, predicate)`.

The harness also keeps a lifecycle log of what it drove, in order - `h.transitions()` yields `"open"`, `"process"`, `"watermark"`, `"snapshot"`, `"close"` - for asserting lifecycle ordering without instrumenting the operator.

## Test sources and sinks

Deterministic stream endpoints implementing the engine's real `Source<T>`/`Sink<T>` contracts, so they compose with harnesses, the local runtime and the cluster alike:

```cpp
clink::test::TestSource<Event> src;
src.emit(e1, 1000).emit(e2, 2000).watermark(2500).emit(e3, 3000);
```

- **`TestSource<T>`** - a scripted, bounded source. `produce()` emits exactly one script entry per call, so checkpoint barriers can land between any two entries. Its cursor is checkpointable through the production `snapshot_offset`/`restore_offset` hooks: a restored source with the same script resumes after the last checkpointed entry - nothing re-emitted, nothing skipped - which is what makes exactly-once recovery testable.
- **`CollectSink<T>`** - collects records (with event times) and watermarks, thread-safely; keep a `shared_ptr` to it and read `values()`/`records()` after the pipeline runs.
- **`FailingSink<T>`** - accepts N records, then throws `InjectedFailure` once: the sink-side crash for recovery tests.
- **`TransactionalTestSink<T>`** - records the full two-phase-commit lifecycle: `on_data` fills the current epoch, `on_barrier` stages it under the checkpoint id (a terminal barrier also commits immediately, the engine's bounded-stream contract), `on_commit` promotes it (idempotently), `on_abort` discards it. `committed_values()` is exactly what an external system would durably hold; `pending_checkpoints()`, `uncommitted_values()`, `commits()` and `aborts()` expose the intermediate states.

## Design rules

- Deterministic: no sleeps, no polling loops, no wall clock.
- In-process: no threads, sockets or services in operator tests.
- Reuse over reimplementation: production semantics come from calling production code, never from a parallel mock runtime.
- Typed: the output model is the engine's own `StreamElement<T>`; no `void*`, no string-keyed lookups.
- RAII: the harness closes what it opened.

## Roadmap (implemented incrementally)

1. Foundation: capture, one-input harness, manual time, lifecycle - DONE.
2. Keyed harness (typed state inspection, key-scoped timers, key selectors) + `ProcessFunction` factory helpers - DONE.
3. Two-input harnesses (co-process, joins, connected streams) with the engine's real two-input watermark combination - DONE.
4. Snapshot/restore (the real backend + `snapshot_timers` cycle), failure injection, lifecycle log - DONE.
5. Test sources and sinks (scripted, replayable, transactional) - DONE.
6. `LocalTestEnvironment` (full pipelines over the local runtime) and `MiniCluster` (the in-process JM+TM fixture).
7. Assertions, sequence/property-testing support, compiling documentation examples, and migration of representative core tests onto the framework.

## Related

- [./operator-model.md](./operator-model.md) - the operator, emitter and stream-element model the harness drives
- [./time-and-windowing.md](./time-and-windowing.md) - watermarks and timers
- [./state-and-backends.md](./state-and-backends.md) - the state the keyed harness will expose
