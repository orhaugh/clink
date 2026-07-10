# The testing framework (clink::test)

> The public, supported API for unit-testing user code built on clink - stateless functions, stateful and time-aware operators, and (as the framework grows) complete pipelines - deterministically, in-process, in milliseconds.

## Overview

`clink::test` lets library consumers test their functions and operators without a cluster, without threads, and without the wall clock. The design principle is **fidelity by construction**: the harness does not mock the engine - it composes the same production pieces an operator runner would (a real `RuntimeContext` over an in-memory state backend, the operator's own `TimerService` with an injected manual clock, the engine's `Emitter`) and drives the operator through its real hooks. Event-time timer firing *is* the operator's own `on_watermark` path; processing-time firing replicates the runner's between-pops `poll_due`. What a test observes is what production does.

Link the `clink::test_support` CMake target from test executables only; production binaries must not depend on it. Everything lives in `namespace clink::test` under `include/clink/test/`.

## Where it lives

| Path | What |
| --- | --- |
| `include/clink/test/output_capture.hpp` | `OutputCapture<T>`: the typed emission log + a real `Emitter<T>`; doubles as the stateless-function collector |
| `include/clink/test/one_input_harness.hpp` | `OneInputOperatorHarness<In, Out>`: lifecycle-enforced single-input operator harness with manual time |
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

## Design rules

- Deterministic: no sleeps, no polling loops, no wall clock.
- In-process: no threads, sockets or services in operator tests.
- Reuse over reimplementation: production semantics come from calling production code, never from a parallel mock runtime.
- Typed: the output model is the engine's own `StreamElement<T>`; no `void*`, no string-keyed lookups.
- RAII: the harness closes what it opened.

## Roadmap (implemented incrementally)

1. Foundation: capture, one-input harness, manual time, lifecycle - DONE.
2. Keyed harness (`state_for`, `timers_for`, key selectors) + `ProcessFunction` factory helpers.
3. Two-input harnesses (co-process, joins, broadcast) with the engine's real two-input watermark combination.
4. Snapshot/restore (the real backend + `snapshot_timers` cycle), failure injection, lifecycle recorder.
5. Test sources and sinks (controllable, replayable; transactional over the committing-sink framework).
6. `LocalTestEnvironment` (full pipelines over the local runtime) and `MiniCluster` (the in-process JM+TM fixture).
7. Assertions, sequence/property-testing support, compiling documentation examples, and migration of representative core tests onto the framework.

## Related

- [./operator-model.md](./operator-model.md) - the operator, emitter and stream-element model the harness drives
- [./time-and-windowing.md](./time-and-windowing.md) - watermarks and timers
- [./state-and-backends.md](./state-and-backends.md) - the state the keyed harness will expose
