# process-function bench

Exercises the Flink `KeyedProcessFunction` surface — the workhorse
for stateful business logic where windows don't fit. Verifies clink's
process-function operator, timer service, and per-state-type APIs
under load.

## Workload

  - **Source**: 10M events, 1000 keys.
  - **Process function**:
      - On each record:
          - read+update a `ValueState<long>` (running count per key)
          - append into a `ListState<long>` (last 8 values, FIFO)
          - upsert a `MapState<string,long>` (per-tag counters)
          - register an event-time timer at `event.ts + 100ms`
      - On timer fire:
          - emit `(key, count, latest_8_avg, top_tag, top_tag_count)`
          - prune the ListState to the last 4 entries
  - **Sink**: counting sink + p99 record-to-emit latency (the
    function attaches an emit-ts and the sink computes a histogram).

## Why it matters

A process function is where most production logic lives. The bench
exercises three distinct state primitives in one operator, the timer
service driving event-time fires, and a custom emit-on-timer pattern
that is hard to fit into windows. clink and Flink must produce the
same per-key emit sequence to be considered equivalent.

## What it verifies

  - ValueState read+update semantics.
  - ListState append + truncate semantics.
  - MapState upsert + iteration semantics.
  - Event-time timer registration, ordering, fire.
  - Cross-key timer interleave (timers from key A fire alongside key
    B in event-time order).
  - Per-record latency (timer fire happens within `100ms + ε`).

## Status

  - Spec: this file.
  - clink job: `clink-job/` (built and run by the runner).
  - Flink job: `flink-job/` (Maven project, same workload).
  - Runner: `run.sh` (drives both sides; latest logs under `results/`).
