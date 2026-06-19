# Canonical comparison pipeline

Both engines run this exact pipeline.

## Record layout (24 bytes, little-endian)

```
struct Record {
    int64_t ts_ms;   // event-time, epoch milliseconds
    int64_t key;     // grouping key (0..K-1)
    int64_t value;   // payload to sum
};
```

Producer encodes; both engines decode using the same byte layout.

## Pipeline

```
Kafka(topic: bench-in)
  -> decode bytes -> Record
  -> assign_timestamps_monotonic(ts_ms)
  -> key_by(key)
  -> tumbling_window(1000 ms event-time)
  -> aggregate<int64_t, Out>(
       sum init/combine,
       (key, window, sum) -> Out{window.end, key, sum})
  -> encode 24 bytes (little-endian)
  -> Kafka(topic: bench-out)
```

## Output record (24 bytes, little-endian)

```
struct Out {
    int64_t window_end_ms;
    int64_t key;
    int64_t sum_value;
};
```

Both engines emit the full triple. clink uses the emit-form
`aggregate<Agg, Out>(init, combiner, emit_fn)` (see
`KeyedTumblingWindowEmitOperator`); Flink uses the equivalent
two-function `.aggregate(AggregateFunction, ProcessWindowFunction)`.

## Parallelism + scale

- Parallelism: 4 subtasks per stage.
- Records: 10,000,000.
- Distinct keys: 1,000.
- Event-time: ts_ms = i * step where step is chosen so the 10M records
  span 100 windows (window_size_ms * 100 / record_count rounded). The
  producer writes ts_ms in monotonic order.
- Watermark strategy: bounded-out-of-orderness 0 ms (records already
  arrive in ts_ms order in the bench).

## Metrics

For each engine we report:
- `wall_seconds`: time from "first input record consumed by source" to
  "last output record emitted by sink".
- `throughput_rec_per_s`: 10_000_000 / wall_seconds.
- `output_record_count`: expected = 100 windows * 1000 keys = 100_000.
  Mismatch is a correctness failure, not a perf result.

Latency is intentionally *not* a v1 metric: it depends on watermark
progression strategy and adds noise that obscures the throughput
comparison. A latency follow-up can be added once v1 numbers stabilize.

## Anti-cheat rules

To keep the comparison honest:
- Both engines use parallelism=4 and the same Kafka topics.
- Both engines re-read from the input topic starting at offset 0 each
  run (no checkpoint reuse).
- Each engine runs in a freshly-brought-up container set; no warm JVMs
  or warm clink processes leak across runs.
- Input topic is pre-populated before the run starts; the engine is
  not racing the producer.
- Output topic is drained and recreated between runs.
