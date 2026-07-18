# clink vs Flink - head-to-head benchmark

Apples-to-apples throughput benchmark. Both engines run the same
pipeline (Kafka → `key_by` → 1s tumbling event-time window → sum →
Kafka) against the same pre-populated Kafka topic, with the same
parallelism, and we measure how long each takes to drain it.

## Pipeline

See `pipeline.md` for the full spec. Short version:

```
Kafka(bench-in, 24-byte records: ts_ms | key | value)
  -> decode bytes
  -> assign_timestamps_monotonic(ts_ms)
  -> key_by(key)
  -> tumbling_window(1000 ms event-time)
  -> aggregate(sum of value)
  -> 8-byte sum
  -> Kafka(bench-out)
```

Default scale: 10M records, 1000 keys, 100 windows, parallelism 4.
Expected output: 100,000 sums.

## Prerequisites

- Docker + Docker Compose
- Maven + Java 21 (for the Flink JAR)
- Python 3 with `confluent-kafka` (`pip install -r driver/requirements.txt`)
- clink built with `-DCLINK_BUILD_BENCH=ON`

## Run

```bash
./run.sh
```

Override scale with env vars:

```bash
RECORDS=100000000 KEYS=10000 WINDOWS=1000 ./run.sh
```

Output lands in `./results/`:
- `flink.json`, `clink.json` - per-engine raw metrics.
- `clink_coordinator.log`, `clink_worker_*.log`, `clink_submit.log` - clink-side logs.

The final scoreboard is printed at the end of `run.sh`.

## What's measured

- **wall_seconds**: monotonic-clock window from first observed output
  record to last observed output record on the sink topic.
- **record_count**: total output records observed. Correctness check:
  `record_count == n_keys * n_windows`.
- **throughput_rec_per_s**: `record_count / wall_seconds`.

Latency is intentionally not measured in v1 (depends on watermark
progression strategy and adds noise). A latency follow-up can land
once throughput numbers stabilize.

## What's NOT in v1

- TLS / SASL Kafka.
- Stateful failure / recovery comparison (both engines have checkpoints
  but exercising recovery during the bench is a separate harness).
- Multiple pipeline shapes (only the canonical key_by + tumbling +
  sum). Stateful interval join + Pure-map are the natural follow-ups.
- Latency p50/p99 (see above).

## Honest caveats

- Both engines are configured with checkpointing OFF and at-least-once
  Kafka sink. Turning checkpointing on costs both engines, but not
  symmetrically - Flink's snapshot machinery is more mature, so this
  benchmark intentionally compares the hot path.
- Flink and clink use different default chaining strategies. Operator
  chaining is *disabled* on the Flink side (`env.disableOperatorChaining()`)
  for parity with clink's current chain coverage. Re-enabling Flink
  chaining and pushing clink chaining harder is a follow-up.
- The producer pre-populates the input topic *before* either engine
  runs; the engines are not racing the producer.
- Topic partition count matches parallelism (4) on both engines.

## Adding a new pipeline shape

Drop a sibling Java class under `flink-job/src/main/java/com/clink/bench/`
and a sibling `.cpp` under `clink-job/`, register both via this
README, and add a flag to `run.sh` selecting which to run. Producer
record shape is the only thing that needs to stay symmetric.
