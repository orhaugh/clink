# In-process clink-vs-Flink bench

Drives both engines with the **same synthetic source + sink in process**
so the bench measures engine work (operator dispatch, state I/O,
serde, scheduling) instead of Kafka throughput.

## Record type

A ~2 KB rich nested struct. Both engines define the same shape and
serialise it via their own type system. Serde happens on the wire (TM
↔ TM, where applicable) and on every state-backend write / checkpoint.

```
struct Event {
    int64_t  ts_ms;
    int64_t  key;
    int64_t  value;
    string   payload;              // ~1500 bytes of synthetic text
    vec<int64_t> tags;             // 50 entries (~400 bytes)
    map<string, string> attributes;// 4 entries (~100 bytes)
};
// Total ~2 KB per record.
```

## Pipeline

```
source(in-proc, bounded, N events)
  -> assign_timestamps_monotonic(ts_ms)
  -> key_by(key)
  -> tumbling_window(1000 ms event-time)
  -> aggregate -> EventStats{ sum_value, count, latest_payload }
  -> sink(count records, no I/O)
```

`EventStats` keeps the latest payload to ensure window state is "rich"
(~1.5 KB per (key, window) slot), exercising RocksDB writes per
input record and snapshot serde per checkpoint.

## State backend + checkpointing

- **Backend**: RocksDB on both engines.
- **Checkpoint interval**: 5 s. Multiple checkpoints fire during the
  run, exercising snapshot serde.

## Default scale

- 10 M events
- 1 000 keys
- 100 windows (event-time spread over 100 × 1 s)
- Expected aggregate output: 100 000 records.

## What we measure

Wall-clock time from job submission to job completion (both engines
run bounded so source finishes naturally, windows fire on EOS, sink
counts to N, job ends).

## What we DON'T measure (skipped on purpose)

- Kafka producer/consumer throughput — that's infrastructure, not engine.
- Network bridge throughput at par > 1 — separate concern, addressed
  once par=1 numbers are clean.
- Cluster scheduling / multi-TM placement — par=1, single TM.
