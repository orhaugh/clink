# nexmark_compare - clink vs Flink on Nexmark (apples-to-apples)

A cross-engine Nexmark throughput comparison. Both engines run the SAME query over
ONE pre-generated Nexmark dataset read from identical Kafka topics, under a written
matched premise, measured the same way. Sibling of `benchmarks/flink_compare`
(which compares a single keyed-window pipeline); this reuses its skeleton and its
honesty discipline for the Nexmark workload.

Read `pipeline.md` FIRST. It pins every dimension a quoted number depends on
(topology, shared input, format, durability, watermark, parallelism, measurement,
correctness gate, and the only honest headline). A number printed without that
premise holding is not apples-to-apples.

## Why this exists

clink's own Nexmark bench (`benchmarks/clink_nexmark_bench`) is clink-only:
in-process, single TaskManager, logical-stream rate, with a steady-state mode for
clink-vs-clink tracking. It is NOT comparable to another engine. The only existing
cross-engine number in this repo (`flink_compare`) is a different, single-pipeline
workload. This harness produces the first defensible clink-vs-Flink Nexmark number.

## Headline scope

SQL-vs-SQL on {q0 stateless, q5 windowed-agg, q8 windowed-join}; q6 reported
separately as clink-SQL-vs-Flink-DataStream (excluded from the geomean); q13/q14/q10
out of v1. See `pipeline.md` for the rationale.

## Build increments (each independently verifiable)

- [x] **INC 0** - premise doc (`pipeline.md`) + scaffold. The contract, written
  first.
- [x] **INC 2 (core)** - shared-input producer: `nexmark_dump` runs clink's
  `NexmarkGenerator` ONCE (all types) and writes `nx-person/auction/bid` NDJSON,
  the same JSON both engines decode. Verified Docker-free for determinism
  (byte-identical on replay) and the 1:3:46 ratio. Kafka load is the next step.
- [ ] **INC 3** - clink Nexmark Kafka SQL job: re-point the existing query SQL onto
  `connector='kafka'` over the shared topics; clustered submit; count + steady-state
  via the consumer. q0 first.
- [ ] **INC 4** - Flink Nexmark SQL job (q0 canary): extend the pom with
  flink-table + flink-sql-connector-kafka; run the upstream nexmark-flink q0 SQL on
  the pinned Flink image reading the same topics. First real ratio. RISK: upstream
  nexmark-flink SQL targeted Flink 1.x; 2.2.0 compatibility is unverified - q0 is
  the canary.
- [ ] **INC 5/6** - q5 (windowed agg), q8 (windowed join), each gated on per-query
  output-row agreement.
- [ ] **INC 7** - q6 as SQL-vs-DataStream, its own row.
- [ ] **INC 8** - scoreboard: per-query Time/Cores/Cores*Time/ratio + SQL-only
  geomean + banner; optional durable-mode matrix.

## Preliminary result (q0, parallelism 1)

First apples-to-apples number. q0 (stateless pass-through), both engines reading
the same pre-generated `nx-bid` Kafka topic (JSON), hot-path durability
(checkpoints off, in-memory state), steady-state throughput measured identically
from each output record's broker append timestamp (`message.timestamp.type=
LogAppendTime`) over the middle 80%:

| engine | q0 steady events/sec | output rows (gate) |
|---|---|---|
| clink | ~497,000 | 460,000 ✓ |
| Flink 2.2.0 | ~170,000 | 460,000 ✓ |

clink ≈ 2.9x on this query, and notably it wins despite paying the heavier
`JsonValue` row-materialisation cost the JSON-input premise disclosed (which
favours Flink on stateless queries). Both produced exactly 460,000 rows, so the
correctness gate passes.

Caveats this number still carries (so it is NOT yet the full headline of
`pipeline.md`): parallelism is 1 on BOTH (matched, the cleanest per-core
comparison; parallel scaling needs a clink SQL parallelism flag, a follow-on
engine feature - clink SQL ops default to parallelism 1 today). No CPU
normalisation yet (events/sec, not events/sec/core). q0 only - the stateless
floor. q5/q8 (windowed) need the mid-stream-firing measurement and matched SQL
(next increment).

## Producer (INC 2)

```bash
cmake -S . -B build -DCLINK_BUILD_BENCH=ON -DCLINK_BUILD_SQL=ON
cmake --build build --target nexmark_dump -j

./build/benchmarks/nexmark_dump --events 1000000 --out-dir /tmp/nx
# writes /tmp/nx/{person,auction,bid}.ndjson, one JSON object per line,
# the same shape clink's nexmark_source emits and Flink's JSON format decodes.
```

Determinism + ratio are checked by running it twice and diffing, and counting
per-type lines (≈ 1:3:46). The Kafka load (INC 3) ships the NDJSON to the three
topics via the driver venv (`confluent_kafka`).

## Toolchain

Docker (Kafka + Flink containers), a Python driver venv (`confluent_kafka`), Maven
for the Flink job. The Flink image is pinned in `docker-compose.yml`. Reused from
`flink_compare`.
