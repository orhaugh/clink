# Nexmark-on-clink benchmark

Runs queries from the [Nexmark streaming benchmark](https://github.com/nexmark/nexmark)
as clink SQL jobs over a generated event stream, on an in-process
JobManager + TaskManager cluster, and reports input-event throughput.

## Build & run

```bash
cmake -S . -B build -DCLINK_BUILD_BENCH=ON -DCLINK_BUILD_SQL=ON
cmake --build build --target clink_nexmark_bench -j

./build/benchmarks/clink_nexmark_bench --query q0 --events 5000000 --slots 8
# {"query":"q0","events":5000000,"slots":8,"wall_ms":...,"events_per_sec":...,
#  "events_per_sec_per_core":...}
```

Flags: `--query qN`, `--events N` (total events), `--tps N` (dateTime spacing =
1000/tps ms/event), `--slots N` (TaskManager slots = parallelism; joins need >= 7).

## How it works

- **Generator** (`include/clink/nexmark/generator.hpp`): the deterministic
  seeded Nexmark Person/Auction/Bid stream (1:3:46 ratio). Exposed to SQL as
  `connector='nexmark'` (a `GeneratorSource<Row>`).
- **Per-type base tables**: clink joins require base tables, so `person`,
  `auction`, `bid` are each a `nexmark_source` filtered to one type
  (`nexmark_type=...`). Every instance runs the same seeded stream and advances
  over every event, so foreign keys (an auction's `seller`, a bid's `auction`)
  resolve across the tables.
- **Sink**: a `connector='blackhole'` discard sink so sink I/O does not distort
  throughput (the runner still counts records).
- **Runner**: the in-process cluster from the SQL runtime tests; the bounded
  source drains and the harness times the job round-trip.

## Query coverage (first tier)

| query | shape | notes |
|---|---|---|
| q0 | pass-through | projection only |
| q1 | currency conversion | stateless arithmetic (DECIMAL) |
| q2 | selection | `mod()` computed in a derived table, filtered in the outer WHERE (clink WHERE compares a column to a literal) |
| q3 | local-item join | INNER `auction ⋈ person` on `seller = id` + filter |
| q20 | bid expansion | INNER `bid ⋈ auction` on `auction = id` + filter |
| q11 | user sessions | per-bidder `COUNT(*)` over a 10s `SESSION`, emitting the session `window_start`/`window_end` |
| q12 | bids per window | per-bidder `COUNT(*)` over a 10s `TUMBLE` (event-time analogue of Nexmark's proctime q12) |

`window_start`/`window_end` are now projectable from a windowed GROUP BY (any
window kind), aliasable, BIGINT ms-since-epoch.

A derived table (including a windowed aggregate) can now be a JOIN input, so the
join-shaped window queries are no longer wholly blocked. The remaining gaps are
per-query, not the join-input restriction:
- q7 (highest-bid): needs the per-window range match (`price = maxprice AND
  dateTime IN [window_start, window_end)`); clink's join ON is a single equality
  and WHERE compares a column to a literal, so the range half is not expressible.
- q8 (new-users): needs a composite (multi-column) equi-join key
  (`id = seller AND window_start = window_start`); clink's equi-join key is a
  single column.
- q5 (hot-items): a different shape (nested windowed aggregate feeding a
  per-window MAX, then a composite IN-subquery), not a plain join.

Then the analytics tier: OVER/Top-N + distinct/filter aggregates
(q15/q17/q18/q19). See the feasibility scoping.

Window queries: use a lower `--tps` (e.g. `--tps 50000`) so `dateTime` spans many
windows (spacing is `1000/tps` ms/event); at the default tps the run fits in one
window and fires at end-of-stream.

## Caveats (v1)

- `wall_ms` includes job deploy/round-trip overhead, so use a large `--events`;
  a steady-state (warm-up-subtracted) mode is a follow-on.
- `events_per_sec` is the logical-stream rate (`events / wall`). Multi-table
  queries instantiate the generator per table, re-deriving the shared stream.
- In-process single TaskManager; not a distributed-cluster number.
- Excluded by named gaps (not silently): q6 (unsupported in Flink too), q10
  (partitioned sink), q13 (`FOR SYSTEM_TIME`), q14 (`CREATE FUNCTION`), q21
  (`regexp_extract`), q22 (`split_index`).
