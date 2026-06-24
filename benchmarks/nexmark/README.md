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

## Query coverage

| query | shape | notes |
|---|---|---|
| q0 | pass-through | projection only |
| q1 | currency conversion | stateless arithmetic (DECIMAL) |
| q2 | selection | `mod()` computed in a derived table, filtered in the outer WHERE (clink WHERE compares a column to a literal) |
| q3 | local-item join | INNER `auction ⋈ person` on `seller = id` + filter |
| q20 | bid expansion | INNER `bid ⋈ auction` on `auction = id` + filter |
| q11 | user sessions | per-bidder `COUNT(*)` over a 10s `SESSION`, emitting the session `window_start`/`window_end` |
| q12 | bids per window | per-bidder `COUNT(*)` over a 10s `TUMBLE` (event-time analogue of Nexmark's proctime q12) |
| q7 | highest bid | per-window `MAX(price)` as a join side (equi on price) + a column-vs-column range residual `dateTime IN [window_start, window_end)` |
| q8 | new users | two windowed aggregates joined on `seller = id` + a column-vs-column window-equality residual |
| q5 | hot items | per-(auction,window) count joined to the per-window `MAX` of those counts (a non-windowed GROUP BY -> changelog) + a count-equals-max residual. Run with `--slots 16` (nested windowed counts + a join). |
| q15 | per-day stats | `DATE_TRUNC('day', datetime)` bucket + `COUNT(*)` / `COUNT(DISTINCT bidder)` / `COUNT(DISTINCT auction)` |
| q17 | per-auction-day stats | `GROUP BY auction, day` + `COUNT(DISTINCT)`, `MIN`/`MAX`/`AVG`/`SUM(price)` |
| q18 | latest bid per (auction, bidder) | `ROW_NUMBER() OVER (PARTITION BY auction, bidder ORDER BY datetime DESC)`, `rn <= 1` (TOP-N-per-key changelog) |
| q19 | top-10 per auction | `ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC)`, `rn <= 10` (TOP-N-per-key changelog) |

Enabling capabilities now in clink SQL: `window_start`/`window_end` projectable
from any windowed GROUP BY (aliasable, BIGINT ms-since-epoch); a derived table
(incl. a windowed aggregate) usable as a join input; column-vs-column comparisons
in WHERE (so a single-equi-key join + a residual predicate expresses range and
composite-key joins); RETRACTION/CHANGELOG streams - a non-windowed GROUP BY
emits a changelog (opt-in, when it feeds a join/netting sink), the equi-join
consumes those retractions, and a netting sink resolves the +/- to the final
relation; and the analytics tier rides existing features (`ROW_NUMBER` TOP-N-per
-key, `COUNT(DISTINCT)`, `DATE_TRUNC`). A changelog stream (TOP-N output) is
accepted by `connector='changelog'` (netting), `mode='upsert'`, or a discard
`connector='blackhole'` sink. q8's per-window grouping carries a `COUNT(*)`
(unused) because clink requires an aggregate in a GROUP BY SELECT.

Fourteen Nexmark queries now execute (q0/q1/q2/q3/q5/q7/q8/q11/q12/q15/q17/q18/
q19/q20). The remaining queries are excluded by named gaps (below).

Window queries: use a lower `--tps` (e.g. `--tps 50000`) so `datetime` spans many
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
