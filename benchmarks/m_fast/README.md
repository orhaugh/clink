# M-Fast benchmark

Quantifies the three M-Fast async wins. Each is a clink-vs-clink A/B on the
same backend tier; the only variable is the M-Fast mechanism. These are
controlled-latency synthetic-loader benchmarks (the loader just sleeps a fixed
per-round-trip latency `L`), not S3/MinIO end-to-end, so the ratios isolate the
mechanism instead of measuring object-store noise. Every result reports a
premise-free counter (round-trip count or emit position) alongside wall-clock,
and the benches hard-fail their structural invariants, so they cannot silently
regress to a meaningless number.

There is no cross-engine ratio here. The point is that the M-Fast mechanisms
were shipped but unmeasured.

## The three levers

| Lever | Mechanism | Headline metric |
|---|---|---|
| ASYNC-9A | io_threads default 1 -> 8 (the cold-read IO pool was serialized) | async speedup over sync |
| ASYNC-10 | read coalescing: N per-key `get_async` collapse to one `get_many_async` | round-trips N -> 1 |
| ASYNC-12 | deadline-aware resume: urgent reads served first within a completion batch | urgent mean emit position |

## How to run

```sh
cmake -S . -B build -DCLINK_BUILD_BENCH=ON && cmake --build build -j

# ASYNC-9A (io_threads fix): the io_threads sweep of the async-state bench.
# P=1 is the pre-fix default (serialized), P=8 the post-fix default.
./build/benchmarks/clink_async_state_bench --records=4096 --latency-us=200 --pool-sizes=1,8,16

# ASYNC-10 + ASYNC-12:
./build/benchmarks/clink_mfast_bench --records=4096 --io-threads=8 --latency-us=200 --urgent=400
```

## ASYNC-9A: the io_threads serialization fix

The pre-fix default funnelled every in-flight cold read through ONE IO thread,
so the async path could not overlap reads at all. The fix made the default 8
(with a `?io_threads=` knob). Same operator, same per-read latency `L`, same IO
pool size `P` on the sync and async arms; only the execution model differs.

Representative (N=4096 distinct cold keys, L=200us, 12-core host):

| io_threads P | async speedup over sync |
|---|---|
| 1 (pre-fix default) | 1.00x (no overlap - the bug) |
| 8 (post-fix default) | 8.23x |
| 16 | 16.49x |

At P=1 the async path is byte-for-byte as slow as the serial sync path; the fix
turns the IO pool on. Speedup is bounded by `min(io_threads, in-flight keys,
AEC cap)` and the machine's cores.

## ASYNC-10: read coalescing

N records over N distinct cold keys through one running-SUM operator, on a
pool-backed `RemoteReadBackend` whose `RemotePool` models RTT-dominated cost:
`read()` is one round-trip (sleeps L); `read_many()` is ONE round-trip for the
whole batch (as `S3RemotePool`'s manifest-load + batched/coalesced gets are).

Representative (N=4096, P=8, L=200us):

| arm | pool round-trips | batched reads | wall |
|---|---|---|---|
| non-coalescing | 4096 (+ infra) | 0 | ~160 ms |
| coalescing | 0 data | 1 | ~10-20 ms |

The round-trip reduction (4096 -> 1) is exact and premise-free; it is the
headline. The wall ratio (~7-14x here) varies run-to-run because the coalescing
arm's wall is small and dominated by coroutine park/resume overhead at this N,
so treat it as corroboration of the round-trip win, not a precise multiple.
`S3RemotePool` ADDITIONALLY dedups same-content-hash keys into a single GET
(proven in the s3 suite, not re-run here).

## ASYNC-12: deadline-aware resume

A QoS/ordering property, not throughput. N reads all park then release together
(one completion batch); the `u` urgent records (lowest deadline) ARRIVE LAST -
the worst case for FIFO. Metric: the mean emit position of the urgent subset.

Representative (N=4096, u=400 urgent):

| resume order | urgent mean emit position |
|---|---|
| FIFO | 3895.5 (urgent served last) |
| Priority | 199.5 (urgent served first) |

Priority pulls the urgent subset from the back of the batch (FIFO: ~N - u/2) to
the front (~u/2). The numbers are deterministic. With per-record downstream
cost, emit position maps directly to completion latency: Priority finishes the
urgent work first. This reorders only already-safe completions within one poll
(the per-key gate guarantees distinct keys), so correctness is unchanged.
