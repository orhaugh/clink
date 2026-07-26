# Split cloud rig: engine node + broker node

A cross-engine nexmark rig on dedicated hardware, built because the single-box
harness could not answer the question being asked of it.

## Why this exists

On the laptop harness the broker, both engines and every container share the same
cores, and the broker's CPU is charged to neither engine. The run-to-run spread on
identical work was wide enough to swamp the effect being measured: clink's q0 was
recorded at both 1.06M and 571k rec/s for the same configuration. Any conclusion
drawn from a single-box A/B at that spread is a coin toss.

Here the broker gets its own node, the engine node has four dedicated vCPUs, and
the two engines run on the SAME node one after the other. Flink's numbers came back
reproducible to within 4% across trials, versus roughly 2x on the laptop.

## Layout

    provision.sh        create the cluster (TOPOLOGY=split for the 8-core rig)
    teardown.sh         delete everything labelled purpose=clink-bench
    split-broker.yml    Kafka + Zookeeper, host networking, on the broker node
    split-engine.yml    both engines, one profile at a time, on the engine node
    split-run.sh        drive a run from the laptop, sample on the node
    record.py           fold one run's measurements into a result JSON
    summarize-split.py  the comparison table and the ratios
    thread_cpu.py       attribute a worker's CPU to thread names
    job_id.py           read the job id out of clink_submit_sql's output

Servers bill until destroyed. Run `./teardown.sh` when finished and
`./teardown.sh --check` afterwards; do not take the script's word as the only
evidence, check the Hetzner console too.

## Rig details that matter

- **Dedicated vCPU (CCX), not shared (CX/CPX).** Shared vCPU means noisy
  neighbours, and that variance is what this rig exists to remove.
- **Matched shapes.** Flink gets 1 JobManager + 1 TaskManager with 4 slots; clink
  gets 1 coordinator + 1 worker. One clink worker, not four, so a keyed shuffle has
  the same in-process opportunity on both sides. A 4-worker clink would shuffle over
  real TCP while a 1-TaskManager Flink shuffled in memory, and the comparison would
  be measuring topology rather than engine.
- **`metrics.fetcher.update-interval: 200` on Flink.** At the 10s default every
  rate the sampler computes is quantised to that period, which reports Flink's
  LOWER bound as its result. This rig gives Flink its true number.
- **Fresh stack and fresh consumer group per measured run.** Chained runs on one
  warm cluster drift monotonically in CPU (2.6x by the sixth job), and a reused
  group id resumes at a committed offset and drains nothing.
- **The sampler runs on the engine node, not the laptop.** Polling a counter across
  the public internet at a 100ms interval folds tens of ms of RTT into a 500ms
  slope window.

## Baseline, 2026-07-26

Rig: 2x ccx23 (4 dedicated vCPU each), fsn1. Engine node runs one engine at a time
against the broker node over the private network. Topic `nx-bid`, 9.2M
nexmark-shaped bid records across 4 partitions, parallelism 4, blackhole sink.
clink at commit 2102570 (`ghcr.io/orhaugh/clink-runtime:main`, amd64), Flink 2.2.0
on java21. Two trials each.

**Broker control, measured from the engine node** so it bounds both engines
identically: `kafka-consumer-perf-test` served 1.06M rec/s to one consumer thread
(1.71M rec/s fetch rate), 982k rec/s to four. Anything reading below ~1M rec/s here
is limited by itself, not by the broker.

| query | engine | sustained rec/s | end-to-end drain rec/s | cores of 4 | events/cpu-s | anon MB |
|-------|--------|----------------:|-----------------------:|-----------:|-------------:|--------:|
| q0    | clink  | 1,860,806 | **1,442,641** | **1.51** | **420,668** | **169** |
| q0    | flink  | **2,448,806** | 1,073,387 | 2.67 | 228,742 | 1,161 |
| q12   | clink  | 1,361,300 | **843,453** | **1.66** | **286,604** | **277** |
| q12   | flink  | **1,649,798** | 679,719 | 2.98 | 148,268 | 1,661 |

Read together rather than one column at a time:

- clink is **1.84x (q0) and 1.93x (q12) more CPU-efficient** per event.
- clink is **6.9x (q0) and 6.0x (q12) lighter on memory**.
- clink finishes the same input **1.35x (q0) and 1.19x (q12) sooner end to end**.
- Flink's peak sustained slope is higher (clink 0.76x on q0, 0.83x on q12). Flink
  bursts faster and clink finishes sooner, because Flink spends more of the run in
  startup and taper.

So the honest summary at this commit is: clink already wins decisively on cost per
event and on memory, is ahead end to end, and is behind on peak burst rate.

## Verified after the July 2026 performance pass, 2026-07-26

> Read the third-provision figures below for the current numbers; the ratios in this
> section were measured on the second provision and the per-provision machine variance
> is larger than the changes made since (see "The 2026-07-26 verification run").

Second provision of the same rig (2x ccx23, fsn1), clink at 00be5fe against Flink
2.2.0 on java21, both engines on the same engine node one after the other, same 9.2M
records across 4 partitions, parallelism 4, blackhole sink, two trials each. Flink was
re-baselined on THIS provision rather than compared across provisions. Broker control
from the engine node: 1.11M rec/s to four consumer threads, 2.04M rec/s fetch rate.

| query | engine | sustained rec/s | end-to-end drain | cores of 4 | events/cpu-s | anon MB |
|-------|--------|----------------:|-----------------:|-----------:|-------------:|--------:|
| q0    | clink  | **3,252,744** | **2,967,742** | **1.08** | **799,305** | **69** |
| q0    | flink  | 2,080,737 | 1,010,434 | 2.48 | 243,773 | 1,159 |
| q12   | clink  | **2,175,089** | **1,540,088** | 2.09 | **322,581** | 1,412 |
| q12   | flink  | 1,451,387 | 675,924 | 2.75 | 158,484 | 1,650 |

Against the same rig's earlier baseline at 2102570:

| | q0 then | q0 now | q12 then | q12 now |
|---|---:|---:|---:|---:|
| throughput vs Flink | 0.76x | **1.56x** | 0.83x | **1.50x** |
| efficiency vs Flink | 1.84x | **3.28x** | 1.93x | **2.04x** |
| events/cpu-s | 420,668 | **799,305** | 286,604 | **322,581** |
| memory vs Flink | 6.9x lower | **16.7x lower** | 6.0x lower | 1.2x lower |

Total worker CPU for the same 9.2M records fell from 20.9s to 11.2s, and the
per-thread attribution says exactly which fixes did it:

| stage | 2102570 | 00be5fe | |
|-------|--------:|--------:|---|
| kafka_text_source | 7.90s (37.8%) | 3.05s (27.3%) | batched fetch, -61% |
| blackhole_sink | 3.36s (16.1%) | 0.28s (2.5%) | duplicate registration fixed, -92% |
| json_string_to_row | 4.56s (21.8%) | 3.82s (34.1%) | now the largest single item |
| rdk:broker1 | 3.71s (17.8%) | 2.94s (26.3%) | |
| network_bridge | 0.89s (4.3%) | 0.62s (5.5%) | |
| project_row | 0.45s (2.2%) | 0.48s (4.3%) | |
| **total** | **20.9s** | **11.2s** | **1.87x less CPU** |

The run is CPU-bound, so a 1.87x CPU reduction is what the 1.75x throughput rise on
q0 comes from. Nothing here changed the amount of work the queries do.

### Verifying the shuffle change end to end, 2026-07-26

The index+Take split measured 37% faster in isolation. End to end it is real but small,
and the honest way to see it is per-stage attribution rather than throughput. Same node,
both images pulled by digest tag, q12:

| | sha-67a69a1d (before) | sha-0702e95d (after) | |
|---|---:|---:|---:|
| `hash` stage CPU | 7.29s | 6.17s | **-15.4%** |
| total worker CPU | 34.4s | 34.0s | -1.2% |

So the split is roughly 40% of what the `hash` thread does (the rest is key extraction,
the routing decision and pushing to four downstream channels), and the `hash` thread is
about 20% of the pipeline. 15% of 20% is ~3% of worker CPU, at the edge of what this rig
resolves.

**The throughput A/B could not resolve it and is not quoted as evidence.** Two trials of
the SAME image on q12 spanned 278,703 and 328,571 events/cpu-second, an 18% spread, so a
~3% effect is well inside the noise floor of that measurement. q0, which has no keyed
shuffle and served as the control, also drifted. Per-stage attribution is the right
instrument for a change of this size; sustained throughput is not.

### CORRECTION: q12 memory is not "state any engine must hold"

The section below concluded that q12's ~1.4 GB was un-fired window state, on the strength
of ruling out the allocator, the columnar path and in-flight batching, plus the
observation that memory tracks records consumed and then goes flat. That conclusion was
WRONG, and the test that should have been run first is the obvious one: hold the records
and the window fixed and vary only the NUMBER OF GROUPS.

The dataset spans 1.0 second of event time, so all 9.2M records fall in a single
10-second window. `GROUP BY bidder` gives 195,710 groups; `GROUP BY channel` gives 5:

| grouping | groups | clink anon |
|---|---:|---:|
| `GROUP BY bidder` | 195,710 | 1,407 MB |
| `GROUP BY channel` | 5 | 641 MB |

  766 MB attributable to grouping / 195,710 groups = **3,914 bytes PER GROUP**, for an
  int64 key and a COUNT(*). Two orders of magnitude more than the data.

  641 MB independent of group count, against 70 MB for the same engine on stateless q0.
  That is pipeline buffering - channels bounded by a count of BATCHES rather than bytes.

Flink's memory rises ~359 MB between q0 and q12 while holding the same 195,710 groups, so
on state alone clink is currently the HEAVIER engine and only its far smaller runtime
keeps the total below. Both components are defects with identifiable causes, and both are
open work. `docs/efficiency.md` has been corrected accordingly.

The methodological lesson is worth more than the number: "memory tracks records consumed
then plateaus" is equally consistent with per-record retention AND with per-group state
that happens to grow as new keys arrive. Ruling out three wrong explanations is not the
same as establishing the fourth, and the decomposition that would have settled it took one
extra run.

### Memory, and the earlier (superseded) q12 analysis

The stateless case is emphatic and is what a native engine should look like: on q0
clink holds **72 MB** of anonymous memory against Flink's **1,146 MB - 16x lower**,
reproducibly, and the peak is 87 MB against 1,513 MB.

q12 is the interesting one: 1,230-1,416 MB against Flink's 1,505-1,555 MB, only ~1.1x
lower, which does not look like a C++-versus-JVM result at all. Four hypotheses were
tested on the rig and three are wrong:

| hypothesis | test | result |
|---|---|---|
| glibc arena retention | `MALLOC_ARENA_MAX=2` | 1,419 -> 405 MB, but throughput HALVED (q0 2.98M -> 1.57M rec/s). The memory fell because the pipeline slowed, not because retention was released. |
| allocator generally | jemalloc via LD_PRELOAD, same binary | memory UNCHANGED (1,342-1,409 MB). Not retention. |
| the columnar path retaining Arrow batches in panes | `CLINK_DISABLE_COLUMNAR=1` | 1,416 -> **4,663 MB**. The columnar path is *saving* 3.3x, not causing it. |
| in-flight batching | source `max_batch_size` 1024 -> 128 | 1,468 -> 1,109 MB, about a quarter of it. A contributor, not the cause. |

Sampling memory against records consumed settles it:

    t=6s   3,825,920 records   861 MB
    t=8s   8,484,608 records  1,309 MB
    t=12s  9,200,000 records  1,260 MB   (ingest complete)
    t=20s  9,200,000 records  1,260 MB   flat
    t=30s  9,200,000 records  1,260 MB   flat

It grows with ingest and then stops dead. That is **un-fired window state**: panes for
(bidder, window) groups that no watermark has yet closed. It does not release after
ingest because with no further input there is no watermark to advance and fire them,
which is ordinary streaming behaviour at end of stream, not a leak.

So the earlier "unexplained regression" (277 MB at 2102570 against 1,4xx MB now) has a
cause, and it is the speed-up itself: at 2102570 the blackhole sink cost 0.37us per
record and throttled ingest, so panes fired closer to real time and less window state
was ever resident at once. A faster engine holds more concurrent window state for the
same query. Both engines are dominated by that same state on q12, which is why clink's
16x advantage on the stateless query narrows to ~1.1x here - and clink is still the
lower of the two.

Worth keeping in mind when quoting memory: **clink's advantage is in the engine, not in
the state**. On stateless work it is an order of magnitude; on a large windowed
aggregation both engines mostly hold the user's data.

### Third provision, 2026-07-26: clink at 1eb6fae vs Flink on the same node

| query | engine | sustained rec/s | end-to-end drain | cores of 4 | events/cpu-s | anon MB |
|-------|--------|----------------:|-----------------:|-----------:|-------------:|--------:|
| q0    | clink  | **2,977,002** | **2,791,462** | **1.08** | **709,877** | **70** |
| q0    | flink  | 2,289,062 | 1,057,958 | 2.58 | 233,207 | 1,146 |
| q12   | clink  | **1,872,542** | **1,587,132** | 2.16 | **310,811** | 1,230 |
| q12   | flink  | 1,601,015 | 707,311 | 2.99 | 155,091 | 1,505 |

    q0 : 1.30x throughput, 3.04x efficiency, 16.4x lower memory
    q12: 1.17x throughput, 2.00x efficiency,  1.2x lower memory

The efficiency and memory columns are the durable results; the throughput ratio moves
several tens of percent between provisions (1.56x and 1.30x for q0 on two different
machines at effectively the same commit), so quote it as "ahead", not to two decimals.

### Wider vectors (AVX2) buy nothing: measured, not argued

The question was whether compiling the engine for a modern x86 ISA - rather than the
toolchain baseline, which on x86-64 is SSE2 - would raise throughput. It does not.

Two images built from the SAME commit (67a69a1), one default and one with
`CLINK_ISA_BASELINE=x86-64-v3` (AVX2/FMA/BMI2, Haswell 2013+), A/B'd on one node,
two trials each:

| | baseline (SSE2) | x86-64-v3 (AVX2) |
|---|---:|---:|
| q0 sustained rec/s | 3,108,200 / 3,099,433 | 2,894,537 / 3,018,005 |
| q0 events/cpu-s | **745,543 / 760,959** | 715,953 / 727,273 |
| q12 sustained rec/s | 2,065,368 / 1,998,848 | 2,090,964 / 2,004,986 |
| q12 events/cpu-s | 270,270 / 300,850 | 275,862 / 273,321 |

Neutral on the windowed query, and about 4% WORSE on the stateless one. The flag
demonstrably took effect - `objdump` counts 256-bit ymm instructions rising from 7,379
to 27,570 and FMA/broadcast from 438 to 1,265, with the binary 246 KB larger - so this
is not a case of the option doing nothing.

Why there was nothing to win is visible in the CPU attribution above: **the arithmetic
is about 4% of q0** (`project_row`), while the two largest stages are library code that
already dispatches to AVX2 at RUNTIME regardless of our compile flags. Those 7,379 ymm
instructions in the BASELINE binary are simdjson's and Arrow's own vectorised kernels,
compiled with per-implementation target attributes and selected on the CPU at startup.
Raising our floor widens loops that are not where the time goes, and pays for it in code
size.

The hardware note, for anyone tempted by AVX-512: the rig's AMD EPYC Milan (Zen 3)
advertises avx2 and bmi2 but **not** avx512f, so an `x86-64-v4` build would fault on it.
A published image cannot assume a level above v1 without knowing the deployment floor,
which is why `CLINK_ISA_BASELINE` is empty by default and an ISA-raised image gets its
own tag suffix instead of `:main`.

What this rules out is a compile-flag win. It does not rule out targeted work where the
profile actually points - the shuffle `hash` at 19.3% of q12, and Arrow's arithmetic and
comparison kernels, which this build cannot reach because `arrow::compute::Initialize()`
is not exported by the Arrow package clink links (see
`include/clink/operators/columnar_filter_operator.hpp`). Both are about using kernels
that are already vectorised, not about hand-writing intrinsics.

### jemalloc: measured, worth having, not a memory fix

Tested by deriving an image from the SAME clink binary with `libjemalloc2` preloaded, so
the only variable is the allocator. Two trials each against glibc on the same node:

| | glibc | jemalloc | |
|---|---:|---:|---|
| q0 sustained | 2.92-3.07M | 3.08-3.09M | ~neutral |
| q0 events/cpu-s | 726k (mean) | 735k (mean) | ~neutral |
| q0 anon MB | 70-74 | 72-73 | no change |
| q12 sustained | 1.84-1.87M | 1.96M | **+5%** |
| q12 events/cpu-s | 307-311k | 306-331k | ~neutral |
| q12 anon MB | 1,230-1,362 | 1,342-1,409 | no change |

So: a real ~5% throughput gain on the stateful query, nothing on the stateless one, and
**no memory benefit at all** - which is itself the useful result, because it rules the
allocator out as the explanation for q12's memory (see above).

That malloc is on the critical path at all is not in doubt: capping glibc to two arenas
halves throughput. jemalloc's per-thread caches are why it does not pay that contention
while still not retaining like the arena-capped case.

Not adopted as a hard dependency on a 5%-on-one-query result. The sensible shape is to
keep it opt-in - preload it in a deployment that wants it, exactly as measured here -
and revisit if a workload shows more. Recorded so the question is not re-opened without
these numbers.

### The 2026-07-26 verification run, and the decode work on top of it

A third provision measured clink at 1eb6fae (the JSON-decode pass) against Flink
re-baselined on that machine. Two things it established:

**Machine-to-machine variance between provisions is real and larger than the decode
change.** clink q0 read 799k events/cpu-s on the second provision and 710k on the
third, at commits one apart. A SAME-MACHINE A/B settles it: pulling both
`:sha-00be5fe7546d` and `:sha-1eb6faea2859` onto one node and running q0 twice each
gave **726k events/cpu-s for both** - identical within noise. So a cross-engine ratio
must come from one provision (Flink re-baselined alongside), and a clink-vs-clink delta
of a few percent needs the two images on the same node.

**The decode work does not show end to end on x86.** It is +19% on the decode measured
in isolation - but that was measured on ARM (the development machine), and its largest
single component was replacing a `memcmp` call with an inline byte loop, which is a win
where the call dominates and a wash where the platform ships an AVX2 `memcmp`. Neutral
on x86, not a regression. Worth remembering before optimising byte-level code on the
dev machine and assuming it carries.

## Where clink's CPU actually goes

`thread_cpu.py` attributes a worker's CPU to thread names by differencing
`/proc/<pid>/task/*/stat`, which is what per-process CPU cannot show. Both queries
say the same thing: the plumbing dominates and the computation is nearly free.

q0, 9.2M records, 20.9s total worker CPU:

| stage | CPU | share | per record |
|-------|----:|------:|-----------:|
| kafka_text_source | 7.90s | 37.8% | 0.86 us |
| json_string_to_row | 4.56s | 21.8% | 0.50 us |
| rdk:broker1 (librdkafka) | 3.71s | 17.8% | 0.40 us |
| blackhole_sink (only increments a counter) | 3.36s | 16.1% | 0.37 us |
| network_bridge (24 threads) | 0.89s | 4.3% | |
| project_row (the actual query) | 0.45s | 2.2% | 0.05 us |

q12, 9.2M records, 31.0s total worker CPU:

| stage | CPU | share | per record |
|-------|----:|------:|-----------:|
| kafka_text_source | 8.47s | 27.3% | 0.92 us |
| hash (keyed shuffle) | 5.98s | 19.3% | 0.65 us |
| json_string_to_row | 4.82s | 15.5% | 0.52 us |
| rdk:broker1 | 3.51s | 11.3% | |
| tumbling_window (the actual aggregation) | 2.90s | 9.4% | 0.32 us |
| network_bridge (72 threads) | 2.19s | 7.1% | |
| union_4 | 1.51s | 4.9% | |
| row_compute_key | 1.00s | 3.2% | |
| assign_timestamps | 0.60s | 1.9% | |

Three readings worth keeping:

1. **The Kafka source is the single largest cost in both queries** (27-38%), at
   roughly 0.9 us per record just to hand bytes onward.
2. **An operator boundary costs about 0.37 us per record.** That is what a sink
   which does nothing but increment a counter charges, so it is handoff, not work.
   q0 crosses three such boundaries.
3. **The query itself is 2% (q0) and 9% (q12).** Optimising the operators is not
   where the remaining throughput is; the ingest path and the boundaries are.
