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

### One regression, unexplained

q0 memory improved (69 MB anon, from 169 MB), which is consistent with no longer
building name-keyed rows. **q12 memory went the other way: 1,412 MB against 277 MB at
the baseline**, so clink's memory advantage on q12 fell from 6.0x to 1.2x. It is
recorded here rather than left out of the table.

Two candidate causes were tested on the rig and neither accounts for it:

- **Born-columnar output into the window** (the `tumbling_window_row` gate fix):
  ruled out. `CLINK_DISABLE_COLUMNAR_OUTPUT=1` measured 1,425 MB against 1,468 MB
  with it on - no material difference.
- **In-flight batching.** Channels are bounded by ELEMENT count (1024) rather than
  by bytes or records, and each element is a batch of up to 1024 records, so a
  channel can hold ~1M records and a faster source fills more of them. Shrinking the
  source batch 8x (`max_batch_size='128'`) cut memory to 1,109 MB - a real effect, but
  only about a quarter of the gap, so it is a contributor and not the cause.

What remains is not diagnosed and no explanation is offered for it here. Byte-bounded
(rather than element-bounded) channel capacity is worth having on its own merits, but
choosing a capacity needs a measured sweep on a rig that can saturate the engine, not
a guess.

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
