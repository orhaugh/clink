# Efficiency and environmental impact

Stream processing runs continuously. A pipeline that ingests events all day occupies
its cores and its memory all day, so the cost of processing one event is paid millions
of times over, and it shows up as machines provisioned, power drawn and money spent.

This page publishes what clink actually measured against a JVM stream processor on
identical hardware doing identical work, and is explicit about what those measurements
do and do not establish. Every figure here comes from a run whose method, commit and raw
output are recorded; nothing is modelled or extrapolated.

## The two headline results

Per event processed, on the same hardware, producing byte-identical output:

| | clink | JVM engine | ratio |
|---|---:|---:|---:|
| CPU time per event, stateless query (q0) | **983,957 events/cpu-second** | 254,425 | **3.87x less CPU** |
| CPU time per event, windowed query (q12) | **482,940 events/cpu-second** | 168,993 | **2.86x less CPU** |
| Memory, stateless query (q0) | **76 MB** | 1,169 MB | **15.5x less** |
| Memory, windowed query (q12) | **749 MB** | 1,625 MB | **2.2x less** |
| Cores occupied of 4, q0 | **0.84** | 2.57 | |
| Cores occupied of 4, q12 | **1.71** | 2.94 | |

On the stateless query clink drains the same input faster than the JVM engine while
occupying **0.84 of four cores against 2.57**.

The memory result still splits between the two queries, and the reason is worth reading
before quoting the q12 row: most of that 749 MB is window state the query genuinely
requires, not engine overhead. See
[Where the memory difference comes from](#where-the-memory-difference-comes-from).

## How it was measured

**The workload.** Nexmark q0 (a stateless projection) and q12 (a windowed GROUP BY),
9.2 million nexmark bid records read from Kafka, parallelism 4, output discarded so the
measurement is the engine rather than a downstream connector.

**The hardware.** Two dedicated-vCPU cloud nodes: one 4-vCPU node running whichever
engine is being measured, one 4-vCPU node running the broker alone. Dedicated rather
than shared vCPU, because noisy neighbours were the largest source of error in an
earlier single-machine attempt. The broker is on its own node so its CPU is never
charged to, or subsidised by, either engine.

**One engine at a time, on the same node.** The two engines never run together. Each
gets the same four dedicated cores, the same broker, the same records. Both were
measured on the same physical machine within the same hour, and the JVM engine was
re-baselined on that machine rather than compared against a figure from another one.

**Matched shapes.** The JVM engine ran 1 JobManager and 1 TaskManager with 4 slots;
clink ran 1 coordinator and 1 worker. Its metrics fetch interval was lowered from the
10 second default to 200 ms, because at the default every rate computed for it is
quantised to 10 seconds and the engine is reported below its true speed. That change
works against clink and was made anyway.

**Fresh stack per run.** Every measured run got a newly composed cluster and a fresh
consumer group. A reused cluster drifts upward in CPU as it warms (2.6x by the sixth
job on this harness), and a reused consumer group resumes at a committed offset and
measures nothing.

**CPU is read from the OS, not from the engine.** cgroup v2 `cpu.stat usage_usec` for
every container belonging to the engine under test, sampled before and after, so it
includes the JVM's own threads and clink's client library threads alike. Memory is
cgroup `memory.stat anon`: the engine's own heap and stacks, excluding page cache, which
is the kernel's to reclaim and not the engine's working set.

### The correctness gate

An efficiency comparison is meaningless if the two engines are not doing the same work.
Both were run through a gate that requires identical output row counts:

```
q0    460,000 rows = 460,000 rows      clink = JVM engine
q12   184,767 rows = 184,767 rows      clink = JVM engine
```

Not approximately equal. Identical, on both a stateless and a stateful query. The
efficiency figures above are for engines producing the same answer.

## Where the CPU difference comes from

clink is a native binary. There is no bytecode interpreter, no just-in-time compiler
warming up, and no garbage collector scanning a heap while records flow. Data moves
through the engine as Apache Arrow column batches, so a batch of a thousand records is
one allocation and one pass rather than a thousand objects.

A second, measurable consequence is that clink reaches its full rate almost at once. The
gap between an engine's peak sustained rate and its rate over the whole run is warm-up
and taper:

| | peak sustained | whole-run average | fraction of peak reached |
|---|---:|---:|---:|
| clink q0 | 3,978,542 rec/s | 3,792,293 rec/s | **95%** |
| JVM engine q0 | 2,735,414 rec/s | 1,156,942 rec/s | 42% |

For a long-running pipeline this matters little. For anything that starts, stops, scales
or fails over, it is the difference between paying for warm-up repeatedly and not.

The engine's own instrumentation shows where its remaining time goes, and it is not the
query: on q0 the projection the query actually asks for is 2.9% of worker CPU, while JSON
decoding is 31% and the Kafka source is 30%. That is published in full in
[the benchmark record](https://github.com/orhaugh/clink/blob/main/benchmarks/nexmark_compare/cloud/README.md),
including the optimisations that were tried and measured to be worthless.

## Where the memory difference comes from

This is the figure most worth understanding properly, because the honest version is
narrower than the headline.

**On stateless work the difference is an order of magnitude: 76 MB against 1,169 MB.**
That is the engine itself, and it is what a native runtime buys. No JVM heap to size, no
metaspace, no garbage collector headroom, no object header on every record. This is the
figure that generalises to any pipeline whose working set is small.

**On the windowed query the gap is narrower: 749 MB against 1,625 MB.** Most of clink's
figure is state the query genuinely requires - a partial aggregate per (key, window) group
that no watermark has yet closed - rather than engine overhead. Holding the records and the
window fixed and varying only the number of groups separates the two. The dataset spans one
second of event time, so every record falls in a single 10-second window; `GROUP BY bidder`
produces 195,710 groups and `GROUP BY channel` produces five:

| grouping | groups | clink memory |
|---|---:|---:|
| `GROUP BY bidder` | 195,710 | 831 MB |
| `GROUP BY channel` | 5 | 444 MB |

So about 387 MB is per-group state - **1,977 bytes per group** - and 444 MB is independent
of the group count (pipeline buffering: channels are bounded by a count of batches rather
than by bytes, so a deeper pipeline holds proportionally more data in flight).

For comparison, the JVM engine's memory rises about 456 MB between the stateless and the
windowed query while holding the same 195,710 groups. **So clink is now lighter on the state
itself as well, by about 1.2x.**

An earlier version of this page reported the opposite, and the correction is worth being
explicit about. Measured on 26 July, clink's per-group state was 3,914 bytes and it was
roughly twice as heavy as the JVM engine on state alone; the page said so. That was a defect,
not a floor: a per-group accumulator carried inline storage for aggregate features the query
never used - two maps, two vectors, a decimal and two more values for a `COUNT(*)` that needs
one integer. Moving the rarely-used members behind a lazily allocated pointer halved the
per-group cost, and pushing the query's column projection down into the JSON decoder narrowed
what the pipeline buffers. Both are in the repository history with their measurements.

**1,977 bytes per group is still more than an int64 key and a counter need**, so this is
progress rather than a finished job. What remains is container overhead - a map node per open
window, a retained row per bucket for reconstructing group columns, the keyed-state entry -
not the accumulator.

The honest summary: clink's advantage is largest where the engine dominates (an order of
magnitude on stateless work) and narrows as the user's own state grows, which is what any
engine should look like. It is now ahead on both.

## Translating this into a footprint

**clink has not measured wall power, and this page does not claim a figure in kWh or in
CO2e.** Doing so honestly requires the power draw of the specific servers, the data
centre's PUE, the grid's carbon intensity at the time of running, and the embodied
carbon of the hardware. Those are properties of a deployment, not of an engine, and any
number quoted without them would be decoration.

What the measurements do support is the input to that calculation. CPU time and memory
are what capacity planning is denominated in, and capacity is what draws power:

- **CPU time per event** determines how many cores must be provisioned to sustain a
  given event rate. Measured here at 3.87x lower (stateless) and 2.86x lower (windowed).
- **Memory per instance** determines how much RAM must be provisioned, and often which
  instance size must be bought. Measured here at 15.5x lower for the runtime itself.
- **Cores occupied** was 0.84 of 4 against 2.57 of 4 on the stateless query, while draining
  the same input sooner on the same machine.

To apply this to your own footprint, take your existing pipeline's provisioned CPU and
RAM, scale by the ratios above for a comparable query shape, and multiply by your own
power, PUE and carbon-intensity figures. clink supplies the engine-side ratio; only you
have the rest of the equation.

## What was not measured

Stated plainly, because an efficiency page without this section should not be trusted:

- **Wall power, PUE, carbon intensity and embodied carbon.** None of it. See above.
- **Two query shapes, one workload family.** Nexmark q0 and q12. A different pipeline,
  particularly one dominated by a connector or by user code, may differ.
- **One hardware generation.** AMD EPYC Milan, 4 dedicated vCPU per node. clink's own
  ISA experiments on this hardware showed results do not always transfer between
  architectures.
- **An untuned comparison.** The JVM engine ran its upstream image with the harness's
  configuration and one change made in its favour (the metrics interval). A tuning
  effort on either side would move the numbers.
- **Throughput ratios are not stable to the decimal.** One clink commit measured 1.56x and
  1.30x against the JVM engine on two different machines of the same type. The efficiency and
  memory columns held steady across those runs; the raw throughput ratio did not. Treat
  throughput as "ahead" and cost-per-event as the durable figure.
- **A finished windowed-state representation.** clink's per-group state measured 1,977
  bytes for an int64 key and a count, down from 3,914, and the remainder is container
  overhead rather than the accumulator. Read the q12 memory row as "clink today", not
  "clink at its floor".
- **Long-run behaviour.** These are minutes-long runs, not weeks. Memory behaviour over
  a long-lived job with continuous watermark progress is not what this measured.

## Reproducing it

The harness is in the repository, not described in prose. Provisioning, running and
teardown are three commands:

```bash
cd benchmarks/nexmark_compare/cloud
TOPOLOGY=split ./provision.sh
ENGINE_IP=<engine> BROKER_PRIVATE_IP=<broker> ENGINES="clink flink" \
  QUERIES="q0 q12" PAR=4 EVENTS=9200000 REPEATS=2 ./split-run.sh
./teardown.sh && ./teardown.sh --check
```

The correctness gate is a separate harness, because gating on output equality and
measuring sustained throughput want different setups:

```bash
cd benchmarks/nexmark_compare
QUERIES="q0 q12" PARALLELISM=4 EVENTS=500000 ./run.sh
```

## Provenance

- **Measured** 27 July 2026, clink at commit `764e570`, against Apache Flink 2.2.0 on
  Java 21 (upstream image, unmodified), re-baselined on the same machine in the same session.
- **Rig** 2x Hetzner ccx23 nodes in `fsn1` (4 dedicated vCPU each, AMD EPYC Milan), one
  engine node and one broker node, private network between them.
- **Raw output** every figure on this page comes from the eight per-run JSON files
  published alongside it: [`assets/efficiency-2026-07-27.json`](assets/efficiency-2026-07-27.json).
  The previous round is kept for comparison at
  [`assets/efficiency-2026-07-26.json`](assets/efficiency-2026-07-26.json).
- **Full record**, including the method corrections that voided earlier numbers, the
  per-stage CPU attribution, and the optimisations that were measured and rejected:
  [`benchmarks/nexmark_compare/cloud/README.md`](https://github.com/orhaugh/clink/blob/main/benchmarks/nexmark_compare/cloud/README.md).

An earlier round of measurements on a single machine was discarded rather than published:
with the broker and both engines sharing cores, clink's q0 was recorded at both 1.06M and
571k records per second for the same configuration. That spread was wider than the effect
being measured, which is why the two-node rig above exists.
