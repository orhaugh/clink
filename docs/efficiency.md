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
| CPU time per event, stateless query (q0) | **709,877 events/cpu-second** | 233,207 | **3.04x less CPU** |
| CPU time per event, windowed query (q12) | **310,811 events/cpu-second** | 155,091 | **2.00x less CPU** |
| Memory, stateless query (q0) | **70 MB** | 1,146 MB | **16.4x less** |
| Memory, windowed query (q12) | 1,230 MB | 1,505 MB | 1.2x less |
| Cores occupied of 4, q0 | **1.08** | 2.58 | |
| Cores occupied of 4, q12 | **2.16** | 2.99 | |

The memory result splits sharply between the two queries, and the reason matters more
than the headline. See [Where the memory difference comes from](#where-the-memory-difference-comes-from).

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
| clink q0 | 2,977,002 rec/s | 2,791,462 rec/s | **94%** |
| JVM engine q0 | 2,088,165 rec/s | 1,021,201 rec/s | 49% |

For a long-running pipeline this matters little. For anything that starts, stops, scales
or fails over, it is the difference between paying for warm-up repeatedly and not.

The engine's own instrumentation shows where its remaining time goes, and it is not the
query: on q0, the projection the query actually asks for is 4.3% of worker CPU, while
JSON decoding is 34% and the Kafka client is 26%. That is published in full in
[the benchmark record](https://github.com/orhaugh/clink/blob/main/benchmarks/nexmark_compare/cloud/README.md),
including the optimisations that were tried and measured to be worthless.

## Where the memory difference comes from

This is the figure most worth understanding properly, because the honest version is
narrower than the headline.

**On stateless work the difference is an order of magnitude: 70 MB against 1,146 MB.**
That is the engine itself, and it is what a native runtime buys. No JVM heap to size, no
metaspace, no garbage collector headroom, no object header on every record.

**On the windowed query the gap nearly closes: 1,230 MB against 1,505 MB.** clink is
still lower, but only slightly, and the reason is that most of that memory is not the
engine at all. It is window state: partial aggregates for every (key, window) group that
no watermark has yet closed. Any engine computing that query has to hold it. This was
established rather than assumed, by ruling out the alternatives on the rig:

| tested | result |
|---|---|
| Is it allocator retention? | No. jemalloc preloaded into the same binary changed memory not at all. |
| Is it the columnar path holding Arrow batches? | No, the opposite. Disabling columnar execution raised memory to 4,663 MB, so the columnar path is *saving* 3.3x. |
| Is it records buffered in flight? | Partly. Shrinking the source batch eightfold recovered about a quarter of it. |
| Is it state? | Yes. Memory tracks records consumed (861 MB at 3.8M records, 1,309 MB at 8.5M) then stops dead the moment ingest completes and stays flat. |

**So the claim this page makes is precise: clink's memory advantage is in the engine, not
in your data.** On stateless or lightly-stateful pipelines that is roughly an order of
magnitude. On a large windowed aggregation, both engines mostly hold the same user state
and the advantage narrows to the engine's own overhead, which is a small share of the
total. Anyone sizing a deployment should apply the first figure to the runtime and the
second to the working set.

## Translating this into a footprint

**clink has not measured wall power, and this page does not claim a figure in kWh or in
CO2e.** Doing so honestly requires the power draw of the specific servers, the data
centre's PUE, the grid's carbon intensity at the time of running, and the embodied
carbon of the hardware. Those are properties of a deployment, not of an engine, and any
number quoted without them would be decoration.

What the measurements do support is the input to that calculation. CPU time and memory
are what capacity planning is denominated in, and capacity is what draws power:

- **CPU time per event** determines how many cores must be provisioned to sustain a
  given event rate. Measured here at 3.04x lower (stateless) and 2.00x lower (windowed).
- **Memory per instance** determines how much RAM must be provisioned, and often which
  instance size must be bought. Measured here at 16.4x lower for the runtime itself.
- **Cores occupied** was 1.08 of 4 against 2.58 of 4 on the stateless query, doing the
  same work in the same wall time on the same machine.

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
- **Throughput ratios are not stable to the decimal.** The same clink commit measured
  1.56x and 1.30x against the JVM engine on two different machines of the same type. The
  efficiency and memory columns held steady across those runs; the raw throughput ratio
  did not. Treat throughput as "ahead" and cost-per-event as the durable figure.
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

- **Measured** 26 July 2026, clink at commit `1eb6fae`, against Apache Flink 2.2.0 on
  Java 21 (upstream image, unmodified).
- **Rig** 2x Hetzner ccx23 nodes in `fsn1` (4 dedicated vCPU each, AMD EPYC Milan), one
  engine node and one broker node, private network between them.
- **Raw output** every figure on this page comes from the eight per-run JSON files
  published alongside it: [`assets/efficiency-2026-07-26.json`](assets/efficiency-2026-07-26.json).
- **Full record**, including the method corrections that voided earlier numbers, the
  per-stage CPU attribution, and the optimisations that were measured and rejected:
  [`benchmarks/nexmark_compare/cloud/README.md`](https://github.com/orhaugh/clink/blob/main/benchmarks/nexmark_compare/cloud/README.md).

An earlier round of measurements on a single machine was discarded rather than published:
with the broker and both engines sharing cores, clink's q0 was recorded at both 1.06M and
571k records per second for the same configuration. That spread was wider than the effect
being measured, which is why the two-node rig above exists.
