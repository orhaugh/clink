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

Those are measured **on a single worker node at parallelism 4**. On a multi-node rig at
parallelism 12 the same comparison gives 2.60x (stateless) and 1.15x (windowed) - see
[What would make this wrong](#what-would-make-this-wrong), because the cause is a clink task-
placement defect and not an inherent limit. Treat the figures above as a best case for
topology until that is fixed.

If you want them in dollars and kilowatt-hours, see
[Translating this into a footprint](#translating-this-into-a-footprint) - with the warning that
a CPU ratio is **not** an energy ratio unless the freed capacity is actually surrendered. On a
fixed fleet the same 3.87x becomes about 1.8x on power, because an idle core still draws
roughly a quarter of its loaded power.

A later run on 28 July 2026, after a data-loss fix in clink's cross-worker shuffle,
reproduced the same direction and magnitude on a fresh provision: 4.08x (stateless) and
3.89x (windowed) less CPU per event, and memory 16x lower on both. Its absolute rates are
**not** comparable to the table above, because its input was a synthetic payload rather
than the nexmark dataset - see [Provenance](#provenance). It corroborates these figures
rather than replacing them.

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

**clink has not measured wall power, and nothing below changes that.** No kWh or CO2e figure
on this page is a measurement. What follows is arithmetic over published coefficients, every
one of them named and swappable, so a reader who disagrees with an input can substitute it and
get their own answer.

The measurements above stop at CPU-seconds and megabytes on purpose. This section goes
further and prices them, because "3.87x less CPU" is not a number anyone budgets in. It is
divided into two tracks, and they are not equally solid.

**Track A, cost,** converts the measured CPU figures into cloud instance-hours at published
list prices. It is close to measurement: the only modelled inputs are a utilisation target
and an instance choice.

**Track B, energy and CO2e,** needs a server power model, a PUE and a grid carbon intensity.
None of those were measured on the rig. Track B is a model throughout and is labelled as
such on every line.

The earlier statement on this page stands: clink has not measured wall power. Nothing below
changes that. What follows is arithmetic over published coefficients, shown step by step so
you can substitute your own and get a different answer.

### The boundary being priced

Fixed once, applied identically to both engines, and never widened or narrowed between them:

> Steady-state operational cost and energy of the engine containers running one query on one
> node type. Excludes the broker, object storage, checkpoint storage, network egress, load
> balancers, observability, CI, container images, cold starts, and the embodied carbon of the
> hardware.

That boundary is narrower than the Software Carbon Intensity specification requires, which
mandates inclusion of idle machines, monitoring, build and deploy pipelines, backup and
failover, and an embodied-emissions term M. So this is **not an SCI score and should not be
described as one.** The embodied term is omitted because no product carbon footprint exists
for the specific hardware and the total resources of the physical host are not published, so
two of the four inputs to M would be guesses. Note that the omission works against clink:
the excluded terms include cold start and image size, where clink's measured advantage is
largest.

### The scenario

One million nexmark events per second, sustained, for one calendar month. Both query shapes.
Both engines sized from their own measured events per CPU-second. A month is 730 hours
(8,760/12), so the month processes 2.628 trillion events.

One million events per second is chosen because it is a round number a reader can rescale,
and because it is large enough that both engines need more than one instance on at least one
query shape. It is not a claim that any particular pipeline runs at that rate.

#### Step 1: vCPU demand

A vCPU here is one SMT thread. The measurement reads cgroup `cpu.stat usage_usec`, which
counts thread-seconds, and the rig's 4 dedicated vCPU sit on 2 physical AMD EPYC Milan
cores. The instance chosen in step 2 is also SMT and also Milan, so the unit is consistent
on both sides of the conversion. Applying a per-physical-core figure to these numbers would
overstate power for both engines and overstate it more for the heavier CPU user.

| query | engine | events per vCPU-second (measured) | vCPU-seconds per wall second at 1M events/s |
|---|---|---:|---:|
| q0 | clink | 983,957 | 1.02 |
| q0 | JVM engine | 254,425 | 3.93 |
| q12 | clink | 482,940 | 2.07 |
| q12 | JVM engine | 168,993 | 5.92 |

#### Step 2: provisioned vCPU and instances

Nobody provisions to 100% of demand. The scenario targets **70% steady-state utilisation**,
which is a normal figure for a latency-sensitive service with headroom for skew and
recovery. Divide demand by 0.7, then round up to whole instances.

Instance: **c6a.xlarge**, 4 vCPU and 8 GiB, AMD EPYC 7R13 (Milan), us-east-1, Linux, shared
tenancy. This is the AWS family that matches the measured hardware on both generation and
SMT convention. Newer families (c7a, c8a) are faster per vCPU but are non-SMT, so a vCPU
there is a whole physical core and the measured numbers would not transfer without a
re-benchmark.

| query | engine | demand (vCPU) | at 70% target | instances (ceil) | provisioned vCPU | actual utilisation |
|---|---|---:|---:|---:|---:|---:|
| q0 | clink | 1.02 | 1.45 | **1** | 4 | 25.4% |
| q0 | JVM engine | 3.93 | 5.61 | **2** | 8 | 49.1% |
| q12 | clink | 2.07 | 2.96 | **1** | 4 | 51.8% |
| q12 | JVM engine | 5.92 | 8.45 | **3** | 12 | 49.3% |

The instance ratio is 2x on q0 and 3x on q12, against measured CPU ratios of 3.87x and
2.86x. That mismatch is not noise. It is instance granularity, and it is discussed below.

#### Step 3: dollars

c6a.xlarge on-demand list is $0.15300 per instance-hour, so $111.69 per instance-month at
730 hours.

| query | clink | JVM engine | difference | ratio |
|---|---:|---:|---:|---:|
| q0 | $111.69 | $223.38 | $111.69 | 2.00x |
| q12 | $111.69 | $335.07 | $223.38 | 3.00x |

At ten million events per second, where whole-instance rounding stops dominating:

| query | clink instances | JVM instances | clink $/month | JVM $/month | ratio |
|---|---:|---:|---:|---:|---:|
| q0 | 4 | 15 | $446.76 | $1,675.35 | 3.75x |
| q12 | 8 | 22 | $893.52 | $2,457.18 | 2.75x |

Normalised, at the ten-million-per-second scale: **q0 $17.00 against $63.75 per trillion
events; q12 $34.00 against $93.50 per trillion events.**

These are list on-demand prices, which almost no sustained deployment pays. A one-year
Standard Reserved Instance, all upfront, is 38.3% below on-demand for c6a, which is
$0.02360 per vCPU-hour or $68.91 per instance-month. Every figure in the two tables above
scales by 0.617 and **no ratio changes.** Two honest caveats on that: a reservation is a
sunk commitment, so an efficiency gain realised mid-term saves nothing until renewal; and a
flexible Compute Savings Plan discounts materially less (20.5% to 33.1% for one year), so
"Savings Plan" and "Reserved Instance" are not interchangeable words here.

#### Step 4: kWh, and this is where the model starts

Server power is **not** proportional to CPU utilisation, and getting this wrong is the single
largest error available in this calculation. Cloud Carbon Footprint's model, which is the
most widely used open methodology, is:

```
watts per provisioned vCPU = min_watts + utilisation x (max_watts - min_watts)
```

For AMD EPYC 3rd Generation (Milan), CCF's AWS coefficient set gives **min 0.46 W and max
1.96 W per vCPU**, derived from SPECpower_ssj2008 submissions divided by thread count. So
23% of a fully loaded vCPU's draw is charged whether the software does anything or not.

Cross-checked directly against a measured Milan machine: a 2-socket EPYC 7763 (256 threads)
drew 122 W at active idle and 406 W at 100% SPECpower load, which is 0.48 W and 1.59 W per
thread. The idle figures agree closely; CCF's max is about 23% above that particular
chassis, and a second 7763 result on the same benchmark drew 460 W, so the spread across
identical CPUs is real. CCF's coefficient is used as central because it averages many
machines; the measured 1.59 W appears in the sensitivity table as the low end.

Facility overhead: **PUE 1.14**, AWS's reported fleet average for calendar 2025.

| query | engine | provisioned vCPU | utilisation | W per vCPU | server W | with PUE | kWh/month |
|---|---|---:|---:|---:|---:|---:|---:|
| q0 | clink | 4 | 25.4% | 0.84 | 3.36 | 3.84 | **2.80** |
| q0 | JVM engine | 8 | 49.1% | 1.20 | 9.57 | 10.91 | **7.97** |
| q12 | clink | 4 | 51.8% | 1.24 | 4.95 | 5.64 | **4.12** |
| q12 | JVM engine | 12 | 49.3% | 1.20 | 14.40 | 16.42 | **11.98** |

Energy ratios: 2.85x on q0, 2.91x on q12. Note these differ from the cost ratios (2.00x and
3.00x), because after rounding the two engines sit at different utilisations and the idle
floor is spread over different vCPU counts. Cost tracks instances bought; energy tracks
instances bought and how hard each is worked. They are not the same quantity.

At ten million events per second, where both engines land near the same utilisation:

| query | clink kWh/month | JVM kWh/month | ratio |
|---|---:|---:|---:|
| q0 | 18.8 | 72.1 | 3.83x |
| q12 | 38.1 | 107.6 | 2.82x |

Per trillion events: q0 **0.72 kWh against 2.74 kWh**; q12 **1.45 kWh against 4.09 kWh**.

#### Step 5: CO2e

Grid intensity **0.271 kg CO2e/kWh**: EPA eGRID2023 subregion SRVC (SERC Virginia/Carolina),
total output emission rate, location-based, data year 2023, which covers us-east-1. This is
an annual average of all generation, combustion emissions only, and it deliberately ignores
any contractual clean-power instruments the operator holds. It is applied identically to
both engines.

| query | scale | clink kg CO2e/month | JVM kg CO2e/month | difference |
|---|---|---:|---:|---:|
| q0 | 1M events/s | 0.76 | 2.16 | 1.40 |
| q12 | 1M events/s | 1.12 | 3.25 | 2.13 |
| q0 | 10M events/s | 5.10 | 19.53 | 14.43 |
| q12 | 10M events/s | 10.33 | 29.16 | 18.83 |

**The absolute numbers are small, and saying so is more useful than dressing them up.** A
ten-million-events-per-second windowed pipeline running all year is a modelled saving of
about 834 kWh and 0.23 tonnes CO2e. That is a real saving and it is not a large one. The
defensible headline from this work is capacity and cost, not tonnage. Anyone multiplying a
per-event figure up to a global total to produce a megatonne number is compounding every
assumption in this section and should be disbelieved, including if it is us.

### When the CPU saving becomes fewer instances, and when it does not

Three things stand between a CPU ratio and a bill.

**Instance granularity.** AWS, Azure and GCP instance sizes double at each step, so a
continuous 2.86x advantage rounds to a staircase. In the one-million-events-per-second case
the q0 advantage rounds from 3.87x down to 2.00x, because clink's 1.02 vCPU of demand still
has to buy a whole 4-vCPU instance and sits 75% idle. At ten million per second the same
advantage rounds to 3.75x. **Below a few instances the granularity dominates the engine
difference entirely.** At small scale, and particularly for a single-instance pipeline, the
honest answer is that both engines cost the same.

**Memory, and whether it binds.** The measured working sets, expressed per vCPU of demand:

| query | engine | memory | vCPU demand | GiB per demanded vCPU |
|---|---|---:|---:|---:|
| q0 | clink | 76 MB | 1.02 | 0.07 |
| q0 | JVM engine | 1,169 MB | 3.93 | 0.29 |
| q12 | clink | 749 MB | 2.07 | 0.35 |
| q12 | JVM engine | 1,625 MB | 5.92 | 0.27 |

Every one of those is below the 2.0 GiB per vCPU that the cheapest compute-optimised family
supplies. **So on these two workloads the memory advantage converts to exactly zero direct
instance cost.** CPU is the binding constraint for both engines, and the 15.5x memory figure
buys headroom, not money. Any model that multiplies a memory saving by a dollars-per-GB rate
is wrong here.

Memory starts to matter at three points, and only then. First, if state per vCPU exceeds
2 GiB, the workload moves to a general-purpose family: m6a.xlarge is 4 GiB per vCPU at
$0.04320 per vCPU-hour, 12.9% more than c6a, and at that point instance count is set by RAM
rather than by CPU and the CPU saving stops reducing it. The memory ratio becomes the lever
instead. Second, packing density: a smaller resident set is how you fit more jobs per host,
which is a real saving that this scenario does not model because it prices one job. Third,
reliability: an under-set JVM memory limit OOMKills rather than throttles, so operators set
a floor with headroom and pay for the floor. That floor is a configuration decision and may
bear little relation to the 1,169 MB and 1,625 MB actually observed.

**Whether you actually resize.** This is the largest of the three and it is not a property of
either engine.

### The non-linearity, stated plainly

A naive model of the form "cores times watts" assumes power scales linearly from zero. It
does not. Take both engines on **the same fixed fleet**, sized for the heavier one, at one
million events per second:

| query | fleet | clink utilisation | clink W | JVM utilisation | JVM W | power ratio | CPU ratio |
|---|---|---:|---:|---:|---:|---:|---:|
| q0 | 2 x c6a.xlarge | 12.7% | 5.21 | 49.1% | 9.57 | **1.84x** | 3.87x |
| q12 | 3 x c6a.xlarge | 17.3% | 8.63 | 49.3% | 14.40 | **1.67x** | 2.86x |

**On a fixed fleet a 3.87x CPU saving is worth about 1.8x on power, and a 2.86x CPU saving
about 1.7x.** The idle floor is charged either way. Quoting the CPU ratio as the energy ratio
overstates the saving by roughly 1.7x to 2.1x, and it overstates it **in the direction that
flatters clink**, which is the worst kind of error to make in your own favour.

The saving only approaches the CPU ratio when the freed capacity is genuinely surrendered:
fewer instances, so the idle draw of the removed machines goes with them. The resized figures
in step 4 (3.83x and 2.82x) are close to the CPU ratios for exactly that reason.

So the answer depends on an operational decision, not on the engine, and both bounds should
be published:

- **If the freed headroom is taken as fewer nodes:** energy falls by roughly the CPU ratio,
  2.8x to 3.8x here.
- **If it is taken as more throughput per node:** energy falls by the dynamic-power
  difference only, 1.7x to 1.8x here, and the rest of the gain shows up as capacity.

There is a third case worth naming: the freed headroom gets filled with additional work, and
total energy does not fall at all. That is the rebound argument and it is not a strawman.

### Sensitivity

Which conclusions are robust and which are artefacts of an assumption. The critical
structural point first: **electricity price, PUE and grid carbon intensity all cancel out of
the ratio.** They multiply both engines identically, so they move only the absolute figures.
If any assumption swing below changes the ratio, that is flagged; most do not.

| assumption | central | plausible range | effect on the ratio | effect on absolutes |
|---|---|---|---|---|
| Instance price | $0.03825/vCPU-h (c6a on-demand) | $0.02360 (1yr RI, all upfront) to $0.05389 (c8a on-demand) | none | -38% to +41% |
| PUE | 1.14 (AWS, CY2025) | 1.09 (Google TTM 2025) to 1.54 (Uptime 2025 industry) | none | -4% to +35% on kWh and CO2e |
| Grid intensity | 0.271 kg/kWh (eGRID2023 SRVC) | 0.110 (NYUP) to 0.473 (RMPA) across data-centre subregions | none | 0.41x to 1.75x on CO2e |
| Average vs marginal grid factor | average (0.271) | SRVC non-baseload 0.587 | none | 2.17x on CO2e |
| min:max watts per vCPU | 0.46 : 1.96 (EPYC 3rd Gen) | 0.48 : 1.59 (measured 2x7763) to 0.58 : 2.53 (Granite Rapids) | **material at fixed fleet:** a higher idle share shrinks the ratio | ±25% on kWh |
| Utilisation target | 70% | 50% to 90% | small; both sides scale together | inversely proportional |
| **Consolidation** | fleet resized | fixed fleet | **large: 3.83x falls to 1.84x (q0), 2.82x to 1.67x (q12)** | large |
| **Scale** | 1M events/s | 1M to 10M+ | **large at small scale: q0 cost ratio 2.00x at 1M/s, 3.75x at 10M/s** | proportional |
| Memory binding | not binding (max 0.35 GiB/vCPU) | binds above 2.0 GiB/vCPU | **changes which resource sets instance count** | +12.9% per vCPU on m-family |
| Self-hosted electricity price (not used above) | 8.85 c/kWh (EIA, US industrial, rolling 12m to May 2026) | 6 to 22 c/kWh site-dependent | none | factor of 3 to 4 on a self-hosted bill |

Robust across the whole table: **cost and energy both fall, by a factor of roughly two to
four, on these two query shapes, when the fleet is resized.** No assumption swing in the
table reverses the direction or takes the ratio below about 1.7x.

Not robust: any absolute kWh, dollar or CO2e figure to better than a factor of two; any ratio
at all at single-instance scale; anything on a fixed fleet above about 1.8x.

### What would make this wrong

A hostile reader should find their objection here, already stated.

**The extrapolation problem, which is the most serious, and it is now measured on real
hardware - with a result that cuts against the headline.** A multi-node rig (1 control node,
3 worker nodes, an isolated broker, 18 dedicated vCPU) swept BOTH engines across parallelism
4, 8 and 12. Beyond parallelism 4 the job spans worker hosts, so this is the first measurement
containing any cross-host cost. The efficiency ratio decays, and on the windowed query it
nearly disappears:

| clink / JVM engine | parallelism 4 | parallelism 8 | parallelism 12 |
|---|---:|---:|---:|
| q0 (stateless) | 3.44x | 2.84x | 2.60x |
| q12 (windowed) | **2.00x** | **1.13x** | **1.15x** |

clink degrades faster than the JVM engine as the job fans out: on q12 its CPU-per-event is
2.55x worse at parallelism 12 than at 4, against the JVM engine's 1.47x.

The cause is identified and it is a clink defect rather than a property of scale. On a
parallelism-12 run of q0 - a query whose only edges are forward edges, with no shuffle at all -
**67% of data-plane edges were serialised over a socket instead of being in-process pointer
handoffs**, because task placement is greedy first-fit per task and does not co-locate the
operators of one parallel pipeline instance on one host. The JVM engine avoids this by design
through slot sharing. It is a scheduling fix, and until it lands, **the figures at the top of
this page should be read as a single-worker best case.**

An earlier single-host sweep, kept here because the contrast is the whole point, found clink's
q0 CPU-per-event flat across parallelism (1.05x from parallelism 1 to 8):

| parallelism | 1 | 2 | 4 | 8 | |
|---|---:|---:|---:|---:|---|
| q0 events per vCPU-second | 751,566 | 733,198 | 718,563 | 715,706 | **1.05x, flat** |
| q12 events per vCPU-second | 599,002 | 469,361 | 405,862 | 281,911 | **2.1x worse** |

The stateless shape is flat: fanning out costs essentially nothing per event. The windowed
shape degrades 2.1x from parallelism 1 to 8, and all of the loss is in the keyed shuffle,
whose gather runs once per (batch, destination) so its per-row cost grows with the destination
count. That is inherent to a keyed shuffle; the constant factor is an engineering target, the
growth is not.

**So the scenario above understates clink's own per-event cost at higher parallelism**, and it
does so on the windowed shape only. It is not known whether the RATIO against the JVM engine
holds, because that sweep was clink-against-clink and the JVM engine was not measured across
parallelism. That is a real and unclosed gap, and until it is closed the multi-instance figures
in this section should be read as an upper bound on the windowed advantage.

Separately, and unchanged: the rig was one engine node at parallelism 4, with the broker on a
second node. Every shuffle was intra-node. The
measurement therefore contains **zero cross-node data-plane cost for either engine**: no
serialisation across hosts, no cross-host barrier alignment, no distributed checkpoint
coordination, no rescale, no failure recovery, no straggler effects, no rack or spine network
energy. Those are precisely the terms that diverge between engines at scale. Scaling this to
a fleet assumes linear scaling in node count, no coordination growth, an identical query mix,
identical per-node utilisation, and the same relative advantage on cross-node shuffle as on
intra-node shuffle. A single-node measurement cannot estimate contention or coherency terms
at all: fitting them needs several load points, and one point leaves zero degrees of freedom.
Those assumptions are not weakly supported. They are **entirely unconstrained by this data.**
The scenario above is best read as N copies of a measured single-node result, not as a
measurement of a cluster.

**Two query shapes, one workload family.** q0 is a projection passthrough and q12 is a
windowed GROUP BY. No joins, no large-state workloads, no CEP, no late data, no
checkpoint-under-load, no rescale. The functional unit is "per nexmark q0 or q12 event", not
"per event". Relative position on a wide-state interval join is unmeasured and could go
either way.

**The JVM engine was untuned.** Upstream image, harness configuration, one change made in its
favour. That is a defaults-against-defaults comparison, which is a legitimate thing to
measure and is **not** a claim about what a tuned deployment achieves. Object reuse, managed
memory fractions, network buffers, serialiser choice, GC selection and state backend all move
JVM CPU and heap. The memory figure is the most affected: 1,169 MB resident on a stateless
projection is substantially a heap-sizing and GC-policy outcome, and a JVM given a smaller
heap uses less memory and more CPU. Anyone who can produce a tuned counter-run should; the
harness is in the repository for that purpose.

**SMT allocation flatters clink.** cgroup CPU-seconds are hyperthread-seconds. The heavier
CPU user had more co-resident SMT pairs, so its thread-seconds were individually cheaper in
power terms than the lighter user's. The same correction is applied to both engines above,
but the residual asymmetry is unquantified and it runs in clink's favour.

**Utilisation-based power models are blind to memory intensity.** Two workloads at the same
CPU share with different DRAM traffic do not draw the same power. A columnar Arrow engine and
a JVM engine differ substantially in cache behaviour and DRAM traffic. Nothing in a
CPU-seconds measurement resolves this, and it could move the energy result in either
direction.

**Coefficient quality.** CCF publishes point estimates with no confidence intervals, states
it cannot guarantee their accuracy, and its upstream methodology has not been revised since
2023 even though the coefficient data was refreshed in April 2026. eGRID2023 carries data
year 2023, three years stale, and staleness does not run in the conservative direction: US
power sector CO2 rose 4% in 2025 on 3% more generation, so 2026 intensity is plausibly at or
slightly above the 2023 value. SPECpower configurations are tuned for efficiency by their
sponsors and are not fleet-representative.

**Host generation is not fully pinned.** The rig's CPU flags place it on Zen 3 (AVX2 and BMI2
present, AVX-512 absent), but the provider does not publish per-instance CPU models and
states its dedicated-vCPU fleet runs more than one EPYC generation. Carry roughly 25%
uncertainty on the per-vCPU power coefficient on those grounds alone.

**Engine CPU is one line of a bill.** Broker, storage, checkpoint storage, egress,
observability and engineering time were not measured and are outside the declared boundary.
In many deployments they exceed engine compute, which caps how far a 2.86x engine advantage
can move a total cost of ownership.

**Run-to-run variance.** Throughput ratios moved tens of percent between provisions of the
same machine type. CPU-per-event and memory held across those runs, which is why the model is
built on those two and not on throughput. That is a stated observation across a small number
of runs, not a variance figure with an n and a spread, and it should be read as such.

### Inputs and sources

Every coefficient, visible and swappable. Substitute your own and rerun the arithmetic.

| input | value used | plausible range | source | status |
|---|---|---|---|---|
| events per vCPU-second, q0 | clink 983,957 / JVM 254,425 | see run-to-run note | this page, 27 July 2026, commit `764e570` | **measured** |
| events per vCPU-second, q12 | clink 482,940 / JVM 168,993 | see run-to-run note | as above | **measured** |
| memory, anon, q0 / q12 | clink 76 / 749 MB; JVM 1,169 / 1,625 MB | JVM figure is heap-config dependent | as above | **measured** |
| vCPU basis | 1 vCPU = 1 SMT thread | n/a | cgroup `usage_usec`; rig is 4 vCPU on 2 physical Milan cores | measured, host CPU model not published by provider |
| ingest rate | 1,000,000 events/s | 1M and 10M both shown | scenario choice | assumption |
| hours per month | 730 | 8,760/12 | convention | assumption |
| utilisation target | 70% | 50% to 90% | scenario choice | assumption |
| instance | c6a.xlarge, 4 vCPU / 8 GiB, EPYC 7R13 (Milan) | m6a.xlarge if memory binds | AWS Price List Bulk API, us-east-1, published 2026-07-24 | verified |
| price, on-demand | $0.15300/instance-h, $0.03825/vCPU-h | c8a $0.05389/vCPU-h | as above, Linux, shared tenancy, no pre-installed software | verified |
| price, 1yr Standard RI, all upfront | -38.3%, $0.02360/vCPU-h | Compute Savings Plan 1yr: -20.5% to -33.1% | as above | verified (product), derived (multiplication) |
| watts per vCPU, min / max | 0.46 / 1.96 W, AMD EPYC 3rd Gen | 0.48 / 1.59 (measured 2x7763) to 0.58 / 2.53 (Granite Rapids) | Cloud Carbon Footprint AWS coefficient set, SPECpower data to 2026-03-09 | verified, point estimate with no confidence interval |
| power cross-check | 122 W idle / 406 W at 100%, 2x EPYC 7763, 256 threads | second unit on same CPU: 94.6 W / 460 W | SPECpower_ssj2008, ASUSTeK RS700A-E11-RS4U, tested 16 June 2022, AC input at the wall | verified |
| power model shape | `min + U x (max - min)` | measured curve is convex, not linear | Cloud Carbon Footprint methodology | verified (model), approximation (shape) |
| PUE | 1.14 | 1.09 (Google TTM 2025) to 1.54 (Uptime 2025, n=681) | AWS sustainability disclosure, calendar 2025, metered to ISO/IEC 30134-2 | verified |
| grid carbon intensity | 0.271 kg CO2e/kWh | 0.110 (NYUP) to 0.473 (RMPA); marginal SRVC 0.587 | EPA eGRID2023 rev2, Table 1, SRVC total output rate, data year 2023 | verified, three years stale, latest available |
| grid basis | location-based, average, combustion only, at the busbar | delivered basis is 0.282 (SRVC gross loss 4.2%) | as above | verified, basis stated deliberately |
| retail electricity price (not used) | 8.85 c/kWh, US industrial, rolling 12m to May 2026 | 6 to 22 c/kWh site-dependent | EIA Electric Power Monthly Table 5.3, 11 of 12 months preliminary | verified, for self-hosted readers only |
| embodied carbon (M) | **not modelled** | unknown | no product carbon footprint or host resource total published for this hardware | **omitted, flagged** |
| broker, storage, egress, CI | **not modelled** | unknown | outside declared boundary | **omitted, flagged** |

### What you can take from this, and what you cannot

**You can take this.** For a nexmark-shaped stream pipeline on AMD EPYC Milan class hardware,
sized from measured CPU-per-event with a 70% utilisation target and a resized fleet:

| | range | central |
|---|---|---|
| Cloud instance-hours | 2.0x to 3.8x fewer | 2.75x (windowed) to 3.75x (stateless) at multi-instance scale |
| Modelled energy | 1.7x to 3.8x lower | 2.8x (windowed) to 3.8x (stateless) if the fleet shrinks; 1.7x to 1.8x if it does not |
| Modelled CO2e | tracks energy exactly | same ratios; the grid factor cancels |
| Absolute energy | 0.7 to 4.1 kWh per trillion events | small in absolute terms, and the honest headline is capacity, not tonnage |

The cost track is close to measurement. The energy track is a model whose largest uncertainty
is not the emissions factor, the PUE or the price, all of which cancel from the ratio, but
whether the operator resizes the fleet.

**You cannot take this.** Not a claim about any query shape other than a stateless projection
and a windowed GROUP BY. Not a claim about a tuned JVM deployment. Not a claim about a
multi-node cluster, because the measurement contains no cross-node cost for either engine.
Not a measured energy figure, because no wall power was measured. Not an SCI score, because
the embodied term is omitted and the boundary is narrower than the specification requires.
Not a fleet-scale or industry-scale tonnage, under any multiplication.

And not a ratio at single-instance scale. If your pipeline fits on one instance today, it
will fit on one instance with either engine, and the saving is zero until you outgrow it.

## The full nexmark suite, 17 queries

The two headline queries above are the ones with the longest measurement history. This
section reports the whole bid-only nexmark suite on one provision, measured 28 July 2026
at clink `438cb93` against Flink 2.2.0 re-baselined alongside, parallelism 4, two trials
each, 9.2 million canonical nexmark bid records. Raw output:
[`assets/nexmark-full-2026-07-28.json`](assets/nexmark-full-2026-07-28.json).

**CPU per event, and memory.** Mean of two trials. Higher is better for events/cpu-second;
the memory column is how many times *less* anonymous memory clink held.

| query | what it does | clink ev/cpu-s | JVM engine | CPU ratio | clink MB | JVM MB | memory |
|---|---|---:|---:|---:|---:|---:|---:|
| q0 | projection | 1,200,754 | 252,138 | **4.76x** | 70 | 1,159 | **16.4x less** |
| q1 | currency conversion | 490,410 | 243,870 | **2.01x** | 301 | 1,161 | 3.9x less |
| q2 | filter on an expression | 1,002,749 | 258,320 | **3.88x** | 78 | 1,206 | **15.5x less** |
| q7 | windowed max/min | 1,043,853 | 194,464 | **5.37x** | 78 | 1,241 | **15.9x less** |
| q11 | session windows | 370,776 | 43,584 | 8.51x † | 562 | 1,804 | 3.2x less |
| q12 | windowed group-by | 424,206 | 68,970 | **6.15x** | 534 | 1,612 | 3.0x less |
| q14 | expression filter + CASE | 403,237 | 233,170 | **1.73x** | 425 | 1,187 | 2.8x less |
| q15 | count-distinct per window | 739,880 | 120,238 | **6.15x** | 428 | 1,471 | 3.4x less |
| q16 | count-distinct per channel | 528,206 | 138,432 | **3.82x** | 410 | 1,474 | 3.6x less |
| q17 | per-auction aggregates | 417,708 | 52,605 | **7.94x** | 1,071 | 1,681 | 1.6x less |
| q18 | dedup per key | 410,726 | - ‡ | - ‡ | 3,756 | - ‡ | not comparable ‡ |
| q19 | top-10 per auction | 324,560 † | - ‡ | - ‡ | 2,234 | - ‡ | not comparable ‡ |
| q21 | CASE over a string | 484,356 | 229,988 | **2.11x** | 360 | 1,182 | 3.3x less |
| q22 | string splitting | 467,011 | 224,210 | **2.08x** | 464 | 1,180 | 2.5x less |
| qcum | cumulate windows | 436,716 | 47,078 | **9.28x** | 688 | 1,832 | 2.7x less |
| qhop | hopping windows | 414,232 | 32,944 | **12.57x** | 903 | 1,843 | 2.0x less |

† trial-to-trial spread above 1.25x on clink's side (q11 1.42x, q19 1.72x) - indicative
rather than firm.

‡ **the JVM engine has no comparable q18/q19 figures, and an earlier revision of this
table published some anyway.** On both queries its source counter stopped advancing at a
deterministic point - 1,083,142 and 1,084,008 records on q18's two trials, within 0.1% -
and after six quiet seconds the sampler gave up with 12-21% of the input ingested. The
figures this table briefly showed were then doubly wrong: efficiency divided the FULL
9.2M events by the CPU of that truncated window (inflating the JVM figure roughly 8x, in
its favour), and the memory column compared clink's footprint holding ALL 9.2M keys
against the JVM engine's holding an eighth of them. Both cells are withdrawn rather than
recomputed: a truncated window's CPU is dominated by deploy and warm-up, so no honest
number exists for it. The recording harness now withholds efficiency on truncated runs.
The likely cause of the stall is the image-default 1728 MB TaskManager memory meeting
9.19 million keys of dedup state - consistent with every JVM memory reading in this table
sitting at 1.1-1.8 GB - and the next rig run raises it to find out.

**Where clink's memory goes on q18 and q19, measured rather than compared.** Dedup over
this dataset retains nearly the whole input: 9,193,877 distinct (bidder, auction) pairs
across 9.2M bids, one row kept per pair. clink's 3.7 GB is therefore ~428 bytes per
retained row - including pipeline buffers - against a 124-byte serialized payload, so the
in-memory representation costs about 3.4x the data it holds. That is a real overhead with
a bounded remedy (storing retained rows encoded rather than materialized, estimated
25-35% of the state back at some hot-path cost), and it is parked with that accounting
rather than fixed here. It is NOT the pathological per-key overhead this page documents
for the windowed-aggregate state, and the earlier claim in this section that clink "holds
three times more memory than the JVM engine" on these queries is withdrawn - this data
cannot establish a cross-engine ratio the JVM side never finished ingesting.

**Why there is no throughput column.** The sampler's sustained-slope figure was not stable
enough on the JVM engine to publish per query: two identical trials of q2 gave 10.05M and
2.52M records per second, q1 gave a 3.53x spread and q7 1.91x, while the same engine's
drain rate stayed near 1.0M throughout. clink's own throughput was stable everywhere
(worst spread 1.12x), but a ratio is only as good as its weaker half. CPU per event held
within 1.15x across 31 of the 33 query-engine pairs, which is why this table reports that
instead.

**q5 is absent from clink's column** because it never ran: its plan is 36 tasks at
parallelism 4 and the rig's engine node has a 16-slot worker. That is a rig capacity limit,
not an engine result, and it is the one query in the suite with no clink figure here.

**These are blackhole-sink runs**, so nothing in them checks that either engine produced
the right answer - a throughput or efficiency win from an engine doing *less* work would
look identical to a real one. Correctness is gated separately and independently, and on the
same commit: 16 of 16 append-only queries and 4 of 4 changelog queries produce output the
two engines agree on, row for row. See
[the correctness gate](#the-correctness-gate).

## The multi-node baseline: 17 queries, parallelism 12, shuffles across real hosts

Everything above ran on the two-node split rig, where every shuffle stayed inside one
worker. This section is the first **multi-node** baseline: a control node, three worker
nodes and an isolated broker, parallelism 12, so every keyed shuffle crosses real hosts
and the numbers include the cross-host data plane both engines actually pay in a
deployment. Measured 29 July 2026 at clink `6cc4831` against Flink 2.2.0 re-baselined
alongside, three trials per cell, 9.2 million canonical nexmark bid records over 12
partitions. CPU and memory are summed over **every** engine node. Raw output:
[`assets/nexmark-full-topo-2026-07-29.json`](assets/nexmark-full-topo-2026-07-29.json).

**This is a new baseline, not an update to the tables above.** Different topology,
different parallelism, and one deliberate premise change: Flink's TaskManager memory is
set to 6 GB per node (the image default, 1728 MB, is what every earlier round ran - see
the q18/q19 note below for why that had to change). Flink's memory column here is
therefore not comparable to the split-rig rounds. clink's configuration is unchanged
apart from a scheduling-slots cap raised to fit q5's plan.

| query | what it does | clink ev/cpu-s | JVM engine | CPU ratio | clink MB | JVM MB |
|---|---|---:|---:|---:|---:|---:|
| q0 | projection | 857,676 | 179,327 | **4.78x** | 184 | 8,283 |
| q1 | currency conversion | 403,644 | 176,093 | **2.29x** | 430 | 8,285 |
| q2 | expression filter | 741,563 | 180,035 | **4.12x** | 188 | 8,319 |
| q5 | hot items (windowed top-1) | 109,072 | 48,611 | **2.24x** | 3,284 | 9,021 |
| q7 | windowed max/min | 669,953 | 138,404 | **4.84x** | 262 | 8,425 |
| q11 | session windows | 160,031 | 55,890 | 2.86x † | 1,091 | 9,863 |
| q12 | windowed group-by | 183,344 | 67,278 | **2.73x** | 1,050 | 9,279 |
| q14 | filter + CASE | 321,492 | 170,965 | **1.88x** | 558 | 8,265 |
| q15 | count-distinct / window | 564,148 | 106,484 | **5.30x** | 723 | 8,962 |
| q16 | count-distinct / channel | 338,020 | 103,167 | **3.28x** | 817 | 8,715 |
| q17 | per-auction aggregates | 149,708 | 62,178 | **2.41x** | 1,871 | 9,551 |
| q18 | dedup per key | 144,862 | 76,654 | **1.89x** | 5,656 | 10,926 |
| q19 | top-10 per auction | 132,036 | 67,163 | **1.97x** | 3,710 | 10,961 |
| q21 | CASE over a string | 399,657 | 173,249 | **2.31x** | 500 | 8,249 |
| q22 | string splitting | 397,328 | 161,952 | **2.45x** | 561 | 8,240 |
| qcum | cumulate windows | 124,248 | 55,642 | **2.23x** | 1,728 | 9,313 |
| qhop | hopping windows | 109,444 | 41,904 | 2.61x † | 2,321 | 9,406 |

† clink's own trial spread exceeded 1.25x on q11 (1.28x) and qhop (1.29x); treat those two
ratios as indicative. Every other cell held within 1.08x over three trials, both engines.

**Every query drained, on both engines, in every trial** - the first round of which that
is true. Three things this baseline settles:

- **q5 has its first cross-engine figure** (2.24x). It was never a capacity problem on
  clink's side: its 10-operator plan at parallelism 12 is 120 tasks, and the workers'
  scheduling cap was 48 slots across the rig. Raising the cap is configuration, not
  hardware - the rig's cores are unchanged.
- **The Flink q18/q19 stall from 28 July is explained and closed: it was memory.** At
  the image-default 1728 MB TaskManager size its source stalls at a deterministic point
  (~1.08M of 9.2M records) and never drains; at 6 GB it drains every trial. The earlier
  round's withdrawal of those figures stands - and with both engines finally draining,
  the honest comparison is 1.89x/1.97x CPU in clink's favour with clink holding roughly
  half the memory (5.7 vs 10.9 GB summed across nodes on q18) - a far narrower memory
  margin than the stateless queries, consistent with the retained-state accounting in
  the sections above.
- **The efficiency lead survives the network.** The split rig could not say whether
  clink's per-event advantage was an artifact of intra-node shuffles; at parallelism 12
  across three hosts the ratios run 1.88x to 5.30x, every query in clink's favour.

Same standing caveats as every round: blackhole sinks (nothing here checks output;
correctness is gated separately - 16 of 16 append-only and 4 of 4 changelog queries
agree row-for-row across engines), and throughput is not reported per query for the
reasons established in the previous section.

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

### Re-verified 28 July 2026, ratios only

clink at commit `438cb93` on a fresh 2x ccx23 provision in `nbg1`, Flink 2.2.0 re-baselined
alongside, parallelism 4, two trials each. Raw output:
[`assets/efficiency-2026-07-28.json`](assets/efficiency-2026-07-28.json).

| | clink | JVM engine | ratio |
|---|---:|---:|---:|
| CPU per event, q0 | 915,423 events/cpu-second | 224,555 | **4.08x less CPU** |
| CPU per event, q12 | 589,366 events/cpu-second | 151,490 | **3.89x less CPU** |
| Memory, q0 | 74 MB | 1,160 MB | **16.3x less** |
| Memory, q12 | 92 MB | 1,533 MB | **16.7x less** |

Why this is published as ratios and not as a replacement for the headline table: the rig
scripts do not load the Kafka topic, and the engine node had no Linux clink build to
generate a nexmark dataset with, so the topic was filled by `kafka-producer-perf-test`
cycling a 20,000-row bid-shaped payload file to 9.2 million records. Both engines read the
identical topic on one provision, which is what a ratio needs, so the ratios stand. But
repeated payloads decode and cache differently from 9.2 million distinct records - clink's
q0 reads 3.97M records per second here against 1.86M on the canonical dataset - so the
absolute rates describe an easier workload and are not comparable to the rounds above.
They are recorded here rather than promoted into the headline table for that reason.

The run also matters for what preceded it. It is the first measurement taken after
`b791306`, which fixed a data batch larger than the send-credit window being silently
dropped on any shuffle that crossed a worker boundary - so any earlier figure for a query
whose large batches crossed that boundary was measured on a pipeline that could lose
records. The q12 memory figure moving from 749 MB to 92 MB across the two rounds is
mostly the different dataset (far fewer distinct grouping keys in a cycled payload), not
an engine change, and should not be read as a 8x memory improvement.

An earlier round of measurements on a single machine was discarded rather than published:
with the broker and both engines sharing cores, clink's q0 was recorded at both 1.06M and
571k records per second for the same configuration. That spread was wider than the effect
being measured, which is why the two-node rig above exists.
