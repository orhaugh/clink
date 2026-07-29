# Efficiency and environmental impact

Stream processing runs continuously. A pipeline that ingests events all day occupies
its cores and its memory all day, so the cost of processing one event is paid millions
of times over, and it shows up as machines provisioned, power drawn and money spent.

This page publishes what clink measured against a JVM stream processor on identical
hardware doing identical work, and is explicit about what those measurements do and do
not establish. Every figure comes from one run - the current baseline, whose method,
commit and raw output are recorded below; nothing is modelled or extrapolated except
where a section says so. Earlier rounds are superseded and live only in
[Provenance](#provenance).

## The headline results

The whole bid-only nexmark suite - 17 queries - on a five-node cluster: a control node,
three worker nodes and an isolated broker, parallelism 12, so every keyed shuffle
crosses real hosts and the numbers include the cross-host data plane both engines pay in
a real deployment. Three trials per cell, and **every query drained on both engines in
every trial**. CPU and memory are summed over every engine node.

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

**clink uses less CPU per event on all 17 queries: 1.88x to 5.30x, median 2.45x, geometric
mean 2.79x.** The lead is largest where the engine dominates (stateless and
lightly-stateful shapes) and narrowest where the query's own retained state dominates
(dedup and ranking), which is what any honest engine comparison should look like.

† clink's own trial-to-trial spread exceeded 1.25x on q11 (1.28x) and qhop (1.29x); treat
those two ratios as indicative. Every other cell held within 1.08x over three trials, on
both engines.

**Read the memory columns as absolute footprints, not as a ratio.** clink's column is
what it actually held, 184 MB to 5.7 GB summed across four nodes. The JVM engine's column
reflects a deliberate configuration change made in its favour: its TaskManager memory had
to be raised from the image-default 1728 MB to 6 GB per worker, because at the default its
source stalls on the two dedup/ranking queries and never finishes (see
[Where the memory difference comes from](#where-the-memory-difference-comes-from)). A JVM
given more memory uses it, so a memory ratio against that column would partly measure the
allocation rather than the need. What the run does establish: clink completed the entire
suite within 5.7 GB at its heaviest; the JVM engine could not complete it within 5.2 GB of
TaskManager memory (3 x 1728 MB).

There is deliberately no per-query throughput column. clink's sustained throughput was
stable (its q0 read ~11.3M records/s at parallelism 12, worst trial spread 1.12x across
the suite), but the JVM engine's sampled throughput has shown multi-x spreads on identical
trials in earlier rounds while its CPU-per-event held within a few percent - so
CPU-per-event is the durable figure and the one this page reports. Treat throughput as
"ahead", not as a number.

## How it was measured

**The workload.** All 15 bid-only nexmark queries plus two window-kind fillers (`qhop`,
`qcum` - hopping and cumulate windows, which nexmark itself never exercises), over
9.2 million canonical nexmark bid records in a 12-partition Kafka topic. The dataset is
generated on the rig by clink's own deterministic generator (the same generator the
cross-engine harness's dump tool uses, seed 1), so both engines read identical bytes.
Output is discarded on both sides so the measurement is the engine, not a downstream
connector.

**The hardware.** Five dedicated-vCPU cloud nodes on one private network: a 2-vCPU
control node, three 4-vCPU worker nodes running whichever engine is being measured, and a
4-vCPU broker node. Dedicated rather than shared vCPU, because noisy neighbours were the
largest source of error in an earlier single-machine attempt. The broker is on its own
node so its CPU is never charged to, or subsidised by, either engine. Host networking
throughout: a cross-node shuffle is a plain TCP connection, not a bridge-NAT hop added by
the rig.

**One engine at a time, on the same nodes.** The engines never run together. Each gets
the same three worker nodes, the same broker, the same records, within the same session,
and the JVM engine is re-baselined on this provision rather than compared against a
figure from another one.

**Matched shapes, and two declared changes in the JVM engine's favour.** It ran 1
JobManager and 3 TaskManagers (4 slots each); clink ran 1 coordinator and 3 workers. Its
metrics fetch interval was lowered from the 10-second default to 200 ms, because at the
default every rate computed for it is quantised and the engine is reported below its true
speed. And its TaskManager memory was raised to 6 GB per node, without which it cannot
finish q18/q19 at all. clink ran its stock configuration; its workers' scheduling-slot
cap was raised from 16 to 48, which changes which plans are schedulable and nothing about
how anything runs.

**Three trials per cell, fresh stack per trial.** Every measured run got a newly composed
cluster and a fresh consumer group. A reused cluster drifts upward in CPU as it warms
(2.6x by the sixth job on this harness); a reused consumer group resumes at its committed
offset and measures an empty topic.

**CPU is read from the OS, not from the engine.** Per-process CPU for every engine
container on every engine node, summed - so it includes the JVM's own threads and clink's
client-library threads alike, and neither engine can push work onto a node the meter
misses. Memory is cgroup `anon`, summed the same way: heap and stacks, excluding page
cache, which is the kernel's to reclaim.

### The correctness gate

An efficiency comparison is meaningless if the two engines are not doing the same work,
and a blackhole-sink measurement checks nothing by itself - an engine doing *less* work
would simply look faster. Correctness is gated separately, on the same engine lineage:
every append-only query must produce an identical output row count on both engines, and
every changelog query must converge to an identical final state.

```
append-only  16 of 16 queries identical   e.g. q0 460,000 = 460,000; qhop 1,493,218 = 1,493,218
changelog     4 of 4 final states match   e.g. q18 455,895 rows; q19 215,613 rows,
                                          with tombstone counts byte-identical (119,926 = 119,926)
```

Not approximately equal. Identical. The efficiency figures above are for engines
producing the same answer.

## Where the CPU difference comes from

clink is a native binary. There is no bytecode interpreter, no just-in-time compiler
warming up, and no garbage collector scanning a heap while records flow. Data moves
through the engine as Apache Arrow column batches, so a batch of a thousand records is
one allocation and one pass rather than a thousand objects.

A second, measurable consequence is that clink reaches its full rate almost at once. On
the two-node rig this was quantified directly - the gap between an engine's peak
sustained rate and its whole-run average is warm-up and taper, and clink ran at 95% of
its peak over the whole run against the JVM engine's 42%. For a long-running pipeline
this matters little. For anything that starts, stops, scales or fails over, it is the
difference between paying for warm-up repeatedly and not.

The engine's own instrumentation shows where its remaining time goes, and it is not the
query: on q0 the projection the query actually asks for is a low-single-digit percentage
of worker CPU, while JSON decoding and the Kafka source dominate. That is published in
full in
[the benchmark record](https://github.com/orhaugh/clink/blob/main/benchmarks/nexmark_compare/cloud/README.md),
including the optimisations that were tried and measured to be worthless.

## Where the memory difference comes from

Three facts, in decreasing order of how flattering they are, all measured:

**On stateless work clink's whole engine is a rounding error: 184 MB across four nodes**
against the JVM engine's 8.3 GB of configured-and-used allocation. No heap to size, no
metaspace, no collector headroom, no object header per record. This is the figure that
generalises to any pipeline whose working set is small.

**On state-heavy work the gap is real but much narrower, and most of clink's footprint is
the query's own state.** The controlled version of this claim comes from a group-count
experiment on the two-node rig: hold the records and the window fixed, vary only the
number of groups, and the per-group cost separates from the pipeline's fixed cost. That
experiment put clink's per-group state at 1,977 bytes for an int64 key and a count -
down from 3,914 after an accumulator defect was fixed, ahead of the JVM engine's
equivalent by about 1.2x, and still more than an int64 and a counter need. The remainder
is container overhead, and the dedup/ranking operators additionally store each retained
row encoded rather than materialised (measured 25-29% per-row saving on the platform the
rig runs). Read clink's q18 row - 5.7 GB for 9.19 million retained rows - as "clink
today", not "clink at its floor".

**The JVM engine's memory requirement is a cliff, not a slope.** At its image-default
1728 MB TaskManager size it does not finish q18 or q19: its source counter stops at a
deterministic point (~1.08M of 9.2M records) and never advances again. Raised to 6 GB it
finishes every trial. That behaviour - work that silently stalls rather than degrades
when memory is short - is why its memory column in the headline table reflects
configuration as much as need, and why no memory ratio is published from this run.

## Translating this into a footprint

**clink has not measured wall power, and nothing below changes that.** No kWh or CO2e
figure on this page is a measurement. What follows is arithmetic over published
coefficients, every one named and swappable, so a reader who disagrees with an input can
substitute it and get their own answer.

The measurements above stop at CPU-seconds and megabytes on purpose. This section prices
them, because "2.7x less CPU" is not a number anyone budgets in. It uses the two
longest-measured query shapes - q0 (stateless, 4.78x) and q12 (windowed group-by, 2.73x) -
as the representative pair; the other fifteen ratios sit between 1.88x and 5.30x and a
reader can rescale.

**Track A, cost,** converts measured CPU into cloud instance-hours at published list
prices. It is close to measurement: the only modelled inputs are a utilisation target and
an instance choice.

**Track B, energy and CO2e,** needs a server power model, a PUE and a grid carbon
intensity. None were measured on the rig. Track B is a model throughout and is labelled
as such on every line.

### The boundary being priced

Fixed once, applied identically to both engines, and never widened or narrowed between
them:

> Steady-state operational cost and energy of the engine containers running one query on
> one node type. Excludes the broker, object storage, checkpoint storage, network egress,
> load balancers, observability, CI, container images, cold starts, and the embodied
> carbon of the hardware.

That boundary is narrower than the Software Carbon Intensity specification requires, so
this is **not an SCI score and should not be described as one.** The embodied term is
omitted because no product carbon footprint exists for the specific hardware; note the
omission works against clink, since the excluded terms include cold start and image size,
where clink's measured advantage is largest.

### The scenario

One million nexmark events per second, sustained, for one calendar month. Both query
shapes. Both engines sized from their own measured events per CPU-second. A month is 730
hours, so the month processes 2.628 trillion events. One million per second is a round
number a reader can rescale, not a claim about any particular pipeline.

#### Step 1: vCPU demand

A vCPU here is one SMT thread; the measurement counts thread-seconds and the rig's
dedicated vCPU sit on AMD EPYC Milan, as does the instance chosen in step 2, so the unit
is consistent on both sides.

| query | engine | events per vCPU-second (measured) | vCPU-seconds per wall second at 1M events/s |
|---|---|---:|---:|
| q0 | clink | 857,676 | 1.17 |
| q0 | JVM engine | 179,327 | 5.58 |
| q12 | clink | 183,344 | 5.45 |
| q12 | JVM engine | 67,278 | 14.86 |

#### Step 2: provisioned vCPU and instances

Nobody provisions to 100% of demand. The scenario targets **70% steady-state
utilisation**; divide demand by 0.7, round up to whole c6a.xlarge instances (4 vCPU /
8 GiB, AMD EPYC Milan, us-east-1 - the AWS family matching the measured hardware on
generation and SMT convention).

| query | engine | demand (vCPU) | at 70% target | instances (ceil) | provisioned vCPU | actual utilisation |
|---|---|---:|---:|---:|---:|---:|
| q0 | clink | 1.17 | 1.67 | **1** | 4 | 29.1% |
| q0 | JVM engine | 5.58 | 7.97 | **2** | 8 | 69.7% |
| q12 | clink | 5.45 | 7.79 | **2** | 8 | 68.2% |
| q12 | JVM engine | 14.86 | 21.23 | **6** | 24 | 61.9% |

The instance ratio is 2x on q0 and 3x on q12, against measured CPU ratios of 4.78x and
2.73x. That mismatch is not noise; it is instance granularity, discussed below.

#### Step 3: dollars

c6a.xlarge on-demand list is $0.15300 per instance-hour, $111.69 per instance-month.

| query | clink | JVM engine | difference | ratio |
|---|---:|---:|---:|---:|
| q0 | $111.69 | $223.38 | $111.69 | 2.00x |
| q12 | $223.38 | $670.14 | $446.76 | 3.00x |

At ten million events per second, where whole-instance rounding stops dominating:

| query | clink instances | JVM instances | clink $/month | JVM $/month | ratio |
|---|---:|---:|---:|---:|---:|
| q0 | 5 | 20 | $558.45 | $2,233.80 | 4.00x |
| q12 | 20 | 54 | $2,233.80 | $6,031.26 | 2.70x |

Normalised, at the ten-million-per-second scale: **q0 $21.25 against $85.00 per trillion
events; q12 $85.00 against $229.50 per trillion events.** A one-year Standard Reserved
Instance, all upfront, scales every dollar figure by 0.617 and **no ratio changes**.

#### Step 4: kWh, and this is where the model starts

Server power is not proportional to CPU utilisation. Cloud Carbon Footprint's model,
`watts per provisioned vCPU = min + utilisation x (max - min)`, with its AMD EPYC 3rd Gen
coefficients (min 0.46 W, max 1.96 W per vCPU) and AWS's reported fleet PUE of 1.14:

| query | engine | provisioned vCPU | utilisation | W per vCPU | server W | with PUE | kWh/month |
|---|---|---:|---:|---:|---:|---:|---:|
| q0 | clink | 4 | 29.1% | 0.90 | 3.59 | 4.09 | **2.99** |
| q0 | JVM engine | 8 | 69.7% | 1.51 | 12.04 | 13.73 | **10.02** |
| q12 | clink | 8 | 68.2% | 1.48 | 11.86 | 13.52 | **9.87** |
| q12 | JVM engine | 24 | 61.9% | 1.39 | 33.34 | 38.00 | **27.74** |

Energy ratios: 3.36x on q0, 2.81x on q12 - not equal to the cost ratios, because after
rounding the engines sit at different utilisations and the idle floor spreads over
different vCPU counts. At ten million events per second: q0 22.2 vs 100.2 kWh/month
(4.51x); q12 98.7 vs 268.2 (2.72x). Per trillion events: q0 **0.85 against 3.81 kWh**;
q12 **3.76 against 10.21 kWh**.

#### Step 5: CO2e

Grid intensity 0.271 kg CO2e/kWh (EPA eGRID2023, SRVC, location-based average), applied
identically to both engines:

| query | scale | clink kg CO2e/month | JVM kg CO2e/month | difference |
|---|---|---:|---:|---:|
| q0 | 1M events/s | 0.81 | 2.72 | 1.91 |
| q12 | 1M events/s | 2.68 | 7.52 | 4.84 |
| q0 | 10M events/s | 6.02 | 27.16 | 21.14 |
| q12 | 10M events/s | 26.75 | 72.69 | 45.94 |

**The absolute numbers are small, and saying so is more useful than dressing them up.** A
ten-million-events-per-second windowed pipeline running all year is a modelled saving of
about 2,034 kWh and 0.55 tonnes CO2e. Real, and not large. The defensible headline from
this work is capacity and cost, not tonnage. Anyone multiplying a per-event figure up to
a global total is compounding every assumption in this section and should be disbelieved,
including if it is us.

### When the CPU saving becomes fewer instances, and when it does not

Three things stand between a CPU ratio and a bill.

**Instance granularity.** Instance sizes double at each step, so a continuous advantage
rounds to a staircase: q0's 4.78x rounds to 2.00x at one million events per second
(clink's 1.17 vCPU of demand still buys a whole instance and sits 71% idle) and to 4.00x
at ten million. **Below a few instances the granularity dominates the engine difference
entirely** - for a single-instance pipeline, both engines cost the same.

**Memory, and whether it binds.** Working sets per demanded vCPU: clink 0.15 GiB (q0) and
0.19 GiB (q12); the JVM engine 1.45 and 0.61 GiB. All below the 2.0 GiB per vCPU the
cheapest compute family supplies, **so on these workloads the memory advantage converts
to exactly zero direct instance cost** - CPU binds for both engines, and the memory
difference buys headroom and packing density, not money. It starts to matter when state
per vCPU exceeds the family's ratio (the workload moves to a costlier family and RAM sets
the instance count), when jobs are packed per host, and at the reliability floor
operators configure against OOM kills - a floor the q18/q19 stall shows the JVM engine
needs set generously.

**Whether you actually resize.** The largest of the three, and not a property of either
engine.

### The non-linearity, stated plainly

On **the same fixed fleet**, sized for the heavier engine, at one million events per
second:

| query | fleet | clink utilisation | clink W | JVM utilisation | JVM W | power ratio | CPU ratio |
|---|---|---:|---:|---:|---:|---:|---:|
| q0 | 2 x c6a.xlarge | 14.6% | 5.43 | 69.7% | 12.04 | **2.22x** | 4.78x |
| q12 | 6 x c6a.xlarge | 22.7% | 19.22 | 61.9% | 33.34 | **1.73x** | 2.73x |

**On a fixed fleet a 4.78x CPU saving is worth about 2.2x on power, and a 2.73x saving
about 1.7x.** The idle floor is charged either way; quoting the CPU ratio as the energy
ratio overstates the saving in the direction that flatters clink, which is the worst kind
of error to make in your own favour. The saving approaches the CPU ratio only when the
freed capacity is genuinely surrendered - fewer instances, so the idle draw goes with
them (the resized figures in step 4 are close to the CPU ratios for exactly that reason).
And there is a third case worth naming: the freed headroom gets filled with more work and
total energy does not fall at all. That is the rebound argument and it is not a strawman.

### Sensitivity

Electricity price, PUE and grid intensity all cancel out of the ratio - they multiply
both engines identically. What actually moves the conclusion:

| assumption | central | plausible range | effect on the ratio |
|---|---|---|---|
| Instance price | $0.03825/vCPU-h (c6a on-demand) | RI -38% to c8a +41% | none |
| PUE / grid intensity / electricity price | 1.14 / 0.271 kg/kWh / 8.85c | wide | none - cancels |
| min:max watts per vCPU | 0.46 : 1.96 (EPYC 3rd Gen) | 0.48 : 1.59 measured to 0.58 : 2.53 next-gen | material at fixed fleet: a higher idle share shrinks the ratio |
| **Consolidation** | fleet resized | fixed fleet | **large: 4.51x falls to 2.22x (q0), 2.72x to 1.73x (q12)** |
| **Scale** | 1M events/s | 1M to 10M+ | **large at small scale: q0 cost ratio 2.00x at 1M/s, 4.00x at 10M/s** |
| Memory binding | not binding (max 1.45 GiB/vCPU) | binds above 2.0 GiB/vCPU | changes which resource sets instance count |

Robust across the table: **cost and energy both fall, by a factor of roughly two to four
on these query shapes, when the fleet is resized.** No assumption swing reverses the
direction or takes the resized ratio below about 1.7x. Not robust: any absolute kWh,
dollar or CO2e figure to better than a factor of two; any ratio at single-instance scale;
anything on a fixed fleet above about 2.2x.

### What would make this wrong

A hostile reader should find their objection here, already stated.

**One parallelism point.** The baseline is parallelism 12 across three hosts - a real
multi-node measurement, which closes the biggest objection to earlier single-node rounds
(an earlier multi-node sweep found the windowed ratio collapsing to 1.15x, traced to a
clink task-placement defect that serialised even forward edges across sockets; that
defect was fixed before this baseline, which is consistent with the 2.73x measured here).
But this run is one parallelism point, not a sweep: how the ratio moves between
parallelism 4 and 12, post-fix, is not re-measured. Coordination-heavy terms - rescale,
failure recovery, checkpoint under load, stragglers - remain unmeasured at any
parallelism.

**Seventeen queries, one workload family.** The suite covers projections, filters,
windows of every kind, distinct counting, dedup and ranking - but it is all nexmark: JSON
over Kafka, bid-shaped records. No large-state interval joins, no CEP, no late data. The
functional unit is "per nexmark event", not "per event". The footprint scenario prices
two of the seventeen shapes.

**The JVM engine was untuned - with two exceptions in its favour, stated in the method.**
The metrics interval, and the 6 GB TaskManager memory without which it cannot finish two
queries. Object reuse, managed memory fractions, network buffers, serialiser choice, GC
selection and state backend all move JVM CPU and heap, and a tuned counter-run is
invited; the harness is in the repository for that purpose.

**SMT allocation flatters clink.** CPU-seconds are hyperthread-seconds; the heavier CPU
user had more co-resident SMT pairs, so its thread-seconds were individually cheaper in
power terms. The correction is applied identically to both engines, but the residual
asymmetry is unquantified and runs in clink's favour.

**Utilisation-based power models are blind to memory intensity.** A columnar Arrow engine
and a JVM engine differ substantially in cache behaviour and DRAM traffic, and nothing in
a CPU-seconds measurement resolves which way that moves the energy result.

**Coefficient quality.** CCF publishes point estimates with no confidence intervals;
eGRID2023 carries data year 2023 and staleness does not run in the conservative
direction; SPECpower configurations are sponsor-tuned. The rig's exact CPU model is not
published by the provider; carry roughly 25% uncertainty on the per-vCPU power
coefficient.

**Engine CPU is one line of a bill.** Broker, storage, egress, observability and
engineering time are outside the declared boundary and in many deployments exceed engine
compute, which caps how far any engine advantage moves a total cost of ownership.

**Run-to-run variance.** Throughput ratios have moved tens of percent between provisions
of the same machine type; CPU-per-event and memory held. The model is built on those two
for that reason, and the two cells where clink's own spread exceeded 1.25x are flagged in
the headline table rather than averaged into silence.

### Inputs and sources

Every coefficient, visible and swappable. Substitute your own and rerun the arithmetic.

| input | value used | source | status |
|---|---|---|---|
| events per vCPU-second, all 17 queries | headline table | this page, 29 July 2026, commit `6cc4831`, 3 trials/cell | **measured** |
| memory, anon, summed across engine nodes | headline table | as above; JVM column at 6 GB/TaskManager (required to finish q18/q19) | **measured, premise stated** |
| vCPU basis | 1 vCPU = 1 SMT thread | per-process CPU summed over engine containers; AMD EPYC Milan | measured |
| ingest rate / hours / utilisation target | 1M and 10M events/s / 730 h / 70% | scenario choice | assumption |
| instance + price | c6a.xlarge, $0.15300/h on-demand, us-east-1 | AWS Price List, 2026-07-24 | verified |
| watts per vCPU | 0.46 min / 1.96 max (EPYC 3rd Gen) | Cloud Carbon Footprint AWS coefficients | verified, point estimate |
| PUE | 1.14 | AWS disclosure, CY2025 | verified |
| grid intensity | 0.271 kg CO2e/kWh | EPA eGRID2023, SRVC, location-based average | verified, three years stale |
| embodied carbon, broker, storage, egress | **not modelled** | outside declared boundary | **omitted, flagged** |

### What you can take from this, and what you cannot

**You can take this.** For nexmark-shaped stream pipelines on AMD EPYC Milan class
hardware, sized from measured CPU-per-event with a 70% utilisation target and a resized
fleet:

| | range | central |
|---|---|---|
| CPU per event | 1.9x to 5.3x less, all 17 queries | median 2.45x, geometric mean 2.79x |
| Cloud instance-hours | 2.0x to 4.0x fewer | 2.7x (windowed) to 4.0x (stateless) at multi-instance scale |
| Modelled energy | 1.7x to 4.5x lower | 2.7x to 4.5x if the fleet shrinks; 1.7x to 2.2x if it does not |
| Modelled CO2e | tracks energy exactly | the grid factor cancels |

The cost track is close to measurement. The energy track is a model whose largest
uncertainty is not any coefficient - those cancel from the ratio - but whether the
operator resizes the fleet.

**You cannot take this.** Not a claim about workload families nexmark does not cover.
Not a claim about a tuned JVM deployment. Not a parallelism sweep - one point, twelve,
measured post-placement-fix. Not a measured energy figure, because no wall power was
measured. Not an SCI score. Not a memory ratio, for the configuration reason stated
under the headline table. Not a fleet-scale or industry-scale tonnage, under any
multiplication. And not a ratio at single-instance scale: if your pipeline fits on one
instance today, it fits on one instance with either engine, and the saving is zero until
you outgrow it.

## What was not measured

Stated plainly, because an efficiency page without this section should not be trusted:

- **Wall power, PUE, carbon intensity and embodied carbon.** None of it. See above.
- **Parallelism as a variable.** One point, 12, on this topology. The pre-fix sweep that
  showed the windowed ratio decaying with fan-out is obsolete (its cause was fixed) and
  has not been repeated post-fix.
- **Joins beyond nexmark's, CEP, late data, checkpoint-under-load, rescale, failure
  recovery.** The suite is bid-only nexmark plus two window-kind fillers.
- **A tuned JVM comparison.** Upstream image, harness configuration, two changes in its
  favour (metrics interval, TaskManager memory).
- **Throughput as a stable ratio.** CPU-per-event and memory hold across provisions;
  sampled throughput does not. "Ahead" is the durable claim.
- **A finished state representation.** clink's per-group window state is 1,977 bytes for
  an int64 key and a count; its dedup state ~390 bytes per retained row against a ~130
  byte payload. Both improved this month, neither is at its floor.
- **Long-run behaviour.** Minutes-long runs, not weeks.

## Reproducing it

The harness is in the repository, not described in prose:

```bash
cd benchmarks/nexmark_compare/cloud
TOPOLOGY=full LOCATION=nbg1 ./provision.sh
# broker: BROKER_PRIVATE_IP=<priv> docker compose -f split-broker.yml up -d  (on the broker node)
CONTROL_IP=<pub> CONTROL_PRIV=<priv> BROKER_PRIV=<priv> WORKER1="pub:priv:worker-1" \
  ./full-load-canonical.sh
CONTROL_IP=<pub> CONTROL_PRIV=<priv> BROKER_PRIV=<priv> \
  WORKERS="pub:priv:worker-1 pub:priv:worker-2 pub:priv:worker-3" \
  QUERIES="q0 q1 q2 q5 q7 q11 q12 q14 q15 q16 q17 q18 q19 q21 q22 qhop qcum" \
  PARS="12" EVENTS=9200000 REPEATS=3 CLINK_SLOTS=48 FLINK_TM_MEM=6g ./full-run.sh
./teardown.sh && ./teardown.sh --check
```

The correctness gates are separate harnesses (`gate.sh` for append-only row counts,
`upsert_gate.sh` for changelog final states), because gating on output equality and
measuring sustained CPU want different setups.

## Provenance

- **Measured** 29 July 2026, clink at commit `6cc4831`
  (`ghcr.io/orhaugh/clink-runtime:sha-6cc483137e07`), against Apache Flink 2.2.0 on
  Java 21 (upstream image), re-baselined on the same provision in the same session.
- **Rig** five Hetzner nodes in `nbg1`: ccx13 control, 3x ccx23 workers, ccx23 broker
  (dedicated vCPU, AMD EPYC), host networking over a private network.
- **Raw output** every figure on this page comes from the 102 per-run JSON files
  published alongside it:
  [`assets/nexmark-full-topo-2026-07-29.json`](assets/nexmark-full-topo-2026-07-29.json).
- **Full record**, including the harness defects this run found and fixed (a consumer
  group shared across trials measures an empty topic; the Flink job jar is a control-node
  prerequisite):
  [`benchmarks/nexmark_compare/cloud/README.md`](https://github.com/orhaugh/clink/blob/main/benchmarks/nexmark_compare/cloud/README.md).

**Earlier rounds are superseded by this baseline and kept only as raw assets**, because
their premises differ and mixing them produced exactly the additive, round-by-round page
this one replaced: the two-node split-rig rounds of 26-27 July
([`assets/efficiency-2026-07-26.json`](assets/efficiency-2026-07-26.json),
[`assets/efficiency-2026-07-27.json`](assets/efficiency-2026-07-27.json)); a 28 July
ratios-only re-verification on a synthetic cycled payload
([`assets/efficiency-2026-07-28.json`](assets/efficiency-2026-07-28.json)); and a 28 July
split-rig full-suite round
([`assets/nexmark-full-2026-07-28.json`](assets/nexmark-full-2026-07-28.json)) whose JVM
q18/q19 cells were withdrawn when its source turned out to have stalled at an eighth of
the input - the memory cliff documented above, found there, closed here. Two further
rounds never made this page at all: a single-machine attempt whose run-to-run spread
exceeded the effect being measured (why the isolated-broker rig exists), and any figure
predating 28 July for a query whose large batches crossed a worker boundary, because a
send-credit defect fixed that day (`b791306`) could silently drop them in transit.
