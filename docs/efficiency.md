# Cost and environmental footprint

This page prices the measured results - it contains no measurements of its own. The
inputs are the CPU-per-event and memory figures from
[Benchmarks](benchmarks.md), whose method, correctness gate and raw output are recorded
in [its provenance](benchmarks.md#provenance); this page turns them into instance
counts, dollars, and modelled energy and CO2e, with every coefficient named and
swappable.

**clink has not measured wall power, and nothing below changes that.** No kWh or CO2e
figure on this page is a measurement. What follows is arithmetic over published
coefficients, every one named and swappable, so a reader who disagrees with an input can
substitute it and get their own answer.

The measurements stop at CPU-seconds and megabytes on purpose. This page prices
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

## The boundary being priced

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

## The scenario

One million nexmark events per second, sustained, for one calendar month. Both query
shapes. Both engines sized from their own measured events per CPU-second. A month is 730
hours, so the month processes 2.628 trillion events. One million per second is a round
number a reader can rescale, not a claim about any particular pipeline.

### Step 1: vCPU demand

A vCPU here is one SMT thread; the measurement counts thread-seconds and the rig's
dedicated vCPU sit on AMD EPYC Milan, as does the instance chosen in step 2, so the unit
is consistent on both sides.

| query | engine | events per vCPU-second (measured) | vCPU-seconds per wall second at 1M events/s |
|---|---|---:|---:|
| q0 | clink | 857,676 | 1.17 |
| q0 | JVM engine | 179,327 | 5.58 |
| q12 | clink | 183,344 | 5.45 |
| q12 | JVM engine | 67,278 | 14.86 |

### Step 2: provisioned vCPU and instances

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

### Step 3: dollars

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

### Step 4: kWh, and this is where the model starts

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

### Step 5: CO2e

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

## When the CPU saving becomes fewer instances, and when it does not

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

## The non-linearity, stated plainly

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

## Sensitivity

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

## What would make this wrong

The measurement-side objections - parallelism coverage, workload family, tuning,
variance - are stated on [the benchmarks page](benchmarks.md#what-would-make-this-wrong).
These are the objections specific to the model:

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


## Inputs and sources

Every coefficient, visible and swappable. Substitute your own and rerun the arithmetic.

| input | value used | source | status |
|---|---|---|---|
| events per vCPU-second, all 17 queries | [benchmarks headline table](benchmarks.md) | 29 July 2026, commit `6cc4831`, 3 trials/cell | **measured** |
| memory, anon, summed across engine nodes | [benchmarks headline table](benchmarks.md) | as above; JVM column at 6 GB/TaskManager (required to finish q18/q19) | **measured, premise stated** |
| vCPU basis | 1 vCPU = 1 SMT thread | per-process CPU summed over engine containers; AMD EPYC Milan | measured |
| ingest rate / hours / utilisation target | 1M and 10M events/s / 730 h / 70% | scenario choice | assumption |
| instance + price | c6a.xlarge, $0.15300/h on-demand, us-east-1 | AWS Price List, 2026-07-24 | verified |
| watts per vCPU | 0.46 min / 1.96 max (EPYC 3rd Gen) | Cloud Carbon Footprint AWS coefficients | verified, point estimate |
| PUE | 1.14 | AWS disclosure, CY2025 | verified |
| grid intensity | 0.271 kg CO2e/kWh | EPA eGRID2023, SRVC, location-based average | verified, three years stale |
| embodied carbon, broker, storage, egress | **not modelled** | outside declared boundary | **omitted, flagged** |

## What you can take from this, and what you cannot

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

