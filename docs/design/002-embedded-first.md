# 002: Embedded-first - the engine is a library before it is a cluster

**Status:** adopted; the embedded path is a gated, benchmarked product
surface, not a test harness.

## Context

Stateful stream processing has historically forced a choice. Lightweight
libraries start instantly and embed anywhere, but skip the hard correctness
work: no event time, no checkpoints, no keyed state that survives a restart.
Cluster platforms do that work, but demand daemons, orchestration, and
operational ownership before the first result appears. The gap between "I
have a stream and a laptop" and "I have a correct pipeline" is where most
streaming projects stall.

The two halves are not actually in tension: watermarks, barriers, and keyed
state are properties of the dataflow graph, not of a deployment topology.

## Decision

Build one engine that is a library first:

- The whole engine lives in `libclink`. `clink run pipeline.sql` executes a
  pipeline in a single process with no daemons; the same binary embeds behind
  a pure-C ABI, and `pyclink` exposes it to Python with Arrow-native results.
- The distributed runtime is a consumer of the same engine, not its home: a
  Coordinator/Worker control plane deploys the same operator DAGs, and the
  same SQL file submits unchanged with parallelism, failover, and rescale.
- The embedded path carries full semantics - event time, keyed state,
  checkpoints, the same state backends - so graduating to a cluster changes
  where a pipeline runs, never what it computes.
- Cold start is treated as a feature with a regression gate: a Release-only
  test budgets time-to-first-row for the embedded path (~155 ms measured),
  so the library promise cannot silently rot.

## Consequences

- The adoption path has no cliff: prototype in-process, embed in a service,
  or submit to a cluster, all with one artefact and one semantics.
- Engine internals must never assume a resident cluster: anything the
  distributed runtime needs (config, registries, state directories) arrives
  through explicit context rather than ambient infrastructure. This
  discipline is what keeps [jobs deployable as plain compiled
  plugins](004-jobs-as-compiled-plugins.md).
- The trade-off accepted: two first-class execution paths must both stay
  correct and tested. The test suite runs the same operator and state
  contracts through the local executor and the cluster runtime, which is a
  permanent, deliberate cost.

See [Embedded execution](../internals/embedded.md) for how the path works
today.
