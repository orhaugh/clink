# 004: Jobs deploy as compiled plugins

**Status:** adopted for compiled jobs; SQL jobs compile to the same spec
without user code.

## Context

A distributed engine must move user pipeline logic onto worker processes. The
common approaches are shipping an interpreted artefact (which constrains the
user's language and the engine's performance), embedding a virtual machine
(which imports a managed runtime and its operational profile), or requiring
users to rebuild and redeploy the engine binary around their job (which
couples every job change to an engine rollout).

clink is a C++ library whose users write typed operators against
`clink::Pipeline`. The deployment unit should preserve that: native code,
full type fidelity, no managed runtime.

## Decision

A job is a shared object:

- `CLINK_REGISTER_JOB` in the user's translation unit exports the pipeline
  under a stable entry point, capturing the `JobGraphSpec` (the serialised
  operator DAG) alongside the code that implements it.
- `clink_submit_job` uploads the `.so`; every process that participates in
  the job - coordinator and each worker - `dlopen`s it and instantiates only
  its assigned subtasks.
- An ABI hash embedded at build time gates loading, so a plugin built against
  a different engine build is rejected at deploy rather than misbehaving at
  runtime.
- SQL takes a parallel path to the same destination: the frontend compiles
  statements to a `JobGraphSpec` of operators already registered in the
  engine, so SQL jobs deploy with no user binary at all.

## Consequences

- User operators run as native code with zero marshalling at the operator
  boundary, and one artefact carries both the topology and the
  implementation.
- The trade-offs accepted: plugins must be built against a matching engine
  build (the ABI gate makes this explicit and fail-fast), and `dlopen`
  isolation has sharp edges - a plugin's static singletons are private to
  its library, so anything shared (registries, metrics) must flow through
  the host-provided runtime context rather than globals. That rule is now a
  hard convention across the codebase.
- Because the spec travels with the binary, the coordinator can reason about
  topology (scheduling, rescale, lineage) without executing user code.

See [Distributed runtime](../internals/distributed-runtime.md) for the
control-plane protocol and plugin lifecycle.
