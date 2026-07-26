# clink

`clink` is an embedded-first, Arrow-native stream processing engine in modern
C++ (C++23): stateful stream processing with engine-grade semantics - SQL,
event time, keyed state, exactly-once checkpoints - that you run like a tool
rather than operate like a platform.

The whole engine lives in one library, and the same pipeline runs two ways.
In-process, `clink run pipeline.sql` executes it in a single process with no
daemons; `libclink` embeds the engine in any service behind a pure-C ABI, and
`pyclink` returns results as pyarrow tables. At scale, the same SQL file,
unchanged, submits to a distributed Coordinator/Worker cluster with
parallelism, failover, and rescale.

This site is the deep reference for the engine, organised in three layers:

## What it can do

The [capability catalogue](capabilities.md) is the complete, shipped feature
surface in one page: execution model, SQL, state, delivery guarantees,
operations, observability, and embedding APIs, each row linking to the page
that documents it in depth.

## What it costs to run

[Efficiency and environmental impact](efficiency.md) publishes the measured cost of
processing an event: **3.04x less CPU per event and 16.4x less memory** than a JVM
stream processor on the same hardware, producing byte-identical output on a
correctness-gated comparison. It also states plainly where clink currently LOSES (its per-group
windowed state measured about 3.9 KB for an int64 key and a count, heavier than the JVM
engine's) and what was not measured (wall power, so no kWh or CO2e figure is claimed). The raw
per-run output is published alongside it.

## How it works

The [internals references](internals/README.md) explain every subsystem the
way its code is actually structured: the operator model, task lifecycle,
network stack, state backends and snapshot format, checkpointing, the SQL
frontend, columnar execution, and more. Each page cites the sources it
describes.

## Why it is built this way

The [design decisions](design/index.md) record the reasoning behind the
choices that shape everything else - the Arrow-native data plane, the
embedded-first execution model, state as open data, deterministic replay -
including the trade-offs accepted and the consequences that followed.

## Elsewhere

- [Connector catalogue](connectors/README.md): every source and sink, with
  dependencies, factory names, options, and SQL usage.
- [Runnable examples](consumer-examples/README.md): buildable programs from
  hello-pipeline to the testing framework and state-as-data workflows.
- [Repository](https://github.com/orhaugh/clink) and
  [operations console](https://github.com/orhaugh/clink-fe).
