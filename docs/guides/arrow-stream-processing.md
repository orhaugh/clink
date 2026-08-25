---
title: Apache Arrow Stream Processing in C++ - Clink
description: Clink is an Arrow-native C++23 stream processing engine using Arrow for columnar execution, open state, interoperability, and native analytics workflows.
---

# Apache Arrow Stream Processing in C++

Clink is designed around Apache Arrow rather than treating Arrow only as a file or network interchange format. Arrow is part of the execution and state model of the engine.

## Columnar execution

Operators can process Arrow-backed batches directly when they support a columnar path. This lets native streaming pipelines avoid repeatedly converting data into engine-specific row objects for workloads that benefit from columnar execution.

See [columnar execution internals](../internals/columnar-execution.md).

## State as open data

Clink's state model deliberately makes persisted state inspectable outside the engine. Snapshot and state-serving paths use documented Arrow-compatible representations so state can be consumed by tools such as PyArrow, DuckDB, Polars, and other Arrow-aware software.

See [state as open data](../design/003-state-as-open-data.md), [state backends](../internals/state-and-backends.md), and [snapshot format](../internals/state-snapshot-format.md).

## Embedded native processing

Because the engine itself is C++23, Arrow buffers can move naturally between Clink and other native components. `libclink` also exposes a C ABI and Arrow C streams for embedding from other languages and runtimes.

This is useful for native analytics services, edge applications, low-footprint stream processors, and systems that already use Arrow as a common in-memory data layer.

## Distributed execution

The Arrow-oriented model is not limited to embedded execution. The same Clink operator model can run on a distributed Coordinator/Worker cluster with checkpoints, state recovery, rescaling and connector-backed I/O.

See [C++ stream processing engine](cpp-stream-processing.md) and [distributed runtime](../internals/distributed-runtime.md).
