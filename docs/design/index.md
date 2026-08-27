# Design decisions

The choices that shape everything else in the engine, recorded in a compact
decision-record format: the context at the time, the decision, and the
consequences that followed - including the trade-offs accepted, which are as
much a part of the record as the benefits.

These records are point-in-time. Each states what was decided and why; the
[internals references](../internals/README.md) describe how the resulting
subsystems work today, and take precedence wherever detail has evolved.

| # | Decision |
| --- | --- |
| [001](001-arrow-native-data-plane.md) | Arrow is the data plane, not an integration |
| [002](002-embedded-first.md) | Embedded-first: the engine is a library before it is a cluster |
| [003](003-state-as-open-data.md) | State is an open dataset, not an internal format |
| [004](004-jobs-as-compiled-plugins.md) | Jobs deploy as compiled plugins |
| [005](005-hand-rolled-sql-optimizer.md) | The SQL optimizer is hand-rolled, not adopted |
| [006](006-deterministic-replay.md) | Incidents replay deterministically from captured input |
| [007](007-state-generations.md) | State directories are namespaced by topology generation |
| [008](008-hot-rescale.md) | Rescale one operator at a barrier, without stopping the job |
| [009](009-one-declaration-per-type.md) | One declaration describes a type everywhere (proposed, v0.9) |
