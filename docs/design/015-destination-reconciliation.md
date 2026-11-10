# 015: Destination reconciliation

**Status:** accepted, 2026-11-10.

## The problem

Record 014 made a job's checkpoints, captures and traces portable and checkable offline, and said plainly what that leaves out: whether any external system holds what the job wrote. Offline verification reads no destination, so a bundle that passes every check says nothing about the table the job wrote to.

An operator who suspects a destination needs a different question answered, for a bounded scope: does this table hold exactly the effects this completed checkpoint owed it? Answering it takes evidence from inside the job, at the checkpoint's own boundaries, and a separate, explicit read of the destination.

This record adds that for one configuration, Kafka to ClickHouse, and extends 014's evidence rather than building an audit trail beside it. It adds no commit protocol and changes no delivery guarantee.

## The contract

This release reconciles one configuration:

| Part | Supported |
| --- | --- |
| Source | A Kafka SQL table. Its checkpoint position per partition is the next offset to read, so an interval consumed `[from, to)`. Its isolation level is the job's `isolation.level`, librdkafka's `read_committed` when unset. |
| Transformation | Record-wise: filters, projections and per-record expressions. Then the rows a checkpoint's intervals hand the sink are exactly those derived from the records between the bounding checkpoints' positions. Windows, joins and aggregates have well-defined interval effects too, but none of that holds for them, and a key such a job writes can repeat. |
| Sink | The ClickHouse native sink (`insert_format='native'`), at least once. |
| Table | A plain `MergeTree`: not `Replicated*`, `Shared*`, `Distributed`, nor an engine whose merges rewrite rows (`Replacing`, `Summing`, `Aggregating`, `Collapsing`); no `TTL`. `async_insert = 0`, which the sink enforces. A deduplication window (`non_replicated_deduplication_window > 0`) is recommended. |
| Key | One or more integer or `String` columns, not `Nullable`, that identify one row the job writes. A correct run writes each key in exactly one checkpoint interval. |
| Evidence | Workers run with the flight recorder (`--capture-dir`, bounded by `--capture-records`). The bundle is collected with `--include-captures`, and with `--include-state` for the source positions. |

Anything else is refused, or reported unsupported, by name.

### What a write is

- **Acknowledged.** The server answered a synchronous INSERT (`async_insert=0, wait_for_async_insert=1`, sent with every statement). The sink counts a row only then. A barrier returns only after every INSERT of its interval was acknowledged, and the checkpoint is acknowledged after the barrier. A completed checkpoint therefore implies every row of its intervals was acknowledged.
- **Query-visible.** On one `MergeTree` server, rows a synchronous INSERT wrote are in the table's committed parts when it is acknowledged, and a later `SELECT` on that server reads them. This is why replicas are out of scope: a replica can lag the one that took the INSERT.
- **Durable.** ClickHouse writes an INSERT's part before acknowledging it, but by default does not fsync it: `fsync_after_insert` and `fsync_part_directory` are 0 on 26.8. An acknowledged write survives the server process but not necessarily the host losing power. A reconciliation observes the table at one time and establishes nothing about durability beyond that observation.

### Retries and deduplication

An INSERT that fails after its blocks were sent is in doubt, because the server may have written it. The sink resends it. A resend under the INSERT's own token is dropped by the server when the first attempt landed, but only when all of these hold:

- the server is on a tested line (26.3, 26.8) and is the same server (endpoint, name and version) that took the first attempt;
- the table keeps a deduplication log: a `MergeTree` table deduplicates nothing until `non_replicated_deduplication_window` is above 0;
- fewer than the window's worth of other blocks were inserted between the attempts.

The pins P2, P4, P5 and P17 hold the first two on both lines (docs/connectors/clickhouse.md). Any other resend may land twice.

A restore replays what the restored checkpoint did not cover. Rows the discarded run wrote after the restore point are written again, as new INSERTs under fresh tokens, which no log drops. Both are within the at-least-once guarantee, and neither is hidden: the comparison counts every physical row.

## The evidence

One switch arms it, the flight recorder. With `--capture-dir` set:

- **The sink's input.** The sink runner records the sink's input per checkpoint interval as an operator runner records an operator's (`op-<sink>/subtask-<s>/epoch-<N>.cap`, bounded by `--capture-records`, truncation flagged). The engine's own network sinks are not recorded. A sink's `op.json` carries its parameters with every credential-like value redacted.
- **The interval evidence.** After the sink's `on_barrier` returns for checkpoint N, and before the barrier is acknowledged, the runner writes `epoch-<N>.sink.json` atomically (written beside its name, then renamed). It holds:
  - what the interval's capture came to: written, empty or disarmed, records seen and stored, truncated;
  - the barrier before it in this run, where 0 means the run's start;
  - the runner's attempt id, and the last watermark the sink had;
  - the sink's own account, through the Internal `IntervalEvidenceSink` interface.

  The ClickHouse native sink's account has two parts:
  - the destination as its open found it: server UUID, host, version and endpoint; database, table, table UUID, engine, `engine_full`, `create_table_query`; the deduplication and async-insert settings; and the column plan, each declared type with its target type;
  - the ledger of the interval's acknowledged INSERTs: token, rows, attempts, whether one was in doubt, and what its resend could do (`deduplicated`, or `may_duplicate`).
- **The bounding checkpoints' state.** With `--include-state`, the collector adds each participant's snapshot of the completed checkpoints that bound every collected interval, and of the run's restore point. The Kafka positions at both ends of a scope come from those snapshots.

A checkpoint that completed was acknowledged after its evidence was written. A failed write leaves the evidence absent, and absent, truncated or disarmed evidence makes a reconciliation unavailable; it never passes one. Checkpoint ids are never reused across a restore, so one interval's file never mixes runs. A sink's captures are left out of the replay checks, which see a bundle as they did before.

## The scope

The scope of completed checkpoint N is every interval the checkpoint completed. The reconciliation walks back from N through each interval's previous barrier until it reaches a completed checkpoint, or the start of the run. A checkpoint that was triggered and never completed leaves its interval to the next one that did.

Every sink subtask's evidence for every interval must be present, from one runner attempt, and agree on the destination. Each interval's ledger must acknowledge exactly the rows its recorded input holds, since rows the sink wrote beyond its recorded input would sit under keys the comparison never reads. A scope with no rows is unavailable: it establishes nothing. The expected rows are the union of the sink's recorded input over the scope, each reduced to canonical cells. They come from inside the job, before any write, and nothing read from the destination shapes them. They are not independent of clink's transformation: the replay checks and an independent reference, as in the example, cover that.

## Reading the destination

`clink incident-reconcile <bundle> --host --key ...` is the workflow's one command that opens a connection, and only when asked. Offline verification stays as 014 left it.

- **The connection is the operator's.** Nothing it connects with or runs comes from the bundle. The password comes from an environment variable; the bundle supplies only names, quoted as identifiers, and key values, rendered as typed literals.
- **Read only, twice over.** Every statement is a `SELECT` carrying `readonly = 2`, under which the server refuses any write in it (verified on 26.8: code 164). Run it as a user with `SELECT` alone.
- **Bounded.** `max_execution_time` and `max_result_rows`, both set to throw, and the client's own receive timeout. A read past a bound fails whole, so the comparison is unavailable, never decided on part of an answer.
- **Identity first.** The server's UUID and the table's UUID must equal the evidence's, or the run is refused (exit 2). So is a `--job`, `--checkpoint`, `--database` or `--table` that does not fit the bundle.
- **Form.** The table must be a plain `MergeTree` with no TTL, and every column the sink wrote must still have the type it had. Otherwise the result is unsupported or unavailable.
- **Stability.** Mutations not yet done are read before and after, and the scope is read twice with a pause between. A mutation in progress or begun during the read, a changed table definition, or a key read differently twice makes the result unavailable. Done mutations are the table's state: they are listed, and the rows decide.

## Comparing

Rows are compared whole, as multisets, key by key. Counts, totals and digests decide nothing: equal counts and totals over different rows fail. The cells:

| Type | Compared as |
| --- | --- |
| integers | exact |
| `Float32`, `Float64` | IEEE-754 bits: every NaN equal, -0.0 and +0.0 different. The sink converts without rounding, so any difference is a change. |
| `Decimal(P, S)` | the unscaled integer at the column's scale; the server's `12.3` and the row's `12.30` are one value |
| `DateTime64(p)`, `DateTime` | ticks at the column's precision, from the row's epoch milliseconds |
| `Date`, `Date32` | days |
| `String`, `LowCardinality(String)` | bytes |
| `Bool` | exact |
| NULL | equal to NULL and to nothing else |

Other column types (`FixedString`, arrays, maps, tuples and the rest) make the result unsupported. These rules are the comparison's own, not the sink's converter, so a converter defect shows as a difference rather than cancelling out.

For each key there are four kinds of difference:

- **Missing**: no row for the key.
- **Changed**: no exact copy of the expected row, but another row with the key.
- **Duplicated**: more than one exact copy.
- **Unexpected**: any further row with the key.

Any of them fails the result. The report separates them:

- Missing, changed and unexpected rows are outside the at-least-once guarantee.
- Duplicates are within it, but the destination then does not hold the scope's effects exactly once. When the evidence explains them, by an in-doubt resend that may land twice or a restore at the scope's start, the report says so.

Rows of keys outside the scope are not attributable to it, and are not read.

## Verdicts

- **Exit 0**: every check passed.
- **Exit 1**: a check failed. That is a destination that does not hold the scope exactly, or a bundle whose integrity failed.
- **Exit 3**: none failed and one is unavailable or unsupported. The result is partial.
- **Exit 2**: bad usage or a refused request.

The checks are the bundle's integrity (`bundle.*`), `reconcile.scope`, `reconcile.destination` and `reconcile.rows`, in a versioned JSON report (`clink-reconcile-report`, version 1). The report records the observation's times on both clocks, the scope with its source ranges and the log they belong to (the topic, the cluster's id and the topic's id, which the Kafka source records beside its positions), the method, the assumptions, any mutations, and the first discrepancies.

## Three kinds of assurance

| Assurance | Says | From |
| --- | --- | --- |
| The configuration supports a guarantee | what the job's connectors and settings promise | the delivery report, at submission and in the bundle |
| The recorded execution passes the protocol and replay checks | that the evidence is intact, the restore rule and participants hold, the protocol events conform, and epochs replay | `incident-verify`, offline |
| The observed destination matches the bounded expectation | that at one observation time the table held exactly what one checkpoint's intervals owed it | `incident-reconcile`, online and explicit |

None implies the next. A job configured at least once can have a destination that lost rows, and a destination that matches one checkpoint can still be wrong for another.

## The independent reference audit

Reconciliation's expectation is the sink's own record of its input, so it establishes the delivery and nothing about the transformation: a job that computes the wrong rows writes them faithfully, and its captured input and its destination agree. An expectation that can catch that must come from outside the job. The audit takes one from the operator and keeps three relationships apart:

- the reference against the captured input, which is the transformation;
- the captured input against the destination, which is the delivery;
- the reference against the destination.

**The reference is computed by the operator, not by clink.** clink exports the scope offline (`incident-scope`), and the operator runs their own implementation over it and hands back its output. The audit never runs, loads or connects to anything an artifact names, and the reference never uses clink's SQL evaluator or the sink's conversions. A reference computed by clink would share whatever is wrong with the job's evaluation, which is the circularity the audit exists to break. `python/clink-reference` does the parts every reference shares: reading the exact Kafka range, confirming its identity, and writing the artifact. The transformation stays the operator's.

**The artifact is bound to one scope.** Its header carries the scope document verbatim, with a fingerprint over the job, run attempts, job graph, bounding checkpoints, Kafka logs, ranges, isolation level and output columns. The audit compares it with the scope it computes from the bundle and refuses any difference, naming the first field. A reference for another checkpoint, another run, a recreated topic or another schema cannot be compared by mistake.

**Integrity is not provenance.** The end line's hashes over the header and the rows show the file is whole as its writer finished it, and a file without the end line is refused as unfinished. They do not show who wrote it or whether its implementation is independent of the job. The report keeps what the audit verified (the binding, the hashes, the cells' form) apart from what the artifact only records (its implementation, configuration, client and any offsets the operator supplied).

**An incomplete reference is never compared.** Reading a Kafka range completely under `read_committed` means passing its end, not counting to it: transaction markers and aborted records hold offsets that are never delivered, and a quiet poll proves nothing. Expired or compacted history, a recreated topic, another cluster, a log that ends early, an error, a deadline or a bound each make the reference say it is incomplete and why. The audit then holds it unavailable instead of comparing whatever remained.

**Coverage is stated, not implied.** A destination read selects keys. With a reference it reads the union of the reference's and the captured input's keys, so a row the job dropped is looked for and a row it introduced is read. A row of a key neither names is invisible to it, so a passing result says it covered the selected keys, and the report says the rest of the table is not established. Completeness of the whole table is claimed only under `--coverage=complete-table`, and only when:

- the operator declares the table dedicated to one bounded run;
- the run began with no state;
- the bundle shows the run's sink quiet for two barriers after the checkpoint;
- the table's parts are unchanged across a bounded full read.

Two equal reads do not establish a frozen table, and no checkpoint attribution is invented for rows of a live, shared table. An empty expected output passes only when the table is read whole and is empty.

## Unsupported in this release

The following are not supported:

- other destinations and sink modes: ClickHouse's text sink, upserts, materialised views;
- replicated, shared, distributed or row-rewriting tables, and tables with a TTL;
- jobs that are not record-wise, where they need source positions or a key that repeats;
- an embedded `clink run`, whose sinks the flight recorder does not record;
- reading `system.query_log` as acknowledgement evidence;
- continuous reconciliation, repair, signing.

## Implementation and tests

| What | Where |
| --- | --- |
| The interval evidence | `include/clink/runtime/sink_evidence.hpp`, `src/runtime/sink_evidence.cpp`, the sink runner in `include/clink/runtime/dag.hpp`, the worker's sink registration in `include/clink/plugin/plugin_impl.hpp` |
| The ClickHouse sink's account | `impls/clickhouse/src/native/` (`native_sink_core.cpp`, `writer.cpp`, `target_table.cpp`) |
| Collection | `src/incident/collect.cpp`, the bundle reader in `src/incident/bundle.cpp` |
| The reconciliation | `include/clink/incident/reconcile.hpp`, `src/incident/reconcile.cpp` |
| The ClickHouse reader | `impls/clickhouse/include/clink/clickhouse/reconcile_destination.hpp`, `impls/clickhouse/src/reconcile_destination.cpp` |
| The command | `tools/clink_incident.cpp`, `tools/clink_incident_reconcile.cpp`, or its stub without the connector |
| The audit: the scope document, the artifact's reader and binding, the multiset comparison | `include/clink/incident/reference.hpp`, `src/incident/reference.cpp`; the relationships and the whole-table check in `src/incident/reconcile.cpp` |
| The reference library | `python/clink-reference/` (`scope.py`, `canonical.py`, `kafka_range.py`, `artifact.py`) |

- **Unit tests**: `tests/test_incident_reconcile.cpp`. These cover the cells, the comparison, the sink runner's evidence, and whole reconciliations of collected bundles against a destination served from memory: every kind of difference, refusals, missing and truncated evidence, limits, instability, mutations, unsupported forms, and source positions after a restore. The incident gate runs them and requires every case.
- **Live tests**: `impls/clickhouse/tests/test_reconcile_destination_live.cpp`, on each line in `scripts/clickhouse-live.sh`. These cover what the reader reads against the expectation's cells, identity, mutations, bounds, and the server's refusal of a write.
- **The example**: `examples/destination-reconciliation/`, with Kafka and ClickHouse and every scenario checked against an independent reference. It includes jobs with a deliberate defect (a wrong projection, a filter that drops valid rows, one that lets through others, counts and totals equal over different rows), faults in the destination alone, an unrelated key, an empty expected output, references for another scope, unfinished or incomplete, and the cost of the audit.
- **The audit's tests**: `tests/test_incident_reconcile.cpp` (`ReconcileMultiset.*`, and the `ReconcileBundle` audit cases covering the artifact's binding and refusals, each relationship, failure precedence, coverage, and empty scopes). `python/clink-reference/tests/` covers the canonical cells and the artifact offline, and with `scripts/reference-live.sh` the range reader against Apache Kafka, with committed and aborted transactions.
