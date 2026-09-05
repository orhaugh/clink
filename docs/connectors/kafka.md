# Apache Kafka

> Source and sink for Apache Kafka topics over librdkafka. It reads and writes both text/JSON payloads (the `string` channel) and full broker records (the `KafkaMessage` channel).

## Overview

The Kafka connector consumes from and produces to Kafka topics using the librdkafka C/C++ client. It exposes two channel shapes: a `std::string` payload channel for plain text or JSON values, and a `KafkaMessage` channel that preserves the full broker record (payload, optional key, headers, offset, partition and timestamp). Records carry their bytes verbatim; the connector does no schema-aware encoding of its own, so the value bytes are whatever the producing or consuming pipeline puts on the channel (JSON when reached through the SQL frontend). The source binds its consumer position to clink checkpoints so it can replay from a recorded per-partition offset on recovery.

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| librdkafka (`rdkafka++`, `rdkafka`) | System package via apt (Debian) / brew (macOS) | Not pinned by clink |

The connector links librdkafka only; it does not use Arrow on its I/O path. CMake discovers the client in three tiers (CMake config package `RdKafka`, then pkg-config `rdkafka`/`rdkafka++`, then a manual header/library probe under the common Homebrew, `/usr/local` and `/usr` prefixes), so Homebrew, vcpkg, Confluent and source builds all resolve without setting `CMAKE_PREFIX_PATH`.

## Enabling it

Controlled by the `CLINK_WITH_KAFKA` CMake option, which defaults to `AUTO`:

- `AUTO`: build the connector if librdkafka is found, otherwise skip it.
- `ON`: require librdkafka; configuration fails if it is not found.
- `OFF`: do not define the target.

```bash
cmake -S . -B build -DCLINK_WITH_KAFKA=ON
cmake --build build -j
```

When librdkafka ships `librdkafka/rdkafka_mock.h` (version 1.3 and later), CMake also sets `CLINK_HAS_KAFKA_MOCK`, which enables the in-process mock-broker test suite. No Arrow build flag is required.

## Factories

| Factory name | Direction | Record type |
| --- | --- | --- |
| `kafka_message_source` | Source | `KafkaMessage` |
| `kafka_message_sink` | Sink | `KafkaMessage` |
| `kafka_text_source` | Source | `std::string` |
| `kafka_source_string` | Source | `std::string` |
| `kafka_text_sink` | Sink | `std::string` |
| `kafka_sink_string` | Sink | `std::string` |
| `kafka_2pc_sink_string` | Sink | `std::string` |
| `kafka_upsert_sink_string` | Sink | `std::string` |

`kafka_text_source` and `kafka_source_string` register the same builder, as do `kafka_text_sink` and `kafka_sink_string`. The `_string` aliases exist because the SQL planner emits those op-type names. These factories become resolvable once `clink::kafka::install(registry)` is called against the plugin registry.

## Configuration

Options are read from `BuildContext` parameters in `impls/kafka/src/register_factories.cpp` and map onto the `KafkaSource::Options` and `KafkaSink::Options` structs in `impls/kafka/include/clink/connectors/kafka_source.hpp` and `kafka_sink.hpp`.

### Source (`kafka_message_source`, `kafka_text_source`, `kafka_source_string`)

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `brokers` | Yes | (none) | Bootstrap broker list, for example `localhost:9092`. |
| `topic` | Yes | (none) | Topic to consume. |
| `group_id` | No | `clink` | Consumer group id. |
| `client_id` | No | `clink-source` | librdkafka client id. |
| `auto_offset_reset` | No | `earliest` | Where to start when no committed/restored offset exists: `earliest`, `latest` or `none`. |
| `batch_max_wait_ms` | No | `5` | Bounds TOTAL batch formation time in the source's poll loop. Waiting for the first record of a batch still blocks up to `poll_timeout` (idle stays cheap); once a batch has begun, the fill loop stops when this bound elapses and emits a partial batch instead of waiting to accumulate `max_batch_size` records. Keeps per-record latency on a paced or trickling input proportional to this bound rather than `max_batch_size / input-rate`; a saturated consumer queue fills `max_batch_size` well inside the bound, so throughput at the ceiling is unaffected. `0` disables the bound. |

The `KafkaSource::Options` struct also carries `poll_timeout` (100 ms), `max_batch_size` (2048), `commit_mode` (`Auto` on the struct; the registered factories force `Manual`), `enable_debug` (false), `metric_prefix` (`default`), `partition_discovery_interval` (30 s; 0 disables) and the deterministic-ownership pair `subtask_index`/`source_parallelism` (0/1, meaning a standalone source owns every partition). The ownership pair and the `Manual` commit mode are set by the registered factories from the build context: an engine-managed source must not write consumer-group offsets, because the engine's checkpoints are the only resume authority - librdkafka's auto-commit records consumed positions no checkpoint completed, and a restore for a partition without an offset row would resume from that group offset, past records whose effects died with the rewound attempt. For the same reason a fresh partition's start offset is resolved to a concrete number BEFORE `assign()` - the group's committed offset where one exists (a courtesy lookup with engine-owned, bounded retries), else the `auto_offset_reset` policy via the broker's watermarks, else the broker-resolved logical BEGINNING/END - and never librdkafka's `OFFSET_STORED`: leaving the resolution to the client makes the fetch position depend on the group coordinator answering, and on a fresh broker whose `__consumer_offsets` topic was still settling, one `NOT_COORDINATOR` reply left the assignment pending forever - no records, no error, every gauge zero and healthy (issue #8). The resolved number also seeds the partition's resume row, so even a job's first checkpoint carries one for every partition. The remaining options are not parsed from `BuildContext` parameters, so they take their struct defaults unless the typed class is constructed directly.

### How the source fetches

`produce()` fills its batch with `rd_kafka_consume_batch_queue()` on the queue
returned by `rd_kafka_queue_get_consumer()`, so a full batch is normally ONE fetch
call rather than one call per record. The C++ `consume()` wrapper it replaced took
the queue lock, dequeued a single op and heap-allocated a wrapper object for every
record; measured against a real broker that cost 586 ns of CPU per record where the
batched form costs 456 ns, a 22% reduction. Offset bookkeeping accumulates in a short
scratch vector merged into the offset map once per batch (it was a red-black tree
lookup per record for a value only the checkpoint reads), and the `consumed` /
`consume_errors` counters increment once per batch.

Polling that queue counts as a consumer poll, so `max.poll.interval.ms` keeps being
reset and group membership behaves exactly as before. Rebalance ops travel on the same
queue, so the seek-on-assignment callback that makes a clink checkpoint authoritative
over Kafka's committed offset still fires - covered by
`Kafka.SourceReplaysFromSnapshottedOffset`, which resumes at `payload-6` rather than
`payload-0` against the mock cluster and could not do so otherwise.

Roughly four fifths of the source's remaining per-record cost is librdkafka's own:
measured with `CLINK_KAFKA_BENCH_RAW=1` (`benchmarks/clink_kafka_source_bench.cpp`),
which runs the same batched fetch and destroys each message without building a
`KafkaMessage`, the client library floor is 360 ns per record against clink's 456 ns.
Wall-clock cannot see any of this: one reader against a containerised broker saturates
the network pipe (~136 MB/s here) long before it saturates a core, so both fetch paths
measure ~1.12M rec/s and the change reads as a 5% *regression* if judged on throughput.
CPU per record is the metric to A/B against.

### Sink (`kafka_message_sink`, `kafka_text_sink`, `kafka_sink_string`)

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `brokers` | Yes | (none) | Bootstrap broker list. |
| `topic` | Yes | (none) | Destination topic. |
| `client_id` | No | `clink-sink` | librdkafka client id. |
| `acks` | No | `all` | Producer acknowledgement mode: `all`, `1` or `0`. |
| `compression` | No | `none` | Compression codec: `none`, `gzip`, `snappy`, `lz4` or `zstd`. |
| `linger_ms` | No | `5` | Producer batching delay (librdkafka `linger.ms`), a non-negative integer of milliseconds. `0` sends as soon as the producer loop runs, trading batching efficiency for per-record latency. An invalid value fails the deploy with a clear error. |

The `KafkaSink::Options` struct additionally carries `produce_timeout` (30000 ms), `flush_timeout` (30000 ms), `fixed_partition` (unset) and `metric_prefix` (`default`), which are not parsed from `BuildContext` parameters and take their struct defaults.

### Transactional sink (`kafka_2pc_sink_string`)

Accepts `brokers`, `topic`, `client_id` (default `clink-sink-2pc`), `compression` and `linger_ms`, plus:

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `transactional_id` | Yes | (none) | librdkafka `transactional.id`. Must be unique per producer instance. When `parallelism > 1` the factory appends the subtask index. |
| `transaction_timeout_ms` | No | librdkafka default (60000) | librdkafka `transaction.timeout.ms`: how long the broker keeps an abandoned prepared transaction before expiring it. `message.timeout.ms` is set to the same value alongside (librdkafka requires it not exceed the transaction timeout, and a message cannot meaningfully outlive its transaction). Lower it only with a reason - an expired transaction is a final not-committed verdict for in-doubt resolution. |
| `commit_group` | No | (none) | Declares commit-group membership. Does NOT add cross-sink atomicity: a job's transactional sinks already commit on one per-checkpoint broadcast and abort together on any failed ack. Membership only advances when an abort is issued. See [../internals/checkpointing.md](../internals/checkpointing.md). |
| `replay_suppression` | No | `true` | On a restore that could not advance past this subtask's receipted commits, swallow the replay's re-emissions at or below the receipted watermark horizon (see the delivery-semantics section). Exact for watermark-monotone feeds - windowed or aggregated emissions, the shape this sink normally terminates. Set `'false'` for a feed that delivers records at or below the current watermark (late or out-of-order data reaching the sink directly), where the horizon cannot classify a record; such feeds keep the bounded-replay contract, and an armed sink meeting a record without an event time passes it through with a loud log rather than guessing. |

This factory does not parse `acks`; the transactional producer is configured with `enable.idempotence=true` by the underlying sink.

### Upsert sink (`kafka_upsert_sink_string`)

Accepts `brokers`, `topic`, `client_id` (default `clink-sink`), `acks`, `compression` and `linger_ms`, plus:

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `primary_key` | Yes | (none) | Comma-separated list of JSON object fields to extract as the Kafka message key. |

Each incoming row must be a JSON object. A field named `__row_kind` is interpreted as the change kind: `delete` emits a tombstone (empty payload keyed by the primary key), `update_before` is dropped, and `insert`/`update_after` emit the row JSON minus `__row_kind` as the payload.

### Authentication and TLS

All the factories above (sources, sinks, the transactional and upsert sinks) accept SASL and TLS options, mapped onto librdkafka config properties and applied verbatim when the client opens. SASL_PLAINTEXT / SASL_SSL with PLAIN or SCRAM, and SSL / mTLS, are all reachable:

| Option | librdkafka property | Description |
| --- | --- | --- |
| `security_protocol` | `security.protocol` | `plaintext`, `ssl`, `sasl_plaintext` or `sasl_ssl`. |
| `sasl_mechanism` | `sasl.mechanism` | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `GSSAPI`, `OAUTHBEARER`. |
| `sasl_username` / `sasl_password` | `sasl.username` / `sasl.password` | SASL credentials. |
| `ssl_ca_location` | `ssl.ca.location` | CA certificate (path) for verifying the broker. |
| `ssl_certificate_location` / `ssl_key_location` / `ssl_key_password` | `ssl.certificate.location` / `ssl.key.location` / `ssl.key.password` | Client certificate, key and key password for mTLS. |
| `ssl_endpoint_identification_algorithm` | `ssl.endpoint.identification.algorithm` | `https` to verify the broker hostname, `none` to disable. |
| `enable_ssl_certificate_verification` | `enable.ssl.certificate.verification` | `false` to skip broker-cert verification (testing only). |

A security configuration that would be silently weaker than it looks is
refused at build time, before a byte reaches a broker. librdkafka accepts
`sasl_username` / `sasl_password` with no `security_protocol` without
comment - the default is plaintext, so the credentials are configured,
never presented, and the connection is unauthenticated and unencrypted -
and it likewise accepts a named CA or client certificate that a plaintext
transport will never use. Both now throw, naming the option that fixes
them. Deliberate plaintext stays available; it just has to be named
(`security_protocol='plaintext'`). The check runs on the final property
map, so the `kafka.<property>` passthrough below cannot weaken a
transport under configured credentials either.

For any librdkafka property the aliases do not cover, an escape hatch passes it through verbatim: a WITH-option (or programmatic `Options.conf` entry) keyed `kafka.<property>` sets librdkafka `<property>` directly, e.g. `kafka.client.rack`. In SQL the dotted key is an identifier, so it takes double quotes - `"kafka.client.rack" = 'eu-central-1a'` - where a single-quoted key would be a string literal the parser rejects. librdkafka validates each key/value when the client opens and throws on an unknown or unsupported one (for example an SSL setting on a build without SSL support). Credentials are passed through but never logged by the connector.

## SQL usage

Mapped in `src/sql/physical_plan.cpp` under `connector='kafka'`. The planner selects the underlying factory from the table's delivery mode: plain tables use `kafka_source_string` / `kafka_sink_string`, `mode='upsert'` sinks use `kafka_upsert_sink_string`, and exactly-once sinks use `kafka_2pc_sink_string`. Row values are bridged to and from JSON strings (`row_to_json_string` on the sink side, `json_string_to_row` on the source side).

```sql
CREATE TABLE clicks (
  user_id BIGINT,
  url     STRING
) WITH (
  connector = 'kafka',
  format    = 'json',
  brokers   = 'localhost:9092',
  topic     = 'clicks',
  group_id  = 'analytics',
  auto_offset_reset = 'earliest'
);

CREATE TABLE click_counts (
  user_id BIGINT,
  n       BIGINT
) WITH (
  connector   = 'kafka',
  format      = 'json',
  brokers     = 'localhost:9092',
  topic       = 'click-counts',
  mode        = 'upsert',
  primary_key = 'user_id'
);
```

The columnar JSON bridge (`json_string_to_row_columnar`) is the default for a `format='json'` source table: it decodes straight into typed Arrow columns and attaches the sidecar, so downstream columnar operator fast paths fire on the Kafka path. It is exactly byte-equivalent to the row decode - an incapable schema (FLOAT/DECIMAL columns, reserved `__` names, duplicates) or any batch whose records do not round-trip faithfully falls back to the plain row decode, with an adaptive damper so a systematically unfaithful stream does not pay a double parse. Set `columnar_decode='false'` to opt a table out entirely (the planner emits the row-form `json_string_to_row` bridge).

## Example

Programmatic use of the typed connector classes, based on `impls/kafka/tests/test_kafka.cpp`:

```cpp
#include "clink/connectors/kafka_sink.hpp"
#include "clink/connectors/kafka_source.hpp"

using namespace clink;

// Sink: produce ten records to a topic.
KafkaSink::Options sink_opts;
sink_opts.brokers = "localhost:9092";
sink_opts.topic   = "round-trip";
KafkaSink sink(std::move(sink_opts));
sink.open();

Batch<KafkaMessage> batch;
for (int i = 0; i < 10; ++i) {
    batch.emplace(KafkaMessage{"payload-" + std::to_string(i)});
}
sink.on_data(batch);
sink.flush();
// sink.delivered_count() == 10, sink.delivery_error_count() == 0

// Source: consume them back.
KafkaSource::Options src_opts;
src_opts.brokers           = "localhost:9092";
src_opts.topic             = "round-trip";
src_opts.group_id          = "rt-group";
src_opts.auto_offset_reset = "earliest";
KafkaSource source(std::move(src_opts));
source.open();
// source.produce(emitter) emits Batch<KafkaMessage> until cancelled.
```

When going through the plugin registry, call `clink::kafka::install(registry)` once after the built-ins are registered, then look up a factory by one of its registered names.

## Delivery semantics

- Deterministic partition ownership: a source subtask consumes exactly the partitions `p` of its topic with `p % source_parallelism == subtask_index`, assigned manually via `assign()`. There is no consumer-group subscription and no rebalance protocol in the correctness path: group-decided ownership is external state that cannot be held to one checkpoint cut, and both of its failure shapes were measured on the QUAL-01 rig - a partition handed to a subtask without a restored offset silently resumed from the broker's committed group offset, and a partition handed to a subtask holding a *stale* restored offset (a restore hands every subtask the union of all offset rows, which is what a rescale needs) was seeked backwards and its replayed span absorbed twice into open window state. Static `group.instance.id` membership narrowed the second shape but could not close it; deciding ownership inside the engine from the same subtask index the offset rows are scoped to closes both. The registered factories set `subtask_index`/`source_parallelism` from the build context; a standalone (non-cluster) source defaults to owning every partition. Topic metadata is re-polled on `partition_discovery_interval` (default 30s, 0 disables) so new partitions of a repartitioned topic join their owner at the `auto_offset_reset` policy. Each (re)assignment logs one line naming the subtask, its owned partitions and each partition's start-offset origin (`held`, `tracked`, `restored`, or `stored/<reset>`).
- Source replay (exactly-once on the read side): the source records one operator-state row per owned partition into the clink checkpoint via `snapshot_offset`; `restore_offset` narrows the restored union to the partitions the ownership rule gives this subtask, and `open()` assigns each owned partition at its restored offset. The clink checkpoint is the source of truth for the consumer position on recovery; a fresh partition with no clink-side position resolves the group's committed offset first and falls back to `auto_offset_reset`, matching the semantics subscription-based consumption had - resolved by the engine to a concrete offset before assignment, for the issue-#8 reason above. The `StringKafkaSource` adapter delegates the checkpoint hooks to the inner `KafkaSource` so the string and SQL paths retain replay.
- Plain sink (`kafka_text_sink` / `kafka_sink_string`): at-least-once. Records are produced and flushed; on every successful broker ack a `delivered` counter increments, and delivery failures after retry increment `delivery_errors`. With `acks=all` writes are durable, but there is no transactional rollback, so a failure after a partial flush can leave records on the topic.
- Transactional sink (`kafka_2pc_sink_string`): two-phase commit. Records are produced inside an open librdkafka transaction; a checkpoint barrier flushes and records the pending checkpoint id; the broker-side `commitTransaction` happens only when the coordinator signals the checkpoint globally durable (`on_commit`). `on_abort` aborts the prepared transaction. `close` aborts only the OPEN TAIL transaction (records after the last barrier, which the restore replays); a barrier-sealed PREPARED transaction is deliberately left pending, because its checkpoint may have completed with the commit broadcast racing the teardown - aborting it then discards records the `COMPLETED-N` marker obliges recovery to publish, while the slices other subtasks already committed replay as duplicates (measured on the QUAL-01 rig as 13,519 identical-value duplicates in one window). A preserved prepared transaction is finalised by in-doubt resolution at the held restart, or expires at the broker's `transaction.timeout.ms` if its checkpoint genuinely never completed - a bounded `read_committed` latency cost on unclean teardown, never a correctness cost. The coordinator matches this from its side: a checkpoint completing while the job is draining for a restart keeps its durable marker but is not broadcast for commit, so a half-torn-down job can never end up with a completed checkpoint whose external commits are partial; the restart's held resolution finalises it as one decision. The open transaction always carries exactly one checkpoint interval's records: records arriving while a commit is outstanding buffer app-side, and each further barrier seals them into a queued segment produced into its own transaction when the commits cascade down to it. Consumers must read with `isolation.level=read_committed` for the guarantee to hold. Because a broker transaction cannot be resumed by a new producer (fencing or `transaction.timeout.ms` expiry aborts it; librdkafka has no transaction resume), this sink declares `commit_recoverable = false` in its capability record, which puts jobs containing it on the commit-confirmed restore protocol: the worker confirms each commit's execution back to the coordinator, the coordinator writes a `CONFIRMED-N` marker beside `COMPLETED-N` once every such sink subtask has confirmed, and restores select the newest *confirmed* checkpoint rather than the newest completed one. A worker that dies before its broker commit executes therefore costs nothing - the restore replays the interval into a fresh transaction. The window on the other side - a worker that dies after `commit_transaction` returned but before its confirmation was sent - is closed by the commit receipt the sink writes the moment the broker acknowledges (next paragraph): recovery reads the receipt and confirms the checkpoint without asking the broker, so the interval is not replayed. The floor contract under process loss - never missing records, never foreign records, duplicates bounded by at most one contiguous checkpoint interval per process loss - remains what a consumer can rely on with receipts and replay suppression disabled, on feeds outside the suppression premise, or across the microsecond residual documented below; with both defaults in force, a windowed-aggregation pipeline into this sink is zero-duplicates under every fault the qualification campaign injects. Recoverable-commit sinks (Postgres `PREPARE TRANSACTION`, staged-file renames) re-execute their commit at restore instead and never gate on confirmation; see the sink-committer-framework internals page.

**Prepared-transaction resume (in-doubt resolution).** The sink additionally stages a resume handle at every barrier - the `transactional.id` plus the producer identity (`producer_id`/`producer_epoch`) captured from librdkafka's statistics callback, the one place the client exposes it - into operator state, so the handle lives inside the checkpoint. `open()` waits (bounded, ~3s) for the first statistics tick so the identity exists before the first barrier can stage a handle; a handle staged without one refuses resolution loudly rather than guessing. On HA recovery the coordinator's in-doubt resolution reads the handles of each completed-but-unconfirmed checkpoint and commits the orphaned transaction over the wire (`FindCoordinator` + `EndTxn` with the dead producer's identity, sent *before* any successor's `init_transactions` would fence it - `clink/kafka/txn_resume.hpp`); on full success it writes `CONFIRMED-N` and the restore point advances past the interval instead of replaying it, closing both halves of the window on that path. Only the broker's explicit acknowledgement counts as committed; a fenced epoch, timed-out transaction, unsupported broker version, refused authentication, or never-captured identity all fall back to the bounded contract above. An UNREACHABLE broker is not a verdict: resolution executes commits handle by handle (EndTxn is the resolution - there is no read-only probe), so a transport failure part-way through a checkpoint is retried in place with bounded backoff rather than treated as "not committed" - a fallback taken after some handles committed would restore below intervals the walk just published and replay them as duplicates, exactly the interleaving broker chaos overlapping a recovery produces. The resume speaks SASL/PLAIN and SASL/SCRAM-SHA-256 when the resolving process's environment supplies `CLINK_KAFKA_RESUME_SASL_MECHANISM` (`PLAIN` or `SCRAM-SHA-256`) plus `CLINK_KAFKA_RESUME_SASL_USERNAME` / `CLINK_KAFKA_RESUME_SASL_PASSWORD` - credentials never ride the staged handle, because durable checkpoint state must not carry secrets. The SCRAM client (`clink/kafka/scram.hpp`, OpenSSL primitives, built with `clink::tls`) is pinned against RFC 7677's published test vector and verifies the SERVER: a signature mismatch, a nonce that does not extend the client's, or an iteration count below the RFC floor is refused - a broker that cannot prove knowledge of the credentials is never sent EndTxn. Proven live full-circle: a genuine orphan produced with librdkafka's own SCRAM is resumed by this client against a real SASL-required broker and lands committed exactly. Any other mechanism value is refused locally before a byte is sent (no silent downgrade to unauthenticated), and PLAIN over a plaintext connection sends the password in the clear, so pair it with the TLS dialer: setting `CLINK_KAFKA_RESUME_TLS_CA` (plus optional `CLINK_KAFKA_RESUME_TLS_CERT`/`_KEY` for mTLS) makes the resolver dial every resume connection over TLS, verifying the broker against that CA - the sasl_ssl shape. TLS requested in a build without `clink::tls`, or with an unusable CA, is refused loudly; the resolver never downgrades to plaintext, pinned by a test that leaves a plaintext broker standing ready to catch exactly that. The SASL dialogue is pinned hermetically (golden frames, a scripted broker) and live against a real SASL-required Redpanda: the pinned broker speaks SCRAM only - its config validation rejects PLAIN outright - so the live arms prove both the refusal shapes (a PLAIN handshake refused naming SCRAM; unauthenticated connections dropped and surfaced as the transport fallback) and, with SCRAM now spoken, the full authenticated happy path. In-incarnation restarts get the same treatment: when the drain completes with a completed-but-unconfirmed gap on a tracked job, the redeploy is HELD while the resolver answers off-thread - nothing deploys in that window, so nothing can fence the orphan, keeping resolution and restore-point selection one decision. A refused or failed resolution releases the restart on the bounded contract unchanged. Neither librdkafka nor the Java client offers a supported resume API - KIP-939 is the sanctioned protocol path this module anticipates. The whole chain is proven end to end on real processes and a real broker by `KafkaExactlyOnceTest.AnOrphanedBrokerTransactionIsCommittedAcrossACoordinatorFailover` (integration label): a loaded transaction orphaned pre-commit, the worker and the leader both killed, the standby's recovery committing the orphan and finishing the job with every record exactly once.
**Commit receipts + replay suppression.** The wire-level resume above finalises transactions that are still finalisable; two shapes are not. A transaction whose commit ALREADY executed can be answered "not committed" by the broker - a successor's `init_transactions` fences the old epoch, `transaction.timeout.ms` erases the record of it - and a mixed outcome (one subtask's commit executed, a sibling's transaction lost to fencing) is unrepairable at checkpoint granularity: restoring below the checkpoint replays the committed slice as duplicates, restoring at it loses the fenced slice. Both are closed with a per-subtask commit receipt: the instant `commit_transaction` returns, the sink durably writes `sub<K>-<N>` (fsync + rename) under `<checkpoint_dir>/_jobs/<job_id>/receipts/`, carrying the watermark horizon the sealing barrier saw. Recovery's resolution walk takes a receipted handle as COMMITTED with **no wire call** - the receipt is the process's own record of the broker's acknowledgement, which no later fencing, timeout, or broker restart can retract - so verdict ambiguity for executed commits is gone. When the walk still cannot advance (a sibling's transaction got a genuine "not committed" verdict), the restore falls back below the checkpoint and the replay re-produces intervals the receipted subtasks already published; each such sink arms **replay suppression** at `open()` from its receipts newer than the restore point and swallows exactly the re-emissions whose event time is at or below the receipted horizon, retiring itself once the replayed watermark passes it. The cut is exact when the sink's input event times are watermark-monotone - windowed and aggregated emissions, whose fired panes the engine stamps with `window_end - 1` - which is why `replay_suppression` defaults on for this sink and why a feed with late or out-of-order records reaching the sink directly should set it `'false'` (an armed sink passes a record it cannot judge through with a loud log rather than drop it). Receipts are pruned with the same retention sweep that purges superseded checkpoints, never below the newest CONFIRMED marker; a fresh submit (restore id 0) ignores them entirely, so a deliberate reprocess-from-scratch still owes and delivers its full output. One receipt per (subtask, checkpoint) - the same one-2PC-sink-per-subtask constraint the staged handle key already imposes. The window between the broker's `EndTxn` response and the receipt's fsync (a kill there leaves a committed transaction with no receipt - qual01-20260818d rode it) is closed over the wire: the walk's `EndTxn` retry can read as fenced even for a commit that landed, because the staged epoch comes from librdkafka's periodic statistics callback while the broker bumps the producer epoch on every commit, so the resolver disambiguates a fenced answer with `DescribeTransactions` (KIP-664) - `CompleteCommit` under the handle's producer id is the commit landing, sound because every resolution walk runs before anything redeploys. Every commit the walk proves over the wire (or executes itself, on a still-prepared transaction) then gets its missing receipt **materialised** by the walk from the watermark horizon the handle carries - staged at the sealing barrier with the exact value the sink's own receipt would have held - and the walk probes **every** handle of a checkpoint before deciding, never stopping at the first refusal: a mixed verdict (one commit proven, a sibling refused) still leaves each proven commit receipted, so the fallback restore's replay of those intervals is swallowed by suppression instead of publishing them twice. qual01-20260819f rode exactly that corner - one subtask's whole window pane, committed twice - before the walk materialised receipts; a handle staged by a pre-horizon binary is warned about and keeps the bounded-replay contract. A staged handle is likewise authoritative only in its OWN subtask's snapshot: union operator-state restore replicates stale copies of every sink's handle into every subtask, and the walk ignores them. Both are fired deliberately in every qualification campaign (`sink.between_commit_and_receipt` is a mandatory fault). On a broker without DescribeTransactions the fenced ambiguity falls back to the bounded-replay floor. Gated end to end by `KafkaWindowRecoveryTest.AFencedPartialCommitFallsBackWithoutDuplicates` (a stranded prepared transaction fenced during the commit-wait window; exact output required across the fallback), by `KafkaWindowRecoveryTest.AnUnreceiptedCommitInAMixedVerdictIsNotReplayedAsDuplicates` (an ack-window kill composed with coordinator loss and a sibling transaction expiring - the walk must materialise the proven commit's receipt before the mixed fallback replays it), and at the sink surface by the `TxnResumeLive` receipt tests.

**Unresolved-orphan markers + the pre-fence describe.** One interleaving remains after receipts and materialisation: the walk ends UNRESOLVED - its transport retries exhausted against a broker outage, or its watchdog cancel tripped - while a handle's transaction sits committed-but-unreceipted (an ack-window kill). The restore then falls below the completed checkpoint, and the old behaviour let the redeployed sink's `init_transactions` fence the orphan blind: fencing aborts an undecided transaction and erases the broker state that could have named a committed one, after which no probe can tell the two apart - the replay published the committed interval twice (qual01 rig-night composite: three keys' windows, duplicated). Three pieces close it. First, the sink writes its commit receipt BETWEEN the broker's commit and the next `begin_transaction`, so while no successor transaction is Ongoing the transaction coordinator's `CompleteCommit` still names the commit a death interrupted. Second, a walk that ends unresolved persists each unsettled handle as a `sub<K>-<N>.unresolved` marker beside the receipts - its mandated final act, written even when cancelled. Third, the redeployed sink consumes its marker at `open()` BEFORE opening the producer: a read-only `DescribeTransactions` against the never-fenced identity answers `CompleteCommit`/`PrepareCommit` (the commit landed - the sink writes the receipt right there and replay suppression arms from it) or an undecided/aborted state (the init's abort is legitimate and the replay owes the interval). While no broker can answer, the sink REFUSES to open at all - failing the subtask loudly rather than fencing blind - so the orphan stays resolvable; the restart cycle retries and converges when the broker returns, usually via the walk's own wire probe (whose `EndTxn` against the preserved identity answers idempotently) materialising the receipt first. Markers are retired by whichever side resolves them and never survive a final broker refusal. A walk that stops early for any reason also marks every unreceipted handle in the completed checkpoints above its stop, so a commit that executed without its receipt one checkpoint above a refused one is described before it is fenced (the refusal wall, found by the [exactly-once specification](../internals/exactly-once-specification.md) rather than by a rig). Gated by `KafkaWindowRecoveryTest.AnOrphanedCommitIsResolvedBeforeFencing` (armed ack-window kill, walk exhausted against a paused broker, the blind-fence interlock observed live, exact oracle across the heal).

- Upsert sink (`kafka_upsert_sink_string`): keyed at-least-once with log-compaction semantics. Each row is keyed by the configured primary key, deletes become tombstones, and replacement relies on Kafka log compaction by key.

## Limitations

- librdkafka commit-mode, batching and timeout knobs (`commit_mode`, `poll_timeout`, `max_batch_size`, `linger_ms`, `produce_timeout`, `flush_timeout`, `fixed_partition`, `metric_prefix`) exist on the `Options` structs but are not parsed from factory parameters, so through the registered factories they always take their struct defaults.
- SASL and TLS are configured through the options in the Authentication and TLS section above (plus the generic `kafka.<property>` escape hatch); other security mechanisms are reachable only via that escape hatch.
- The transactional sink requires a unique `transactional_id`; with `parallelism > 1` the factory appends the subtask index, and the uniqueness contract across producer instances is otherwise the caller's responsibility.
- The upsert sink requires every input value to be a JSON object; malformed or non-object values are silently skipped, and `primary_key` is mandatory.
- Sink delivery is at-least-once unless the transactional (`kafka_2pc_sink_string`) variant is used.

## Testing

The connector suite (`impls/kafka/tests/test_kafka.cpp`) drives librdkafka's in-process mock cluster (`rdkafka_mock.h`), which speaks the real Kafka wire protocol on a localhost port. Its broker-independent tests pin the offset encoding and the deterministic static-member identity. The distributed integration suite adds `KafkaWindowRecoveryTest.WorkerAndHaCoordinatorFailoverKeepSourceWindowAndSinkOnOneCut`: real Redpanda, four partitions, parallelism four, continuous records in an open keyed window while a worker and then the coordinator are killed, stable worker process ids, a transactional sink, and an exact external oracle. It caught partition reassignment replay that every quiescent recovery test missed. The mock tests self-skip when the build has `CLINK_HAS_KAFKA` off (librdkafka absent) or `CLINK_HAS_KAFKA_MOCK` off (librdkafka older than 1.3, which lacks the mock header); the distributed gate self-skips when Docker is unavailable.

Run them with the `kafka` ctest label:

```bash
cmake -S . -B build -DCLINK_WITH_KAFKA=ON -DCLINK_BUILD_TESTS=ON
cmake --build build -j
ctest --test-dir build -L kafka
```
