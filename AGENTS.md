# clink - working guide

Conventions and orientation for working in this repository, whether by hand or
with an AI coding assistant. This is the canonical guide (`CLAUDE.md` is a
pointer to it). It supplements the root `README.md` and the references under
`docs/`; it does not replace them.

## Working conventions

Standing preferences for this repository. Follow them by default.

- Voice: British English. No em dashes; use a spaced hyphen or restructure the sentence. No hype or filler. `scripts/check-no-em-dashes.py` gates tracked prose in CI and the pre-commit hook.
- Framing: describe clink in its own terms in engine code, tests, and docs, and do not explain features by analogy to other engines. The exceptions are the "inspired by Apache Flink" note in the root README, one sentence in `docs/internals/architecture.md`, and the cross-engine benchmark harnesses under `benchmarks/`, where naming the compared engine is unavoidable. Keep those harnesses premise-pinned and correctness-gated, and make no absolute claims about the other engine's capabilities.
- Git: maintainer changes land directly on `main`; contributor changes arrive as PRs targeting `main` (see `CONTRIBUTING.md`). Commit once a unit of work is done and verified, and push only when asked. A commit an AI assistant wrote ends with the trailer `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>` (model name as current), after a blank line; contributors' own commits carry no trailer.
- Commit subjects: conventional prefix with a scope - `feat(cluster):`, `fix(sql):`, `perf(sql):`, `bench(nexmark_compare):`, `test(integration):`, `docs(hardening):`, `build(image):`, `ci(fuzz):`, `chore(qualification):`. Plain `docs:` and `release: vX.Y.Z` are the accepted unscoped forms. `scripts/commit-msg` enforces the prefix and nothing else; the body stays as it is - prose explaining why, and what the change is answerable to. Install the hooks after cloning with `scripts/install-git-hooks.sh`; they are shims onto the tracked scripts, so a new check can never be silently inert. To add a type, edit the list in `scripts/commit-msg` rather than reaching for `--no-verify`.
- Comments: do not add internal roadmap or milestone tags to comments or commit messages. They carry no meaning outside a work plan and were removed deliberately.
- Stateful operators need a uid: give every stateful operator a stable uid, via `.uid("...")` on the fluent `DataStream` / `KeyedDataStream` or `set_uid("...")` on a Dag-direct operator. The uid derives the `OperatorId` (`operator_id_from_uid` in `include/clink/core/types.hpp`) that state restore, rescale, and schema evolution key on. An operator without one cannot have its state restored, so a missing uid is a correctness bug rather than a style nit.
- Sinks declare checkpoint ownership: a sink that writes out at the barrier what it received before it declares `Sink::gates_checkpoint_ack()`, and a two-phase-commit sink declares `Sink::stages_state_at_barrier()` (every `CommittingSink` does), so `Dag::add_sink` refuses another sink beside it. Every class under `include/`, `src/` and `impls/` deriving from `Sink` or `CommittingSink` is listed in `scripts/sink-ownership.txt` as `owns`, or `free` with the reason; `scripts/check-sink-ownership.py` checks the list against the code in CI and the pre-commit hook, so a new sink fails until it is listed. A missing declaration is a correctness bug: beside another sink, the sink loses the guarantee its record claims (see `docs/internals/checkpointing.md`).
- Build parallelism: use `cmake --build <dir> --parallel 10` and `ctest --test-dir <dir> --parallel 8` (or `-j8`). Never use a bare `-j`, as unbounded parallelism can freeze a workstation.
- clang-format: the pre-commit hook runs the `format-check` target with the clang-format CMake finds. On macOS use the Xcode Command Line Tools binary at `/Library/Developer/CommandLineTools/usr/bin/clang-format`, not Homebrew's newer clang-format; the version skew reformats lines differently and blocks the commit.
- Docs are part of "done": at the end of a feature round, update the affected documentation in the same change rather than as a follow-up. That means the relevant `docs/connectors/<name>.md` or `docs/internals/<page>.md` (and its index `README.md`), the capability catalogue `docs/capabilities.md` and the root `README.md` if a capability changed, and the codebase map below if a subsystem or key file moved or was added. If a shipped feature has no doc page, create one. A feature is not complete until its docs match the code.
- Docs location: `/docs/*` is gitignored except the published subdirectories (`consumer-examples/`, `guides/`, `tutorials/`, `connectors/`, `internals/`, `design/`, `qualification/`, `posts/`, `showcases/`, `assets/`), the site pages (`index.md`, `capabilities.md`, `benchmarks.md`, `efficiency.md`, `sql.md`), and `history/` (tracked archives, never published). Do not commit other `docs/*.md`, which are internal work notes; to publish a new docs subdirectory, add a matching `!/docs/<dir>/` exception AND list it in the `exclude_docs` allowlist in `mkdocs.yml`.
- Docs site: the tracked docs publish to https://orhaugh.github.io/clink/ via `.github/workflows/docs.yml` (MkDocs Material, config in `mkdocs.yml`). New internals/connectors pages must be added to the `nav` in `mkdocs.yml`. Never run `mkdocs gh-deploy` from a working machine - untracked internal notes exist locally under `docs/`; only the CI build (which checks out tracked files only, plus the `exclude_docs` allowlist as a second fence) may publish.
- Public API tiers (design record 011): every installed header carries a 1.x tier in `scripts/public-api-surface.txt`, generated by `scripts/gen-public-api-surface.py` and checked in CI and the pre-commit hook; a header changing tier, or a Stable header newly reaching a lower-tier one, is a contract change and lands as a reviewed manifest diff. The Stable C ABI symbol set is `scripts/libclink-abi-symbols.txt` (append-only, `scripts/check-c-abi-symbols.py`). `tests/api_conformance/` and `tests/sql_conformance/` are frozen like `tests/fixtures/`: additions only, and an edit a build needs there is a Stable-tier break, which 1.x does not take. New public headers default to Internal; promote deliberately by adding a rule to the generator.
- Page front matter: a published page's `---` block must be valid YAML, checked by `scripts/check-docs-front-matter.py` in the docs workflow and the pre-commit hook. MkDocs does not fail on a bad block, it renders it as the page's opening paragraph and silently drops the title and meta description, so quote any value containing a colon-space (`description: "... processing: native ..."`).
- Qualification status: the published page under `docs/qualification/` is the authority for a campaign's status; `qualification-plan.json` follows it, gated by `scripts/check-qualification-status.py` in CI and the pre-commit hook. A campaign page is published only when the campaign is green with retained evidence.
- Showcases gate a release: before tagging, run half-time against the release commit on one machine with its `evidence.sh` (its README, "Release evidence": a fresh worktree of the commit, built from a fresh configure), then `scripts/check-showcases.sh --half-time-evidence <bundle>`. The script first judges the bundle with `scripts/check-half-time-evidence.py`: it must be for exactly that commit (the checkout, `clink`, `clink_node` and the riders job's install), from a committed half-time, with the Worker kill, the savepoint and the Coordinator failover executed and every checker passing; otherwise nothing is dispatched. It then dry-runs release-binaries on `main`, runs every other showcase's CI (`docs/showcases/`, each in its own repository) against that SDK, through the `clink_sdk_run` input each showcase's `ci.yml` takes, and prints one table. It exits 0 only when all seven pass. `--partial` leaves half-time out, exits 3 when the rest pass and is not a release check. A showcase that fails holds the tag until the cause is understood: a clink regression is fixed in clink, and a check that was wrong is fixed in the showcase. The validator's tests are `scripts/tests/` (`python3 -m unittest discover -s scripts/tests`), run in CI.
- Protocol changes update the specification: a change to the coordinator's completion, confirmation or restore-point rules, the 2PC sink choreography, or the in-doubt resolution walk is also a change to `formal/ExactlyOnce.tla` (design record 012). Run `scripts/formal-check.sh` (needs a Java 11+ runtime; fetches the pinned TLC) before committing, and `scripts/formal-check.sh --mutants` when a rule changes. A defect the model or a rig finds becomes a named `Bug` value in the specification with a configuration under `formal/mutants/` that TLC must refute; a mutant TLC accepts is a regression in the model, not a pass. The engine emits a protocol trace at those same decision points (`protocol_trace::Event`, on when `CLINK_PROTOCOL_TRACE_DIR` is set): a new or moved emission site updates `formal/trace/events.txt` and `formal/trace/TraceExactlyOnce.tla` together (`scripts/check-protocol-trace-events.py` gates it), and `scripts/formal-check.sh --trace formal/traces` must still accept the recorded runs.
- Diagrams: use a fenced ` ```mermaid ` block, which GitHub renders, not ASCII art. Keep node labels in double quotes; escape `<`, `>`, and `&` as `&lt;`, `&gt;`, and `&amp;`; use `<br/>` for line breaks; and avoid `[`, `]`, `|`, `{`, and `}` inside labels. For data layouts such as struct fields or byte formats, a Markdown table is fine.

## Codebase map

Orientation so work starts informed instead of re-deriving the layout each time.
Read this before scanning the tree. For any "how does X work" or "where does X
live" question, read the relevant `docs/internals/` page first, then follow it to
the source it cites. Keep these pages current when a subsystem changes.

- Deep per-subsystem references: `docs/internals/` (index: `docs/internals/README.md`).
- Per-connector references (dependencies, factory names, options, SQL usage): `docs/connectors/`.
- Runnable API examples: `docs/consumer-examples/` (against an installed clink, `find_package`) and `docs/consumer-examples/fetchcontent/` (clink pulled from GitHub by `FetchContent`; CI builds it from the commit under test).
- The end-to-end tutorial (Kafka -> clink -> ClickHouse on compose, with a Worker kill): `examples/kafka-to-clickhouse/`, walkthrough in `docs/tutorials/kafka-to-clickhouse.md`. Its `run.sh` is what CI runs against the published runtime image; keep the example, the walkthrough and that script telling the same story.
- The quickstart (a Coordinator serving the ops console, two Workers and two jobs, from the published runtime image): `examples/quickstart/`. The console is clink-fe's release asset, pinned in `scripts/console.env` and installed at `<prefix>/share/clink/console` by `scripts/fetch-console.sh` in the SDK tarball and the runtime image; a Coordinator serves it from beside its binary (`include/clink/http/static_files.hpp`). The runtime-image workflow runs the quickstart against each new image.
- The PostgreSQL CDC example (a table's snapshot and changes -> ClickHouse on compose, with a Worker kill and a value-for-value comparison with PostgreSQL): `examples/postgres-cdc-to-clickhouse/`. Its `run.sh` is what the runtime-image workflow runs against each new image; the CDC contract it relies on is in `docs/connectors/postgres.md`.
- The MongoDB SQL example (a collection's change stream into exact per-customer totals, replaced by key, and an appended change log, checked by mongosh against the workload's own record): `examples/mongodb-orders-sql/`. Its `run.sh` runs clink from `CLINK_IMAGE` or, with `CLINK_BIN_DIR`, from a local build; the contract is `docs/connectors/mongodb.md#sql`.
- The Neo4j example (synthetic transfers from Kafka into a graph, a windowed fan-in indicator enriched with reference device relationships through the Neo4j lookup, a Worker kill and an independent check of every node, relationship and enrichment): `examples/kafka-to-neo4j/`. Its `run.sh` runs clink from `CLINK_IMAGE` or, with `CLINK_BIN_DIR`, from a local build; the sink's and lookup's contract is `docs/connectors/neo4j.md`.

| Subsystem | Source | Internals page |
|-----------|--------|----------------|
| Engine core (types, StreamElement, batches) | `include/clink/core/` | `docs/internals/architecture.md`; declared types + derived codec: `docs/internals/derived-types.md` |
| Operators + DAG + fluent API | `include/clink/operators/`, `include/clink/runtime/dag.hpp`, `include/clink/api/` | `docs/internals/operator-model.md` |
| Local runtime / task lifecycle | `src/runtime/local_executor.cpp`, `include/clink/runtime/` | `docs/internals/task-lifecycle.md` |
| Memory accounting and budgets | `include/clink/runtime/memory_budget.hpp`, `keyed_memory_account.hpp`, `memory_size.hpp`, `arrow_memory_pool.hpp`, `src/runtime/memory_size.cpp`; SQL working maps: `include/clink/sql/working_set.hpp`; SQL entry partitions: `include/clink/sql/partitioned_list.hpp`; SQL disk heap: `include/clink/sql/spill_heap.hpp`; SQL scratch spill: `include/clink/sql/spill_store.hpp`, `src/sql/spill_store.cpp`; SQL TTL: `include/clink/sql/state_ttl.hpp` | `docs/internals/memory-management.md` |
| Jobs, parallelism, scheduling, key groups | `include/clink/cluster/job_graph.hpp`, `include/clink/job/` | `docs/internals/jobs-and-scheduling.md` |
| Cluster control plane (coordinator/worker, protocol, HA, plugin model) | `src/cluster/`, `include/clink/cluster/`, `include/clink/plugin/`, `tools/clink_node.cpp` | `docs/internals/distributed-runtime.md` |
| Network / channels / Arrow wire / backpressure | `include/clink/runtime/network/`, `include/clink/runtime/multi_input_alignment.hpp` | `docs/internals/network-stack.md` |
| Time, watermarks, windows, CEP | `include/clink/time/`, `include/clink/operators/`, `include/clink/cep/` | `docs/internals/time-and-windowing.md` |
| Keyed/broadcast state + backends + queryable state | `include/clink/state/`, `src/state/`, `include/clink/queryable_state/` | `docs/internals/state-and-backends.md`, format contract: `docs/internals/state-snapshot-format.md` |
| Checkpointing, barriers, 2PC sinks | `include/clink/checkpoint/`, `src/checkpoint/`, `include/clink/connectors/file_2pc_sink.hpp`; a source that acknowledges at completion: `include/clink/connectors/checkpoint_ack_ledger.hpp` | `docs/internals/checkpointing.md` |
| Sink committer framework (exactly-once / upsert sinks) | `include/clink/connectors/committing_sink.hpp`, `include/clink/connectors/sql_json_builder.hpp`, per-connector `*_2pc_sink`/`*_upsert_sink` in `impls/<name>/` | `docs/internals/sink-committer-framework.md` |
| Failover, rescale, schema evolution, savepoints | `include/clink/cluster/rescale_*`, `include/clink/state/schema_version.hpp`, `include/clink/state_processor/` | `docs/internals/fault-tolerance-and-rescale.md` |
| Arrow columnar execution | `include/clink/core/arrow_batcher.hpp`, columnar hooks in `include/clink/operators/` | `docs/internals/columnar-execution.md` |
| Async substrate + disaggregated state | `include/clink/async/`, `src/async/`, RemoteReadBackend in `include/clink/state/` | `docs/internals/async-state-execution.md` |
| SQL frontend (parse -> bind -> plan -> ops) | `include/clink/sql/`, `src/sql/` | `docs/internals/sql-frontend.md` |
| Embedded execution (`clink run <file>.sql`, EmbeddedEngine) | `include/clink/embed/`, `src/embed/`, `include/clink/sql/script_runner.hpp`, `tools/clink_run_sql.cpp` | `docs/internals/embedded.md` |
| Data lineage (capture + pluggable export) | `include/clink/lineage/`, `src/lineage/` | `docs/internals/data-lineage.md` |
| Connectors (sources/sinks/backends) | `impls/<name>/` (WebSocket: RFC 6455 in-tree under `impls/websocket/`, no client lib; MQTT: the source's MQTT 5 / 3.1.1 client in-tree, `impls/mqtt/src/mqtt_subscriber.cpp` with the packets in `mqtt_protocol.hpp`, and the sink on libmosquitto: the failure state, the refusals, the bounds and the sliced waits in `impls/mqtt/src/mqtt_sink.cpp`, the libmosquitto calls in `impls/mqtt/include/clink/mqtt/mqtt_client.hpp`, the `sinks` phase of `scripts/brokers-live.sh` with the broker's ACL in `docker/mosquitto.acl`, recovery cells `tests/integration/test_mqtt_sink_recovery.cpp`); the Delta Lake sink is part of the SQL frontend (`include/clink/sql/delta_row_sink.hpp`, `include/clink/connectors/delta_log.hpp`, registered in `src/sql/install.cpp`); ClickHouse native sink: `impls/clickhouse/src/native/` (writer, converter, transport; columnar intake in `intake.{hpp,cpp}`; the open, writer, barrier and closes that do not depend on Rows in `SinkCore`, `native_sink_core.{hpp,cpp}`); its typed C++ helper for `CLINK_FIELDS` structs is the installed Internal header `impls/clickhouse/include/clink/clickhouse/native_sink.hpp`, with its non-template half in `src/native/typed_entry.cpp`, page `docs/connectors/clickhouse.md`; Neo4j sink and lookup: `impls/neo4j/` (options, typed JSON encoding and result decoding, reply judging, the shared client and request watch in `connection.{hpp,cpp}`, the sink, and the lookup pool and `neo4j_lookup_join_row` operator in `lookup.{hpp,cpp}`, over `clink::http_connector`'s `HttpRequest::post_bounded`); the planner lowers a `function='neo4j'` lookup table in `src/sql/physical_plan.cpp`, and lookup joins share their output row through `include/clink/sql/lookup_join.hpp`; live gate `scripts/neo4j-live.sh`, recovery cells `tests/integration/test_neo4j_recovery.cpp`; Kinesis source: `impls/aws/` (ownership, discovery, lineage, the read loop and the checkpoint rows in `src/kinesis_source.cpp`, the SDK calls behind `KinesisShardService` in `src/kinesis_sdk_service.cpp`, the tests' model of the service in `tests/kinesis_model.hpp`), live gate `scripts/kinesis-live.sh`, recovery cells `tests/integration/test_kinesis_recovery.cpp`; Kinesis sink: `impls/aws/` (the partition-key rules, the record and request limits, the response judge, the retry loop and failed state in `src/kinesis_sink.cpp`, the SDK call behind `KinesisWriteService` in `src/kinesis_sink_sdk_service.cpp`, the tests' model of the service in `tests/kinesis_sink_model.hpp` and an in-process stand-in for the PutRecords endpoint in `tests/kinesis_http_stub.hpp`), the sink's phase of `scripts/kinesis-live.sh`, recovery cells `tests/integration/test_kinesis_sink_recovery.cpp`; DynamoDB sink: `impls/aws/` (the record checks, numbers, sizes and item identity in `src/dynamodb_item.cpp`, the buffer, write loop, retries and failed state in `src/dynamodb_sink.cpp`, the SDK calls behind `DynamoDbWriteService` in `src/dynamodb_sdk_service.cpp`, the tests' model of the service in `tests/dynamodb_model.hpp`), live gate `scripts/dynamodb-live.sh`, recovery cells `tests/integration/test_dynamodb_recovery.cpp`; Firehose sink: `impls/aws/` (the record and request limits, the response judge, the retry loop and failed state in `src/firehose_sink.cpp`, the SDK call behind `FirehoseWriteService` in `src/firehose_sdk_service.cpp`, the tests' model of the service in `tests/firehose_model.hpp`), live gate `scripts/firehose-live.sh`, recovery cells `tests/integration/test_firehose_recovery.cpp`; the three AWS gates run in the `aws` workflow; Cassandra / ScyllaDB sinks: `impls/cassandra/` (the record and key rules in `src/cassandra_rules.cpp`, the bounded window of writes, the timestamps, the failed state and the table check in `src/sink_core.cpp`, the two sinks in `src/cassandra_sink.cpp` and `src/cassandra_upsert_sink.cpp`, the driver calls behind `CassandraWriteService` in `src/driver_service.cpp`, the tests' model of the server in `tests/cassandra_model.hpp`), SQL tests `tests/test_cassandra_sql.cpp`, live gate `scripts/cassandra-live.sh cassandra|scylladb` (the `cassandra` workflow), recovery cells `tests/integration/test_cassandra_recovery.cpp`; MongoDB change-stream source and sinks: `impls/mongodb/` (the read pass and its checkpoint-token rule in `src/change_pass.hpp`, the sink's key, write concern and index rules in `src/sink_rules.hpp`, the change rows in `include/clink/mongodb/mongo_event.hpp`, the strict BSON-to-SQL conversions both ways in `src/sql_row.{hpp,cpp}`); the planner binds `connector='mongodb'` in `src/sql/physical_plan.cpp` (`check_mongodb_options`), SQL tests `tests/test_mongodb_sql.cpp`, live gate `scripts/mongodb-live.sh`, recovery cells `tests/integration/test_mongodb_recovery.cpp`; MySQL change stream: `impls/mysql/` (the client side, the server checks, the strict restore, the snapshot and its locks, and the barrier hold in `src/mysql_cdc_source.cpp`; event processing, the definitions in force and the decode policy in `src/mysql_cdc_processor.cpp`; the checkpoint row, event and statement classes in `src/mysql_cdc_rules.cpp`; row decoding in `include/clink/mysql/mysql_row_decode.hpp`), the `cdc` phase of live gate `scripts/mysql-live.sh` (MySQL 8.0 and 8.4, the `mysql` workflow), recovery cells `tests/integration/test_mysql_cdc_recovery.cpp`; MySQL sinks: `impls/mysql/` (the session, the table checks, each flush's transaction, the failure state, retries and the cancel watch in `src/mysql_sink_core.cpp`; values, keys and statements in MySQL's dialect in `src/mysql_sink_rules.cpp`, over the record rules the SQL sinks share (`include/clink/connectors/sink_record_rules.hpp`, `src/connectors/sink_record_rules.cpp`: the JSON reader, stream keys, the changelog marker, statement packing, the changelog buffer); the server calls behind `MysqlSinkServer` in `src/mysql_sink_server.cpp`; the two sinks in `src/mysql_sink.cpp` and `src/mysql_upsert_sink.cpp`; the scripted server the failure tests write to in `tests/test_mysql_sink_failures.cpp`), the planner's option check `check_mysql_sink_options` in `src/sql/physical_plan.cpp`, the `sink` phase of `scripts/mysql-live.sh`, recovery cells `tests/integration/test_mysql_sink_recovery.cpp`; PostgreSQL sinks: `impls/postgres/` (the session, the table check, each flush's transaction, the failure state, retries and the cancel watch in `src/pg_sink_core.cpp`; values, keys and statements in PostgreSQL's dialect in `src/pg_sink_rules.cpp`; the libpq calls behind `PgSinkServer` in `src/pg_sink_server.cpp`; the sinks in `src/postgres_json_sink.cpp`, `src/postgres_upsert_sink.cpp` and `src/postgres_sink.cpp`; the scripted server the failure tests write to in `tests/test_pg_sink_failures.cpp`), the planner's option check `check_postgres_sink_options` in `src/sql/physical_plan.cpp`, the `sink` phase of `scripts/postgres-live.sh` (the `postgres-cdc` workflow), recovery cells `tests/integration/test_postgres_sink_recovery.cpp`; Redis sinks: `impls/redis/` (the connection, the command timeout, the cancel watch, the failure state and the pipelined write that reads every reply in `src/redis_sink_core.cpp`; the keyed sink's key rule in `src/redis_sink_rules.cpp`; the two sinks in `src/redis_sink.cpp` and `src/redis_upsert_sink.cpp`; the scripted server the failure tests write to in `tests/test_redis_sink_failures.cpp`), the planner's option check `check_redis_sink_options` in `src/sql/physical_plan.cpp`, the `sinks` phase of `scripts/brokers-live.sh` (the `brokers` workflow, which runs every broker connector's phases), recovery cells `tests/integration/test_redis_sink_recovery.cpp`; RabbitMQ sink: `impls/rabbitmq/` (the failure state, the confirm drain and its tally, the bounds and the cancel watch in `src/rabbitmq_sink.cpp`; the librabbitmq calls behind `RabbitMqPublisher` in `src/rabbitmq_publisher.cpp`; the bounded connect and handshake the source shares in `src/amqp_conn.hpp`; the scripted broker the failure tests write to in `tests/test_rabbitmq_sink_failures.cpp`), the planner's option check `check_rabbitmq_sink_options` in `src/sql/physical_plan.cpp`, the `sinks` phase of `scripts/brokers-live.sh`, recovery cells `tests/integration/test_rabbitmq_sink_recovery.cpp`; NATS JetStream sink: `impls/nats/` (the failure state, the answer tally, the count and byte windows and the sliced waits in `src/nats_sink.cpp`; the nats.c calls and the per-message answer handler behind `NatsPublisher` in `src/nats_publisher.cpp`; the connect and write deadlines the source shares in `src/nats_conn.hpp`; the scripted server the failure tests write to in `tests/test_nats_sink_failures.cpp`), the planner's option check `check_nats_sink_options` in `src/sql/physical_plan.cpp`, the `sinks` phase of `scripts/brokers-live.sh`, recovery cells `tests/integration/test_nats_sink_recovery.cpp`; Generic HTTP sink: its options and the renderer that refuses a record its framing cannot carry in the `http_sink` factory of `impls/http_connector/src/register_factories.cpp`, over the shared `batched_http_bulk_sink.hpp`, the planner's option check `check_http_sink_options` in `src/sql/physical_plan.cpp`, gate `scripts/http-sink-live.sh` (endpoints the tests serve), recovery cells `tests/integration/test_http_sink_recovery.cpp`; Pub/Sub sink and source: `impls/http_connector/include/clink/http_connector/pubsub.hpp` (the publish verdict, the refusal of an empty record, the pull source and its leases) over the shared `batched_http_bulk_sink.hpp`, their options in `impls/http_connector/src/register_factories.cpp`, the planner's option check `check_pubsub_sink_options` in `src/sql/physical_plan.cpp`, the Pub/Sub phases of `scripts/brokers-live.sh` (the emulator), recovery cells `tests/integration/test_pubsub_sink_recovery.cpp`; HTTP polling source: `impls/http_connector/` (the strict page reader, the cursor checks, the bounded retry loop and the conditional state in `src/http_poll_source.cpp`, its options and modes in `include/clink/http_connector/http_poll_source.hpp`, the bounded GET in `src/http_request.cpp`), capability record `http_poll`, whose `replay_requires_options` the delivery analyser honours, live gate `scripts/http-poll-live.sh` (local servers only), recovery cells and SQL paths `tests/integration/test_http_poll_recovery.cpp`; InfluxDB sink: `impls/http_connector/include/clink/http_connector/influxdb_sink.hpp` (the line-protocol renderer and its point rules, and the reading of write responses) over the shared `batched_http_bulk_sink.hpp`, live gate `scripts/influxdb-live.sh`, recovery cells `tests/integration/test_influxdb_recovery.cpp`; Prometheus Pushgateway sink: `impls/http_connector/src/prometheus_push_sink.cpp` (the sample rules and series identity, the group's path, the reading of push responses, the two `push_series` modes, and a restored run's adoption of its group from the gateway's query API), its options in `include/clink/http_connector/prometheus_push_sink.hpp`, live gate `scripts/prometheus-live.sh`, recovery cells `tests/integration/test_prometheus_recovery.cpp`; Splunk HEC sink: `impls/http_connector/src/splunk_hec_sink.cpp` (the event rules, the reading of event and acknowledgement replies against HEC's documented codes, the indexer acknowledgement loop), its options in `include/clink/http_connector/splunk_hec_sink.hpp`, the protocol stub its tests and cells write to in `impls/http_connector/tests/hec_stub.hpp`, simulated gate `scripts/splunk-hec-gate.sh`, live gate `scripts/splunk-live.sh` (a server someone supplies), recovery cells `tests/integration/test_splunk_recovery.cpp`; the search, polling, InfluxDB, Pushgateway and Splunk HEC gates run in the `http` workflow; Elasticsearch / OpenSearch bulk sink: `impls/http_connector/include/clink/http_connector/bulk_sink_builders.hpp` (the document id rules, the record checks and the bulk response verdict) over the shared `batched_http_bulk_sink.hpp` (resends keyed by document, the cancellable retry pause, the `<sink>.before_write` and `<sink>.after_write` fault points), the request watch `cancel_watch.hpp`, live gate `scripts/search-live.sh elasticsearch|elasticsearch9|opensearch|opensearch3` (main, throttled, secured and cluster phases; the three-node clusters in `docker/search-cluster.yml`), recovery cells `tests/integration/test_search_recovery.cpp`, HTTP fault proxy `tests/integration/bulk_fault_proxy.hpp` | `docs/connectors/<name>.md` |
| Schema Registry wire format + value formats (Avro, Protobuf, JSON Schema on Kafka) | `impls/schema_registry/` (client, framing, formats; linked by `impls/kafka/`) | `docs/connectors/schema-registry.md` |
| Testing framework (public, for library consumers) | `include/clink/test/` | `docs/internals/testing-framework.md` |
| Agent-facing diagnostics (MCP server over the CLI and HTTP diagnostic surface) | `python/clink-mcp/` (`clink_mcp/server.py`, tests), `examples/agent-diagnosis/` | `docs/guides/diagnosing-a-pipeline-with-an-agent.md` |
| Exactly-once protocol specification (TLA+, model-checked in CI) and trace validation | `formal/` (`ExactlyOnce.tla`, `models/`, `mutants/`, `trace/` module + `events.txt`, `traces/` recorded runs, `tools.env`), `include/clink/cluster/protocol_trace.hpp`, `scripts/formal-check.sh`, `scripts/protocol-trace-merge.py`, `scripts/check-protocol-trace-events.py` | `docs/internals/exactly-once-specification.md`, design record 012 |

## Pinned toolchain (one-time bootstrap)

Arrow, Parquet, and iceberg-cpp are pinned at the exact versions in
`scripts/versions.env` and installed into `CLINK_DEPS_PREFIX` (host default
`~/.clink-deps`), so the host and the Debian image link byte-for-byte identical
libraries. CMake auto-prepends that prefix and asserts the Arrow version, so any
`cmake` invocation finds it once it exists. Bootstrap it once on a fresh checkout;
idempotent afterwards:

```bash
scripts/build-arrow.sh && scripts/build-iceberg-cpp.sh   # -> ~/.clink-deps
```

`build-arrow.sh` first tries `scripts/fetch-deps.sh`, which restores a prebuilt
archive from the repo's `deps` GitHub release (about a minute) when one exists
for the platform and pin; otherwise it compiles from source (slow). Archives are
verified against the sha256 pins in `scripts/deps-checksums.txt`, and on macOS
refused unless the linked Homebrew `aws-sdk-cpp` keg matches the one the archive
was compiled against (an ABI-drift SIGBUS otherwise). `CLINK_DEPS_FROM_SOURCE=1`
forces the source build. After bumping `scripts/versions.env`: rebuild + repackage
(`scripts/package-deps.sh` on macOS, the `deps-artifacts` workflow for Linux),
upload to the `deps` release, and update `scripts/deps-checksums.txt`.

`./build_and_test.sh` does this automatically. In Docker it is baked into the
image at `/usr/local` (`scripts/setup-build-env.sh`). To change a version, bump it
in `scripts/versions.env`, delete the prefix, and re-bootstrap. Arrow's S3 support
uses the system aws-sdk (Homebrew on the host, built into the image) rather than
Arrow's bundled CRT, which does not build cleanly here; the data-path
dependencies stay bundled and pinned.

## Build & test

The reproducible path is `./build_and_test.sh`, optionally inside the project's
Docker image:

```bash
./build_and_test.sh                                            # normal build + ctest
./build_and_test.sh --sanitizer asan                           # AddressSanitizer
./build_and_test.sh --sanitizer tsan                           # ThreadSanitizer
./build_and_test.sh --sanitizer ubsan                          # UBSanitizer
./build_and_test.sh --sanitizer all                            # normal + ASan + TSan + UBSan
./build_and_test.sh --image clink-build:latest --sanitizer all
```

The local Docker image is `clink-build:latest`. When running sanitizers or
coverage, prefer the `--image clink-build:latest` form, since that is where the
toolchain (clang, lcov, gcovr, the right librdkafka and libpq versions, and so
on) is pinned; the host machine can be missing pieces. Never run two
`build_and_test.sh` invocations concurrently: they share build directories.

Build artifacts go into `build/`, `build-asan/`, `build-tsan/`, `build-ubsan/`,
and `build-coverage/`, all of which are gitignored.

`build_and_test.sh` and the sanitizer CI jobs run ctest through
`scripts/ctest-with-hang-dump.sh <build-dir> [ctest args]`. Where gdb is
available, it attaches to any test still running after
`CLINK_HANG_DUMP_AFTER` seconds (default 240, inside the sanitizer jobs' 300 s
timeout) and writes every thread's backtrace to
`<build-dir>/Testing/Temporary/hang-stacks-<pid>.log` and to the job log. CI
uploads it with the failed run's logs. A timed-out test therefore shows where
it was blocked, not only what it last logged.

## Fast path

For everyday local iteration without sanitizers:

```bash
cmake -S . -B build && cmake --build build --parallel 10 && ctest --test-dir build -j8
```

`ctest --test-dir build -L core` runs just `clink_core_tests`; `-L kafka`,
`-L postgres`, `-L clickhouse`, `-L s3`, `-L rocksdb`, `-L tls`, and
`-L integration` hit the per-impl test executables.

The SQL frontend is ON by default (`CLINK_BUILD_SQL`), matching what CI, the
release binaries, the runtime image and the wheel all build. Pass
`-DCLINK_BUILD_SQL=OFF` to skip it; `build_and_test.sh` pins it off for the
sanitizer and coverage modes, because the SQL runtime tests hang under ASan and
because the recorded coverage baseline was measured without them.

**`-L` takes a REGEX, so `-L sql` also matches the `mysql` label.** It reports
the frontend cases plus the connector ones, and on a build configured with
`-DCLINK_BUILD_SQL=OFF` it reports a green "sql" run made entirely of the
connector cases - a pass that tested no SQL. Use `-L '^sql$'` when you mean the
frontend. Note also that the SQL suite takes about four minutes on its own, so
it is not part of a quick `-L core` loop.

## Nexmark benchmark ("run a nexmark run")

The harness lives in `benchmarks/nexmark_compare/`. Two scripts do different jobs:

- `throughput_sampled.sh` is the canonical throughput run, and what "run a nexmark run" means by default. It measures sustained engine-side records per second by polling each engine's own counter and taking the maximum slope, so the figure excludes deploy time, JVM warm-up, and the end taper. Defaults: `q0 q12`, parallelism 4, 5M events. Both engines run containerised.
- `run.sh` is the correctness-gated comparison. It times broker append and gates on identical output-row counts. Defaults: `q0 q12`, parallelism 1, 500k events. It runs the host build (`BUILD_DIR`, default `build/`).

Canonical command for a before/after throughput check:

```bash
cd benchmarks/nexmark_compare
SINK=blackhole EVENTS=10000000 QUERIES="q0 q12" PARALLELISM=4 ./throughput_sampled.sh
```

`SINK=blackhole` discards output (using the `q*_bh.tmpl.sql` variants), removing
the Kafka write ceiling so the measurement reflects engine read and process rate
only. `SINK=kafka` (the default) writes to a topic and gates on row count.
`KEEP_UP=1` leaves the cluster running. `STATE_BACKEND=<uri>` (for example
`rocksdb:///tmp/nx-state` or `forst:///tmp/nx-state`; paths are inside the
worker containers) passes a per-job `--state-backend` to clink's submit; unset
keeps the canonical in-memory premise, and a set value makes the run
clink-vs-clink tracking for that backend (the compared engine stays on its
compose-pinned hashmap, so the cross-engine ratio no longer holds the matched
premise). `RUN_TAG=<tag>` suffixes the clink result filenames
(`q12-clink-<tag>.json`) and scopes the startup wipe to that tag, so backend
variants sit side by side. A `forst://` run needs the runtime image built with
the opt-in engine: `docker build --build-arg CLINK_WITH_FORST=ON -t
clink-runtime:latest -f docker/Dockerfile.runtime .`

Four things to get right for a valid before/after:

1. `throughput_sampled.sh` runs the `clink-runtime:latest` Docker image, not the host `build/`. The host build only compiles `nexmark_dump` (data generation) and `clink_submit_sql` (submission), so a code change is not measured until the image is rebuilt at the new commit. Rebuild it first (`verify_distributed.sh` builds or refreshes `clink-runtime:latest`), then run. Check freshness with `docker image inspect clink-runtime:latest --format '{{.Created}}'` against the commit under test.
2. The columnar path fires on Kafka-JSON by default: the SQL planner emits `json_string_to_row_columnar` for every Kafka JSON table, the keyed shuffle splits the Arrow sidecar without materialising rows, and the windowed fold ingests columnar (see the nexmark README "Columnar JSON decode A/B"). For a columnar-vs-row A/B: `ENGINES=clink` runs clink only; patch the `q*_bh.tmpl.sql` DDL with `columnar_decode='false'` for the baseline under a separate `RUN_TAG`; give EACH measured variant a freshly composed stack - chained runs on one warm cluster drift monotonically in CPU and will erase the delta. `KEEP_UP=1` is for debugging, never for measured runs.
3. Only `q0` (projection-style passthrough) and `q12` (windowed GROUP BY) are sampled for throughput. `q6` and `q8` have tiny join inputs that drain in under a second, so they live in the gate harness rather than here.
4. A `STATE_BACKEND` run on a synchronous backend (`rocksdb://`, plain `forst://`) is an integration check, not a state-backend benchmark. The SQL window and aggregate operators keep hot-path state in in-memory maps unless the backend defers reads (`supports_async_get()`); a synchronous backend carries checkpoint and restore durability only. Both operators do flush to it at snapshot time and reload in `open()`, so an open window or accumulator survives a restore. Measured: q0/q12 throughput deltas across memory, rocksdb and forst runs sit inside the harness's run-to-run variance. The deferring backends are `remote-read://`, `forst://...?defer_reads=1`, and `s3sst+forst://` (deferring by default) - with those, per-record state genuinely rides the backend and the operators take their async KeyedState paths. For a per-record backend A/B on the engine side, use `benchmarks/inproc_compare` with `CLINK_STATE_BACKEND=rocksdb://<dir>` vs `forst://<dir>` and `CLINK_WB_STATE_CACHE=0` (strict writes); measured there at parity, with a one-off ~5s cold-start on the first-ever forst run.

Results are gitignored: `results-sampled/` (sampled runs), `results/` (`run.sh`),
and `results-containers/`. Each is a per-query JSON with `sustained_slope` (the
headline records per second), `drain_rate`, `reached_target`, and CPU and wall
seconds. Compare `sustained_slope` before and after for the same query,
parallelism, event count, sink, and state backend.
