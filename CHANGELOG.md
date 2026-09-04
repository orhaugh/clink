# Changelog

## Unreleased

**The public API is tiered, and the Stable tier is held by gates.** Design
record 011 settles what "stable public APIs" means for the 1.x line: source
compatibility on a declared Stable tier, change-with-notice on an Evolving
tier, and no promise on the Internal headers a plugin's include closure
happens to reach. Every installed header now carries a tier in
`scripts/public-api-surface.txt` (126 Stable, 71 Evolving, 265 Internal at
adoption), generated and checked in CI; the manifest also names the
lower-tier headers a Stable header exposes, so a newly reached internal type
is a reviewed diff rather than a side effect. Which members are promised is
enumerated by `tests/api_conformance/`, compile-only units that use every
promised class, function, macro and virtual hook (hooks overridden with
`override`, so a changed signature fails the build), alongside a check that
compiles each Stable header on its own. The SQL dialect gets the same
treatment: `tests/sql_conformance/` freezes thirty scripts with their inputs
and outputs, run through the embedded engine; a persisted catalog directory
is a compatibility domain with frozen fixtures; and a user-defined function
now shadows a built-in of the same name, so adding a built-in in a later
release can never change what an existing script computes. The published
[Compatibility](https://orhaugh.github.io/clink/compatibility/) page states
the promise surface by surface.

**A process function becomes an operator through a public factory.** A
Dag-direct consumer used to reach into `clink::detail` for the adapter that
turns a `KeyedProcessFunction` into an operator; consumer example 04 did.
`make_process_operator`, `make_keyed_process_operator`,
`make_co_process_operator`, `make_keyed_co_process_operator`,
`make_async_keyed_process_operator` and `make_async_keyed_co_process_operator`
now do that on the Stable tier, deducing the key, input and output types from
the function class; the co-input and async bases gained the type aliases that
deduction reads. The adapters stay where they were, unpromised.

**The embedded C ABI is version 2, and can now grow without a version 3.**
`clink_engine_options` gained a leading `struct_size` field, filled in by
the new `CLINK_ENGINE_OPTIONS_INIT`; the library reads only the fields the
caller's declared size covers, so options appended in later 1.x releases
are invisible to older binaries and older binaries get defaults from newer
libraries. A zero `struct_size` is refused by name rather than guessed at.
`clink_version()` returns the library release string for logging. The
exported symbol set is now a tracked, append-only manifest
(`scripts/libclink-abi-symbols.txt`), held equal to the header in CI and
to the built library's dynamic symbol table as a test. This is the one
deliberate break before 1.0 (`CLINK_EMBED_ABI_VERSION` 1 to 2): C callers
recompile against the new header and initialise their options with the
macro; `pyclink` tracks the change and exposes `Engine.version`.

**A Kafka source can no longer be wedged by its group coordinator.** The
tutorial's fresh stacks intermittently read nothing at all: the source
assigned its partition and every gauge sat at zero, healthy, until the
Worker was restarted. librdkafka's own debug log named the mechanism -
the partition was assigned at `OFFSET_STORED`, the first OffsetFetch
answered `NOT_COORDINATOR` while the broker's lazily created offsets
topic settled, and the client re-confirmed the same coordinator without
ever re-serving the pending assignment, so no fetch was ever sent. A
fresh partition's start offset is now resolved to a concrete number
before `assign()`: the group's committed offset with bounded retries,
else the reset policy via broker watermarks, else the broker-resolved
logical offset - the group coordinator is out of the fetch path
entirely. Reproduced deterministically against the mock cluster (the
same injection wedged the old code and passes now), with both
coordinator-outage shapes pinned as tests.

**A first end-to-end tutorial, and what building it found.** `examples/kafka-to-clickhouse`
brings up Kafka, ClickHouse and a clink Coordinator and Worker with one
`docker compose up`, streams a deterministic sensor workload through an
event-time windowed SQL pipeline into ClickHouse, has the reader kill the
Worker mid-stream and start it again, verifies every window against an
expectation recomputed independently of the engine, and opens the job's
keyed state as an Arrow table. The walkthrough is published as "Your first
real Clink pipeline"; `run.sh` runs the whole thing unattended and is what
CI executes against the published runtime image. The same pipeline was run
unchanged, at parallelism four, on a distributed Coordinator/Worker cluster
with a Worker killed mid-stream.

Writing it against the real engine surfaced four defects, all fixed here.
The ClickHouse sink never flushed at a checkpoint barrier, so a row still
in its buffer when the process died after that checkpoint was lost on
recovery, against a connector documented (and, in its own capability
record, declared) as at-least-once; it now flushes in `on_barrier`, and
the tutorial's SQL sets `batch_rows='1'` so it is also correct on v0.8.0.
`clink --capabilities` rendered its manifest before any connector's
`install()` had run, so the published image's manifest listed six
built-ins and stated that Kafka, ClickHouse and every other compiled-in
connector were absent; the CLI now installs the linked connectors first,
and the manifest gate checks the CLI's output as well as the registry. An
idle snapshot-worker queue was reported as `BOUNDED_CHANNEL_STUCK` every
few seconds, escalating to `held=189s`, on a Worker with nothing to do;
`BoundedChannel::mark_idle_pop_normal` lets a consumer that legitimately
waits declare so, and the push-side stall warning is untouched. And the
state inspection commands (`state-cat`, `state-diff`, `state-export`,
`state-query`, `check-savepoint`, `capture-cat`, `replay-diff`) default
their log level to off, as `--capabilities` already did, so `state-query`
no longer prints the embedded engine's task lifecycle around its rows.

**The runtime image's multi-arch manifest is published again.** v0.8.0's
tag run built and pushed both architectures and then failed at the final
step, so `:0.8.0` was never published and `:latest` still pointed at the
previous, amd64-only index: pulling the image on an arm64 machine
reported no matching manifest. The cause was a shell subtlety rather than
anything in the image. `IFS` governs the word splitting of expansions,
not of literal words in the source, so setting `IFS=','` and looping over
a comma-joined tag list that arrives as a literal ran the body once and
emitted `-t a,b,c`, which the registry refused as an invalid reference
format. The split now goes through an expansion.

The same change makes that failure shape cheap to recover from, because
its cost was structural: two long builds succeed, one registry operation
fails, and the only remedy was building both images again. A
`manifest_only` dispatch mode re-creates the manifest from per-arch
digests already in the registry, in about a minute, and the existing
guards still apply to it - the refusal to tag anything but a complete
pair, the smoke run of both binaries, and the check that what was
published really covers both architectures. The v0.8.0 tags were
republished that way and now carry linux/amd64 and linux/arm64.

## v0.8.0 (August 2026)

The launch release: the qualification programme run across the engine's
guarantee surface, and the repository reshaped for public use.

**Ten further qualification campaigns, published green.** After v0.7.0's
QUAL-01, ten more campaigns ran under continuous fault injection, judged by
independent oracles, and were published only once green with evidence
retained: PostgreSQL two-phase commit (QUAL-02), S3 staged multipart commits
(QUAL-03), 29 GiB of keyed state on a disaggregated backend (QUAL-04),
bounded state through declared retention (QUAL-05), wide job graphs at 147
operators and 292 subtasks (QUAL-06), content-level agreement with an
independent reference engine on 19 of 19 queries (QUAL-07), a rolling engine
upgrade with exactly-once continuity (QUAL-08), infrastructure faults from
ENOSPC to stepped clocks (QUAL-09), a running job's keyed-state type changed
and migrated at restore (QUAL-11), and a declared security-refusal matrix
(QUAL-12). The published page is now the authority for a campaign's status,
enforced by a repository gate in CI and the pre-commit hook.

**Engine defects the campaigns found and fixed.** A committing sink holds
open until its final commit lands; checkpoint orphans left by missed
completion broadcasts are swept; chain tasks' state backends register for
retention; a job whose checkpoints fail persistently fails instead of
crashlooping, and the checkpoint-failure circuit breaker judges duration
rather than ticks; a terminal job's HA manifest is retired so recovery
cannot resurrect it; an absent or incomplete snapshot is re-checked before a
restore is refused; savepoints are pinned against retention for the life of
their job; a departed peer is no longer read as a restart cause of its own,
and the network bridge's data-loss detector stays on the send side, where it
works; a worker releases a job's per-operator registrations when the job's
last subtask leaves it; terminal-cancel convergence is bounded, worker
cancels latch for still-constructing tasks, and drain accounting no longer
waits on dead workers; restart drains tolerate a worker lost or replaced
mid-drain; channel closes carry a reason, so a cancel never reads as
end-of-input, relay sources forward their feed's cancellation, and the
end-of-stream ceremony obeys the close reason; the stuck-channel warning
backs off exponentially and states when the wait ended; SQL sinks write the
table's declared schema rather than the row's internal one; the columnar
watermark assigner stamps event times it used to discard; a declared
`state_ttl` bounds DISTINCT and set operations, a retention deadline can
never precede the record that set it, `ALLOW UNBOUNDED STATE` reaches the
planner, and retention is observable through tracked-key and released-key
metrics; `RemoteReadBackend::scan` sees the durable tier, not just what is
hot; the compiled-job submit path reports the job id it created; a subtask
whose operator threw fails instead of completing empty; transient accept
failures no longer read as a clean end-of-stream; the control plane refuses
a TLS configuration it cannot honour, in every build configuration; and the
worker reports its open-file limit at startup, loudly when low.

**Runtime image.** jemalloc is now the image's default allocator, with
Arrow's pool routed to the process allocator (`ARROW_DEFAULT_MEMORY_POOL=
system`). Under repeated recovery, glibc arena retention plus Arrow's
bundled pool kept gigabytes of freed memory resident; the paired defaults
return it - allocator retention, measured, not an engine leak. The
capability manifest and `clink_node --version` report the allocator in use.
The image is published for arm64 as well as amd64, and one command runs a
complete example with the pipeline and its data baked in:
`docker run --rm ghcr.io/orhaugh/clink-runtime:latest run
/opt/clink/examples/sql/hello.sql`.

**Repository, prepared for launch.** The README is a concise landing page
and `docs/capabilities.md` is the authoritative capability catalogue; the
closed production-hardening record is archived under `docs/history/`; the
security policy is release-oriented; structured bug reports, a PR template
and a Discussions route ship under `.github/`; the commit-subject
convention is enforced by a hook rather than described.

No REST API breaks. Wire protocol v2 unchanged in negotiation
(`CommitCheckpoint` gained a backwards-compatible tail field). Snapshot
format unchanged. Plugin ABI v1 unchanged.

## v0.7.0 (August 2026)

The qualification release. 318 commits whose centre of gravity is one
question: do the guarantees hold when processes die at the worst possible
instant? The answer is now published evidence rather than an architecture
claim.

**QUAL-01: Kafka exactly-once, qualified and published.** A windowed
aggregation from Kafka through the transactional sink ran two hours on a
multi-host rig under continuous fault injection - kills armed inside the
two-phase-commit protocol's own windows, coordinator SIGKILLs, broker
restarts and outages, network partitions - and an independent seeded oracle
judged 755/755 windows byte-exact: zero missing, zero duplicates, zero
foreign. The full report, method and honesty-bounded claims are on the docs
site under Qualification. The campaign machinery (fault points compiled
into the runtime, the chaos controller, the oracle) ships in-tree.

**The exactly-once machinery the campaign forced into existence.**
Commit-confirmed restores (a confirmed checkpoint now means its external
commits executed); prepared-transaction resume over the Kafka wire protocol
(orphaned commits finalised at restore-point selection, speaking SASL/PLAIN,
SCRAM-SHA-256 with server verification, and TLS); durable commit receipts
written inside the ack window, with replay suppression swallowing exactly
the re-emissions a receipted commit covers; in-doubt resolution that probes
every handle, materialises receipts for wire-proven commits, persists
unresolved orphans as markers, and is cancellable at a deadline without
abandoning safety; the sink's pre-fence describe, which refuses to open a
producer while its predecessor's transaction is unknowable - because fencing
first erases the only evidence of whether it committed. The single-interval
transaction queue rebuild keeps one checkpoint interval per broker
transaction under every restart shape.

**Cluster robustness under sustained faults.** Worker commit dispatch and
the coordinator-contact lease no longer conflate a busy reader with a dead
peer (a broker-blocked commit or an OS-stalled plugin dlopen severed healthy
workers' sessions); restarts held on missing capacity wait for workers to
return instead of failing the job; checkpoint numbering rises above every
snapshot file any incarnation left on disk, so restart storms cannot
assemble one checkpoint id from two vintages; superseded coordinators are
fenced by epoch with real compare-and-set metadata fencing; restart drains
tolerate sinks legitimately blocked in bounded client calls. Every
coordination record now sits behind one store seam, with filesystem and S3
conditional-PUT implementations sharing a typed contract suite.

**Hot rescale.** Changing an eligible operator's parallelism now runs as an
in-place cutover at a checkpoint barrier - arm, cut, rebind, deploy, swap,
complete - with only the rescaled operator's subtasks cycling; every
ineligible or failed attempt falls back to the stop-the-world replan. Jobs
can declare rescale bounds in the fluent API, and graceful stop-at-savepoint
lands alongside.

**Production-hardening round closed.** The full F1-F101 board from the
adversarial audit: among them per-operator key-group slices, restores that
refuse subtasks a checkpoint never named, real TTL on List/Map/Aggregating/
Reducing and CEP partial-match state with backend expiry compaction, SQL
that refuses clauses it used to silently drop, state_ttl genuinely bounding
the streaming joins, protocol-corrupt receives failing the task instead of
reading as end-of-input, and fatal signals leaving a stack.

**Compatibility, made explicit.** The control-plane wire protocol is now
version-negotiated (v2, with v1 peers retained for rolling upgrades); the
snapshot format version that was only ever written is now enforced at read;
a compatibility-domain inventory with frozen-bytes fixtures pins each
encoding; the capabilities manifest declares its schema version and build
origin. Plugin ABI unchanged (v1).

**Observability and operations.** Real OTLP export - metrics plus lifecycle
spans for submit, HA recovery and rescale - to any OpenTelemetry collector;
checkpoint-staleness and restart-kind metrics; per-job state size; a shipped
Grafana dashboard and a runbook for the shipped alerts; `clink lint
--from-job` linting what is deployed, cross-checked against the
delivery-guarantee analyser.

**Testing surface.** Source and sink contract suites where a capability
claim is a test obligation (the 2PC crash windows run as capability-gated
obligations against real transaction state - and the source suite corrected
parquet's record on its first run); libFuzzer targets whose findings become
permanent regression tests; the SQL differential oracle against a pinned
reference; content-addressed plugin shipping so bytes travel at most once
per receiver.

No REST API breaks. Wire protocol v2 negotiates down to v1. Snapshot format
unchanged (now enforced). Plugin ABI v1 unchanged.

## v0.6.0 (July 2026)

Two engine improvements, both surfaced by driving the SQL-native AI surface with a
real downstream consumer. No REST API or state-format breaks; plugin ABI unchanged (v1).

**Metadata pre-filter on `VECTOR_SEARCH`.** A trailing
`filter_eq='query_col:corpus_col,...'` option scopes each query to the corpus rows
whose named columns equal the query's (a null query value imposes no constraint). It
is a genuine per-query PRE-filter - the operator scores only the matching corpus
subset exactly - so restricting a similarity search by metadata (a document's system,
tenant, and so on) does not lose recall the way post-filtering a top-k would. The
bound columns are validated at plan time to exist in the query input and the vector
table; it combines with the exact flat index, and pairing it with the approximate
HNSW index is a follow-on.

**`clink replay` reconstructs linked-impl operators.** Replay rebuilt an operator from
its `op.json` capture sidecar using only the core SQL Row factories, so a captured job
using an impl operator (`VECTOR_SEARCH`, `ML_PREDICT`, a connector) failed with "no
registered factory" unless a plugin happened to register it. The replay command now
installs the linked impls the same way `clink run` does, so any job clink can run it
can also replay, with no plugin; `--plugin` still layers a downstream job plugin's own
operators on top.

Both ship with tests (`VectorSearchOperator.FilterEqRestrictsToMatchingSystem`,
`ReplayCli.ImplOperatorJobReplaysWithoutAPlugin`, plus the SQL bind and physical-plan
cases) and updated internals docs.

## v0.5.0 (July 2026)

This release hardens the cluster path for a shape the earlier releases never
exercised end to end: a bounded source, checkpointing on, side-output sinks,
and a fan-out topology, deployed as a compiled job plugin and recovered across
a hard worker kill. Every fix below was found by driving that shape with a real
downstream consumer. No REST API or state-format breaks; the plugin ABI is
unchanged (v1).

**Fluent CEP timed-out side output.** `PatternStream::select_with_timed_out<U>(fn, tag, timed_out_fn)`
mints a `CepOperator` with its timed-out emitter wired, and the side stream
comes from the standard `.side_output<U>(tag)` idiom. A spec-built (and
therefore cluster) job can now alert on the absence of a pattern's completion,
not only on a match.

**`clink replay` for plugin-typed operators.** `EpochReplay` was Row-only.
`register_operator<In, Out>` now hangs a type-erased replay driver on the
operator's factory (capturing `In`'s codec to read the capture and `Out`'s to
serialise emissions), so `clink replay --plugin=<so> --verify` and `--emit-test`
work on custom C++ channel types, not just SQL Row. A single-operator replay
discards emissions to unregistered side outputs rather than aborting.

**`clink_submit_job --capture-dir` / `--capture-records`.** The flight recorder
can be armed on a cluster job from the CLI; the fields were already carried end
to end, only the flags were missing.

**The runtime image is a job SDK.** `docker/Dockerfile.runtime` now installs the
headers, static libraries and CMake package and ships `clink_submit_job`, so a
job plugin builds and submits inside the image against the exact engine commit
the cluster runs (the ABI gate is git-SHA equality).

**`within()` binds at match time.** A completing event arriving between
watermarks can no longer produce a match whose event-time span exceeds the
bound; the check no longer waits for watermark-time pruning.

**Checkpoint barriers and watermarks reach side-output channels.** A
side-output consumer previously never saw a barrier, so a checkpointed bounded
job with a side-output sink hung at its end-of-stream final checkpoint (the
pending-ack set never emptied). This is the fix that lets the whole cluster +
checkpoint + side-output shape complete.

**Worker-loss recovery rolls the whole job back** rather than relocating only
the lost worker's subtasks. A mid-stream kill left surviving upstreams holding
stale bridges to relocated peers, whose send failures cascaded into
restart-budget exhaustion.

**Cluster-built sinks get a stable, unique identity** from their spec node id
when no uid is set, so sibling stateful sinks of the same type no longer collide
on one `OperatorId` (which, for the 2PC sink, collided the `PREPARE TRANSACTION`
gid). Fixed across all three sink-build paths: fused, standalone, and the plugin
`register_sink` runner.

**The planner never fuses a side-output consumer** as a chain's next operator
(it had matched by upstream id, ignoring the side tag), and the worker resolves
side-output attachers through the job bundle rather than the process-wide
default (invisible to a dlopen'd plugin under `RTLD_LOCAL`).

**Packaging.** The installed CMake package declares its OpenSSL (HTTP TLS) and
ZLIB (httplib gzip) dependencies, so a consumer linking the HTTP surface
(queryable state) through the prebuilt SDK no longer fails at link with undefined
OpenSSL or zlib symbols.

Every fix ships with a regression test; the new cluster behaviours are pinned by
in-process `TestCluster` tests where reproducible.

## v0.4.0 (July 2026)

A small, focused release: a new connector, prebuilt Linux binaries, and one
crash fix that lean builds of v0.3.0 need. No API or format breaks.

**WebSocket source.** `connector='websocket'` connects to a `ws://` or
`wss://` push feed - the delivery mechanism of most market-data and event
APIs - sends the venue's subscribe message, and emits each text message as
a record; a declared `format='json'` schema rides the columnar JSON decode
exactly as a Kafka table does. RFC 6455 is implemented in-tree over POSIX
sockets (the protocol layer is pinned in tests to the RFC's own worked
examples), so the impl adds zero dependencies: plain `ws://` needs nothing,
`wss://` uses OpenSSL when present. Delivery semantics are stated plainly:
a push stream has no offsets, so at-most-once across restarts - the
documented patterns are bridging to Kafka for durability, or pairing with
the flight recorder, which makes an unreplayable feed locally replayable.
Reconnect with capped backoff re-sends the subscription. Verified against a
real venue: one inline `clink run` statement pulled live trades off a
public exchange stream through TLS into a file
([docs/connectors/websocket.md](docs/connectors/websocket.md)).

**Prebuilt Linux binaries.** Every release now carries
`clink-<ver>-linux-x86_64-ubuntu24.04.tar.gz`: a relocatable SDK prefix -
CLI, daemon, static libs, headers, CMake package, with the pinned Arrow
bundled and `$ORIGIN` rpaths - built at an honest Ubuntu 24.04 glibc floor
and smoke-tested both as a CLI and as a `find_package(clink)` consumer.
Scope is the dependency-free impl set (SQL, file/Parquet, RocksDB state,
HTTP, TLS, WebSocket, Avro, vector search; no object stores, no broker
connectors). A source build keeps everything.

**The embedded dashboard SPA is gone; the coordinator serves the real
console instead.** The hand-rolled page compiled into `clink_node` predated
the [clink-fe](https://github.com/orhaugh/clink-fe) ops console and is
removed. In its place, `clink_node --http-static-dir=<dir>` serves any built
console bundle (clink-fe's `dist/`) same-origin at `/` beside the JSON API -
one port, no CORS setup, no separate web server - with SPA deep-link
fallback, extension-derived content types, and a traversal-guarded,
unit-tested resolver; without the flag, `/` answers with a JSON signpost.
The REST API is untouched. Wildcard HTTP routes (`/foo/*` ->
`path_params["*"]`), documented since the server's first version, are now
actually implemented.

**Crash fix for lean builds.** v0.3.0's vector_search impl registered its
Row-channel operator without registering the Row type, which took the
embedded CLI down at startup ("In not registered") in any build without the
Iceberg impl - whose install happened to register the type first in full
builds. The impl now self-registers the type idempotently. Relatedly, the
Iceberg impl now skips itself (with a clear message) against an Arrow built
without S3, instead of every consumer of `clink::iceberg` failing at link
with undefined `arrow::fs::S3*` symbols.

## v0.3.0 (July 2026)

Ninety-nine commits of engine, benchmark and correctness work since v0.2.0,
not counting this release commit. No public header was removed or renamed
(header changes are additive), and snapshot and savepoint encodings are
unchanged - the GROUP BY accumulator's byte layout was audited for this
release. Operators that previously lost state at a restore now persist it, so
a v0.2.0 snapshot still restores and simply carries none of that newly
persisted state. One behavioural note: the keyed-shuffle routing fix below
means a savepoint restored across the upgrade can move keys between subtasks,
the same class of movement as a rescale.

**The benchmark suite is published, and every query is gated.** The full
17-query nexmark suite now runs on a five-node cluster against canonical data,
each query correctness-gated against an independent oracle at parallelism 4,
and every window kind gated cross-engine. Headline, measured: clink processes
an event for 1.9x to 5.3x less CPU (median 2.45x) than a JVM stream processor
producing identical output. Two earlier q18/q19 figures were found unsound,
withdrawn and re-measured on the corrected harness. The docs site gained a
[Benchmarks](https://orhaugh.github.io/clink/benchmarks/) page and a costed
footprint model (instances, dollars, modelled CO2e).

**Making that gate honest found real defects, now fixed.** The row and
columnar carriers of a keyed shuffle disagreed about which subtask a key
belongs to - the row side read the key through a double, so 74.5% of
FNV-folded keys misrouted at parallelism 4, silently splitting group state
across subtasks; both carriers now read the exact integer. A data batch larger
than the send-credit window is split instead of silently dropped, a failed
send fails the task instead of vanishing, and an unpartitioned stateful
operator is no longer fanned out to produce N answers.

**A restore now preserves what was open.** An open window survives a restore
(tumbling, hopping, session and cumulate - previously every open window was
silently lost), and so do top-N retained rows, LAST_N state, the OVER
aggregate's sync path, and the upsert and netting sinks' compaction view.
Window arithmetic is floored rather than truncated and shared by all ten
sites, event-time reads are exact, window arguments fail at bind time, and a
task that cannot build fails its job.

**Performance.** Projection pushdown reaches the columnar JSON bridge (45% off
the shuffle split, 27% off decode); per-group aggregate state fell by a third
(AggState 264 -> 104 bytes, held there by a static_assert); the windowed fire
stopped scanning every group on every watermark (4.4x at 200k groups); the
keyed split gathers with index + Take (31-38% off); JSON decode is on-demand
(2.5x on the biggest shared cost); the Kafka source fetches in batches (22%
off source CPU per record, and its old batch default cost 4.6x on a saturated
consumer); each parallel pipeline instance is co-located so forward edges stop
crossing the network (+20% on a forward-only query at parallelism 12) - and
the in-process fast path those edges use, dead in every container deployment,
is live again.

**SQL.** WHERE accepts expression operands, and predicates carrying them are
understood beyond the filter operator; a MATCH_RECOGNIZE DEFINE predicate
whose operand is an expression (`price < PREV(price) * 0.997`) now matches,
where it previously compiled to a reference no row resolves and silently
never fired; declared FLOAT and DECIMAL types are honoured at columnar JSON
decode; non-integral doubles are no longer written at six significant digits;
a batch materialises columns by name rather than by declared position.

**Embedded.** The engine configures logging on first open and honours the
`CLINK_LOG_LEVEL` env var (synchronous sinks; a host that initialised logging
first wins), so `clink run` and pyclink stop printing registry chatter that
could not be turned off.

**Build and packaging.** Installed binaries now carry an rpath to the pinned
toolchain, so a host `cmake --install` produces a runnable `clink` - on macOS
the installed binary previously had no `LC_RPATH` at all and dyld refused to
load it (the build tree and the Docker image had masked this). `CLINK_ISA_BASELINE`
makes the x86 ISA floor a decision rather than an accident (AVX2 was measured
to buy nothing on this workload); `CLINK_WITH_JEMALLOC` is opt-in and
observable (`clink_node --version` prints the loaded allocator); an installed
clink now tells its consumers to resolve ArrowCompute, a `find_package(clink)`
fix; simdjson is pinned and kept out of the public headers.

## v0.2.0 (July 2026)

Forty-four commits of engine, build and CI work since v0.1.0. No API or format
breaks: state written by 0.1.0 restores
unchanged, and every new behaviour below is either on by default with a
documented opt-out or off by default.

**Distribution.** The runtime image is published:
`ghcr.io/orhaugh/clink-runtime` (`:0.2.0`, `:latest`, `:main`, `:sha-<short>`),
built by a new workflow on every release tag. The Helm chart, the k8s operator
and its samples now default to it, so `helm install` works without building an
image first - previously they referenced a tag that existed only on the author's
machine. Prebuilt `pyclink` wheels build again on a release tag
(`macosx_14_0_arm64`, self-contained, vendoring no dylibs); two deliberate
reductions keep the macOS floor low - the wheel has no object-store filesystems
(`s3://`, `gs://`, `abfs://`) and no HTTPS in its HTTP subsystem. Source builds
keep both. A documentation site publishes to
[orhaugh.github.io/clink](https://orhaugh.github.io/clink/).

**Columnar execution reaches both ends of the pipeline.** Columnar JSON decode is
now the DEFAULT for Kafka tables (`columnar_decode='false'` opts out), with an
adaptive damper so systematically unfaithful data pays ~1.6% rather than 2x, and
the keyed shuffle splits columnar batches on the cluster path with zero row
decode. Operators can now also EMIT born-columnar output: an append-only INNER
join and the windowed fire build typed Arrow columns directly instead of a
name-keyed row per emission, enabled by the planner only where a consumer can
ingest columnar. Gated nexmark q12: sustained slope 1.08M -> 1.83M rec/s (+69%),
CPU 113s -> 54s.

**Performance.** Scratch keys and transparent state probes removed the per-record
key-string build from the window, session window and aggregate operators (q12 row
path -31%) and from the equi-join (-30%), and a pre-sorted bulk join-output build
took the join to -54% cumulative. Born-columnar emission adds -11% CPU on a join
under a filter.

**State.** New opt-in ForSt backend (`CLINK_WITH_FORST=ON`): `forst://`,
`changelog+forst://`, `s3+forst://`, `s3sst+forst://` for live remote data files,
with an SST cache, cross-machine restore and a staging sweep. Deferred-read mode
puts hot-path state in the engine.

**Correctness fixes.** An input is now closed only when closed AND drained,
closing an end-of-stream data-loss race, and a worker registration is installed
before it is acked.

**Build and packaging.** `find_package(clink)` works on a real host; prebuilt
pinned-toolchain archives give the bootstrap a fast path; `zstd` exports as an
absolute path; new `CLINK_HTTP_TLS` and `CLINK_ARROW_OBJECT_STORES` knobs (both
default to the previous behaviour) let a portable artifact drop dependencies it
cannot vendor. CI gained a gate for the Go operator module, which nothing had
compiled before.

## v0.1.0 (July 2026)

Initial public release: the engine as described in the README.

Naming, settled for 1.0: the cluster roles are the **coordinator** (control
plane) and **workers** (subtask hosts), `clink_node --role=coordinator|worker`;
the fluent API entry point is `clink::api::Pipeline`; the in-process test
cluster is `clink::test::TestCluster`. Domain vocabulary (watermarks, windows,
checkpoints, savepoints, keyed state, key groups, slots) is unchanged.

In brief:

- Typed operator DAG and fluent API on a local runtime and a distributed
  Coordinator/Worker runtime (TLS/mTLS, HA, HTTP API + dashboard).
- Event time end to end: watermarks, tumbling/sliding/session windows,
  interval joins, CEP.
- Keyed and broadcast state over in-memory, file-backed, RocksDB, and
  changelog backends; rescale, schema evolution, savepoints.
- Exactly-once checkpointing with true 2PC sinks (file, Kafka, Parquet,
  S3, Postgres) and an effectively-once upsert family.
- Arrow-native columnar wire format and columnar operator fast paths.
- SQL frontend: embedded (`clink run`, libclink C ABI, pyclink, Flight
  SQL) or submitted to a cluster.
- Deterministic incident replay: flight recorder, `state-diff`,
  `replay --verify`, frozen regression bundles.
- State as data: snapshots are Arrow IPC; export to Parquet/Iceberg;
  queryable live state.
- Connector suite across messaging, object storage, table formats,
  databases, and HTTP endpoints (see `docs/connectors/`).
- Public testing framework (`clink::test`), Kubernetes Helm chart and
  operator, reproducible benchmark harnesses.
