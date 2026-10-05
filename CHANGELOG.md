# Changelog

## Unreleased

**Numbers in a Kafka or WebSocket JSON table compute the same on the columnar
and row paths.** The columnar JSON decode, and joins and windows emitting
born-columnar output, took a numeral with a decimal point or an exponent but an
integral value, such as `7.0` or `5e0`, into a BIGINT or INTEGER column as an
integer, while the row decode keeps it a double; arithmetic follows the value's
kind, so `n / 2` gave 3 on the columnar path where the row path gives 3.5. A
BIGINT or INTEGER column now decodes columnar only for integer tokens in its
range, and anything else takes the row decode. The opposite held for DOUBLE: the
row decode kept an integer token in a DOUBLE column as an integer, so `x / 2` on
a value written as `3` gave 1 with `columnar_decode='false'`; a declared DOUBLE
column now holds a double on both decodes (an integer past 2^53 prints as the
nearest double), and born-columnar output falls back to rows for a DOUBLE or
REAL value held as an integer. The columnar decode also refuses an integer
token outside 64 bits, which the row decode drops, instead of handing the record
on, and no longer reads `-9223372036854775809` into a BIGINT as its minimum.
Streams whose integers arrive as integer tokens, nexmark's included, stay
columnar.

**TIMESTAMP and TIMESTAMPTZ columns ride the columnar path as Arrow millisecond
timestamps.** Within a deployment admitted to the second Row layout (embedded,
or a cluster whose workers all run protocol v3), the columnar JSON decode of a
Kafka table (JSON or a Schema Registry format) or a WebSocket JSON table, and
the born-columnar output of joins and windows, carry integer epoch-millisecond
values as Arrow timestamps, so a Kafka or WebSocket JSON table projected into
the native ClickHouse sink builds no Row on the way; a filter or computed
expression on a TIMESTAMP column still builds rows at that operator. A batch
whose timestamps are ISO or digit text, or numerals with a decimal point such
as `1700000000500.0`, takes the row decode, and joins and windows whose
TIMESTAMP values are text or JSON doubles emit rows, with the same results; a
join switches to rows for the rest of its life on the first such value. On a
cluster that still has an older worker, timestamps ride as text, as before. An
embedded job interrupted under v0.10.0 still resumes: its resume fingerprint
reads the new timestamp codes as the text codes they replaced.

**A listener is never closed under a thread that may be accepting on it.**
The coordinator, the data-plane receiver and the HTTP server woke their accept
thread by closing its listening socket. On macOS a close racing the thread's
entry into `accept()` could wedge both until a connection arrived, which hung
`Coordinator::stop()` under load (the occasional `ShutdownLeaks` timeouts); on
Linux the closed descriptor could be reissued to another listener in the same
process before the thread reached it. Accept threads now wait in `poll()` and
are woken, joined, and only then is the listener closed: the wake is a listener
shutdown on Linux and a self-pipe on macOS. A receiver no longer keeps a peer
it accepted as teardown began, and a failed accept on its listener that is not
a shutdown wake now fails the task with `CLINK_NETWORK_CHANNEL_ACCEPT_FAILED`
instead of ending its input cleanly. `Coordinator::set_accept_factory`
(Evolving): the factory is now called only once a connection is pending, with
a non-blocking listener; one that calls `::accept()` itself must handle EAGAIN
and, on macOS and the BSDs, clear `O_NONBLOCK` on the accepted socket before a
blocking handshake, or use `NetworkSocket::accept_one`, which does both.

**The HTTP server and client work with descriptors numbered 1024 or above.**
httplib waited on sockets with `select()`, so on a long-running node with a
raised `RLIMIT_NOFILE` any descriptor at or above `FD_SETSIZE` made the server
answer 500, the client fail to read its response, and the listen loop fall
back into a blocking `accept()`. It is now built with `CPPHTTPLIB_USE_POLL`.
On macOS `HttpServer::stop()` can take up to 20 ms, because `close()` does not
wake `poll()` there.

**Cluster protocol version 3 admits the Row sidecar's millisecond timestamp
layout per deployment.** A deployment runs the second layout only when it has
a layout-bearing operator (the columnar Kafka JSON decode, a window or a join)
and every worker hosting it registered at protocol v3 or later, decided again
at every submit, restart, replan rescale and HA takeover; a job with none stays
on the first layout and a rolling upgrade costs it nothing. A hot rescale of a
job running the new layout places its new subtasks only on v3 workers, and
falls back to the stop-and-replan only when they need a pre-v3 worker's free
slots. A coordinator from an earlier release never admits it, and a downgrade
silently runs the first layout. Nothing writes the new layout yet. Job modules
(`CLINK_REGISTER_JOB`) built against an earlier release must be rebuilt: the
plugin ABI fingerprint changed.

**The Row sidecar readers accept millisecond timestamp columns ahead of any
producer writing them.** The row layout gains `ts_ms` and `tstz_ms` codes,
read only where a reader admits them and otherwise taken as the text they are
today; nothing writes them yet, so no frame, file, spec or registered schema
changes. `read_cell`, the row reader and the native ClickHouse sink's
columnar intake accept a `timestamp(ms[, tz])` column, and `read_cell` now
refuses an array it cannot read, including a timestamp of another unit,
instead of reading it as a string array or its ticks as epoch milliseconds.
Job modules built against an earlier release must be rebuilt.

**C++ jobs can write a CLINK_FIELDS struct to ClickHouse over the native
protocol.** `make_clickhouse_native_sink<T>` builds the native sink for a
Dag-direct job and `register_clickhouse_native_sink<T>` registers it as the
factory `clickhouse_native_sink_<channel>` for a job module, both in the
Internal header `clink/clickhouse/native_sink.hpp`. The sink takes its column
types from the struct's Arrow schema, so there is no `sql_column_types` and no
encoder to write; unsigned fields land in unsigned columns, with a per-value
range check into a narrower one, timestamps of any unit scale exactly, and
`std::optional` fields land in Nullable ones. Options, refusals, the
at-least-once guarantee and its conditions are the SQL native sink's. The open
report labels a typed sink's columns `arrow=` with the Arrow type where a SQL
sink's show `sql=`, and its options line says `columns=<n> from the batcher
schema`. Consumer example 12 writes a struct end to end, built with
`-DCLINK_EXAMPLES_CLICKHOUSE=ON`.

**`find_package(clink COMPONENTS clickhouse)` now resolves the client's own
dependencies.** The installed package's `clickhouse` component finds the
OpenSSL and abseil the installed ClickHouse client links, so a consumer
project can link `clink::clickhouse` from an install prefix; before, the
exported target named imported targets nothing had defined.

**A multi-input operator or sink no longer holds event time back, or hangs,
when an input closes.** A fan-in sink and an operator fed by several subtasks
now forward the watermark an input's close frees as soon as they see the close,
while another input is still open, as the other multi-input runners already
did; with the remaining inputs idle, time used to stay at the closed input's
last watermark. Every multi-input runner now also sees a close on an input
paused at a barrier: the fan-in sink, the N-input operator, the interval join,
broadcast connect and the co-operator used to miss it, so a later barrier from
another input could never align and the runner never exited. A drained input
(a drain marker and then a close, at a rescale or cutover) now counts as a
handoff rather than an end: once every input has gone, the union, interval
join, broadcast connect and co-operator no longer move the watermark to end of
time, so event-time timers do not all fire at the handoff; open windows still
fire through `flush()` when the runner exits. On their last input's close, the
fan-in sink and the N-input operator leave end of stream to `flush()`, as the
single-input runner does.

**The Linux wheels carry the ClickHouse connector.** The Linux pyclink wheels
(manylinux_2_28, x86_64 and aarch64) carry the ClickHouse connector, including
the native sink with TLS, built against the same static OpenSSL as the Kafka
client. With `secure='true'` and no CA named, a wheel trusts `SSL_CERT_FILE` or
`SSL_CERT_DIR`, or, where its OpenSSL's own default locations are missing
(AlmaLinux 8, or Debian and Ubuntu without the `openssl` package), the
distribution's CA bundle. The wheels workflow runs a live TLS round trip on
both architectures, including inside `almalinux:8` and `debian:bookworm-slim`
with no CA configured, and a tag publishes no wheel unless they pass. The wheel
gate now also fails on a dynamic export outside `clink_*`, on more than one
OpenSSL, and on lz4 taken from more than one archive or from the ClickHouse
client's.

**A windowed aggregate in front of a blackhole or native ClickHouse sink emits
columnar output.** The planner now counts the column binding in front of every
SQL sink as a columnar consumer when every sink it feeds takes a columnar batch
without building rows, which `blackhole` and the native ClickHouse sink
(`insert_format='native'`) do. A window whose SELECT is its own output, the
shape nexmark q12 uses, therefore emits columnar batches into either sink; in
front of any other sink it stays in row form, and results are unchanged. Join
plans are unchanged: a SQL join always has a projection after it, which already
counted as a columnar consumer. Nexmark q12 blackhole figures from before this
change are not comparable. An embedded job interrupted under v0.10.0 still
resumes: its resume fingerprint leaves `columnar_output` out only on an
operator that feeds the binding, which v0.10.0 never promoted, so every
fingerprint v0.10.0 recorded still matches.

**The native ClickHouse sink takes a columnar batch without building rows.** A
SQL pipeline whose Kafka JSON decode runs columnar, or whose join or window
emits columnar output, now stays columnar into `insert_format='native'`. The
sink converts the batch from its arrays, cell for cell by the same rules as the
rows it would turn into, so the landed values and any conversion failure are
unchanged. A batch whose event-time column is not int64, which carries a value
type the Row sidecar does not, or which carries a declared column's name twice,
takes the row path as before. New metrics
`clink_clickhouse_input_batches_total{carrier}` and
`clink_clickhouse_columnar_declined_total{reason}`; the close summary reports
`columnar_batches` and `row_batches`. Integer, float,
boolean, timestamp and date columns are taken without copying when the batch's
own builders made them; a column read from an Arrow IPC frame or imported
through the C Data Interface is still converted, and a chunk that shares a
column with its input stays charged to the memory budget until the task
thread frees it. Text columns into VARCHAR, and decimal columns into
a DECIMAL of the same precision and scale, are taken without copying too: a
text column when none of its values in the batch begins with the engine's
decimal marker byte (`\x01`), a decimal column when every value fits its
precision. A column that fails its condition is converted cell by cell for
that batch, so the landed values are unchanged. `CLINK_DISABLE_COLUMNAR=1` restores the
row path.

**The column binding in front of every SQL sink no longer turns a columnar
batch into rows.** `row_bind_columns`, which the planner puts in front of every
Row sink, materialised every columnar batch that reached it since 2dd2e23,
which also undid the blackhole sink's columnar path: nexmark q0 blackhole
figures taken since then include one materialisation per batch at the bind.
It now renames and selects the batch's columns without building rows, and a
sink that takes rows builds them itself, once per batch, as the bind did
before; output is unchanged. q12 is unaffected by this change, because since
2dd2e23 its window emits rows in front of the bind, so q12 blackhole figures
from before and after that commit already differ for that reason. A sink
column declared as `__source_partition` or `__row_kind` keeps the row path.

**A join or window's columnar output no longer changes values on the way to
the next operator.** A join or window that emitted columnar output carried
TIMESTAMP, SMALLINT and DATE values held as numbers, and ARRAY, MAP and ROW
values, on as text, and a REAL its source had not rounded to float precision on
as the rounded float, so a JSON sink or a later GROUP BY could see a string or a
changed number where the row path kept the original value. Such cells now send
the emission down the row path, and a join stays on the row path from then on;
the same types carried as text, and nulls, keep the columnar path. A window's
columnar fire also left its panes without an event time, where the row fire
stamps `window_end - 1`, so the exactly-once Kafka sink's replay suppression,
which matches on event time, passed a replayed pane through after a restore
(logging that it could not match it), and operators downstream saw no event
time; columnar panes now carry the same stamp. The columnar Kafka JSON decode
rounded an integer past 2^53 in a DOUBLE column, and in a BIGINT column on its
fallback parse, where the row decode kept it exact; such a batch now takes the
row decode. State restored from an affected job may hold groups keyed by the
text form, which do not merge with new groups keyed by the value. Job modules
built against an earlier release must be rebuilt.

**The native ClickHouse sink finds the system's trusted CAs where the OpenSSL
it was built with looks elsewhere, as the Kafka connector already does.** With
`secure='true'` and no `tls_ca_file` or `tls_ca_dir`, the sink trusts the
default CA locations of its OpenSSL. A static OpenSSL compiled for one layout,
as the Linux wheels link, can look where another distribution keeps no CAs.
When OpenSSL's default file and directory are both missing and neither
`SSL_CERT_FILE` nor `SSL_CERT_DIR` is set, the sink now also trusts the first
standard CA bundle or directory it finds, in the order the Kafka connector
probes them, and the open line names it (`tls=on (system CAs from <path>)`).
A named CA is still the only one trusted. The runtime image and host builds
find OpenSSL's defaults and are unchanged.

**A slow marker write no longer gets a live worker declared lost.** The
coordinator ran a worker's protocol handlers on the thread that read its
heartbeats, so the `COMPLETED` or `CONFIRMED` marker write for a checkpoint
that worker's frame completed held its heartbeats for as long as the store
took; on an object store slower than the heartbeat timeout the worker was
declared lost and the completed checkpoint became a restart, and the job's
completion records, written under the coordinator's lock, held every worker's
heartbeats at once. Each worker connection now has a reader that stamps
liveness and answers heartbeats, and a dispatch thread that runs the handlers
in the order the frames arrived. The dispatch backlog is capped
(`max_worker_dispatch_backlog_frames`, 10000, and
`max_worker_dispatch_backlog_bytes`, 64 MiB); a worker that exceeds either
loses its connection, as for a malformed frame. A frame from a worker declared
lost after the frame was read is dropped in its handler, so a
`SubtaskFinished` is never counted on top of the loss that already accounted
for its subtask. `stop()` no longer races a handler that starts the in-doubt
resolution thread while the coordinator shuts down.

**A worker that re-registers keeps its slots, its finished subtasks and the
jobs placed on its new session.** A re-registering worker's new session has
its heartbeats answered at once; the old session is retired on the new
session's dispatch thread after every frame it had already sent is handled, so
a subtask that reported a clean finish just before the re-registration is not
restarted as lost. Sessions are numbered and every placed subtask records the
session it went to: the retirement folds only what the old session held, a
slot is freed from the session it was charged to, a submit whose worker
re-registers between placing and deploying its tasks deploys on the new
session (or is refused retryably if that session has filled up), and a subtask
retried after the old session reported its error moves to the new session,
slot and all. A worker that registers again while an earlier session of it is
still being retired is refused retryably until the retirement ends, so
superseded sessions cannot pile up behind a slow store, and a new session lost
while still retiring its predecessor leaves the predecessor's subtasks to its
queued frames, including when the loss aborts a hot cutover. No free-slot count
wraps round when a session is charged past its capacity.

**The confirmed restore point never runs ahead of its marker or survives a
restart it does not belong to.** The in-memory confirmed restore point now
advances only once its `CONFIRMED` marker is durable, as the completed one
does, and never for a checkpoint of a run the job has since restarted from; a
checkpoint whose run was redeployed while its `COMPLETED` marker was written is
committed nowhere and no longer moves the new run's completed restore point,
and a marker landing while a restart is held for in-doubt resolution leaves
the walk's range where the hold fixed it, the walk starting from the confirmed
point the hold reported. The exactly-once specification takes each marker's
write and the advance as separate steps, so a restart, a takeover or a
superseded coordinator between them is model-checked, with a new invariant
that the in-memory confirmed restore point is a marker on disk (and a mutant
that breaks it). A superseded coordinator's `CoordComplete`, `WriteCompleted`
and `Broadcast` lines now carry its epoch, and trace validation reads its late
lines as the stale writes landing and its broadcast as a stutter.

**An HA takeover restores from, walks and reports one reading of the job's
markers.** A takeover reads the markers once, so the completed point it
reports is the one it restores from and walks to, and a takeover parked for
capacity deploys the restore point it decided rather than one read again after
a superseded coordinator's marker landed, and does not walk again when it
resumes. A restart of a taken-over or resumed job before its first new
checkpoint completes walks the completed-but-unconfirmed gap the takeover's
walk left open, instead of redeploying straight from the confirmed point.

**A restart waiting for slots after its in-doubt walk no longer walks again on
every watchdog tick, and fails at its capacity deadline.** The deadline is now
configurable as `Coordinator::Config::restart_capacity_timeout`, 180 s by
default; the restart used to wait for ever.

**A checkpoint no longer stalls when an input's close completes a barrier's
alignment.** In a union, join, broadcast, co-operator, fan-in sink or
operator fed by several subtasks, when the other inputs had delivered a
barrier and the last one finished without it, the aligner released the
barrier at the close and every runner discarded the release: the barrier
never left the operator, a fan-in sink never acknowledged it, and the
checkpoint could only time out, which without restart budget fails the job.
Each runner now forwards, snapshots or hands on the barrier as its barrier
path does, ahead of the watermark the close frees.

**A `parquet` source reads files another tool wrote.** The source expected
clink's own event-time column in every file and refused one without it, so a
directory pyarrow, DuckDB, Spark or ClickHouse wrote could not be read, though
the connector page reads such a directory in its first example. A missing
event-time column now reads as no event time for every row, in one file and
in a directory, on every filesystem the source reads. Any other declared column
the file lacks, or an `event_time` column of another type, is still refused,
naming it. A table of more than one column also reads a directory tree through
`prefix`, as the page's example does; only the single-column sources took
`prefix` before, and the example failed at deploy.

**A collect stream ends when the job writing it ends, and reports its
failure.** The stream ended when its sink subtasks had all closed, so a job
that failed before its sink opened left the reader waiting forever, and a
reader could see the end early, between one subtask closing and another
opening or across a restart. It now ends once every batch is read, no sink
subtask is open and every job writing the table has ended. A job that failed
ends the stream with its failure, after the rows it delivered, so pyclink's
`read_all()` raises instead of returning a short result as if it were
complete; a job the user cancelled still ends it normally.

**`LIMIT 0` is accepted.** It was refused as "LIMIT must be a non-negative
integer literal": the parser omits a field whose value is zero, and only
`OFFSET` read the empty value as 0. `LIMIT 0`, with or without `ORDER BY`,
now completes and writes nothing.

**A takeover no longer reuses the id of a checkpoint its dead leader had
begun, and a leader no longer shares ids with a superseded coordinator.** A
recovered job numbered its new checkpoints above the COMPLETED and CONFIRMED
markers and the snapshot files it could see. A checkpoint whose barrier had
reached a worker and whose capture had not landed left no file, and a worker
that outlived the coordinator could still write it after the takeover, at the
path the new run's checkpoint of the same id writes; a later restore could
then read one checkpoint from two vintages. CI's trace validation caught a
takeover reusing such an id. Every checkpoint id is now claimed in
`_jobs/<job>/TRIGGERED` in the checkpoint directory before it is allocated,
and a takeover numbers above the record as well. A claim lands only over a
record holding less, so each id is claimed once across every coordinator of
the job, and a coordinator whose claim is refused numbers above the record. A
superseded coordinator whose trigger loop runs on still reaches the workers
that have not yet re-registered with the new leader, and the two can no longer
send the same id. Each job claims its next id in the background as soon as the
previous one is allocated, on one claimer thread that stays parked between
claims, so the trigger loop never waits on the store and a slow or failing
record holds only its own job; a failing claim is retried after a delay that
doubles up to 2 s, and is reported once. The record names the claimant whose
claim put each id there (the id, then a `claimant=` line), so a claim whose
write landed and whose answer was lost (an ordinary S3 retry of a conditional
write that has landed is refused) is taken as claimed when retried, rather
than as another coordinator's. A job taken over before it completed a
checkpoint of its own, which used to number from 1 again, gets the same floor.
When an id cannot be claimed, the job's periodic checkpoints wait for it, a
savepoint fails with nothing sent, and a hot cutover is not begun, so the
replan takes the request. A source's end-of-input request whose id is not on
record yet is held and answered once the claim lands, never claimed for on the
worker's control connection, whose reader also reads that worker's heartbeats;
while the id cannot be recorded the request goes unanswered, its subtask
fails, and the restart replays the tail. A hot cutover's arm frames go out in
the same coordinator lock hold as the check that the cutover is still being
armed, so an abort's frames always follow them, and a cutover aborted before
that check sends none. The exactly-once specification's id rule now admits the
range of ids a takeover can choose and a renumber past a superseded
coordinator's claims, trace validation rejects a run in which a leader and a
superseded coordinator trigger the same id, and a worker fault point,
`worker.after_trigger_delivered`, holds the window open in `HaFailoverTest`.

**A takeover whose in-doubt resolution confirms the job's own checkpoints
restores from them.** The walk moved the restore point to the newest
checkpoint it proved, but left the directory to restore it from as it was: a
job with nothing of its own confirmed had none, and the deploy's lint refused
the restore, so the job was dropped at the takeover; a job submitted from a
savepoint kept that savepoint's directory. The takeover now restores the
resolved checkpoint from the job's own directory, and its `Redeploy` protocol
event reports that restore point rather than the one from before the walk, on
which the Kafka takeover traces diverged.

**A native ClickHouse sink.** A ClickHouse table with
`insert_format='native'` is written by `clickhouse_native_sink`, which sends
typed Native blocks over the native protocol from a writer thread of its own,
instead of JSON text. It is at-least-once: it refuses targets that take inserts
asynchronously, holds each checkpoint until every INSERT before it is
acknowledged, retries transient and in-doubt failures inside a ten-minute
window with deduplication tokens, and refuses at open anything it cannot
deliver (exactly-once, upsert, changelog, unaligned checkpoints, a second sink
on its chain). The text sink stays the default. The connector page has the
options, the type mapping and the guarantees.

**The text ClickHouse sink is stricter, and says so at deploy.**
`clickhouse_sink` changes in ways a job can notice:
- `format='json'` now selects JSONEachRow; it used to write TSV. Values are
  compared without case, and an unknown value still sends TSV but logs a
  warning at open.
- Every INSERT sends `async_insert=0`, `wait_for_async_insert=1` and its own
  `insert_deduplication_token`, fresh for each batch. Two identical batches no
  longer deduplicate against each other on a replicated table; a batch
  replayed after a restart now lands again where content hashing used to
  absorb it, which at-least-once allows.
- Database and table names are quoted.
- New keys `connect_timeout_ms` (5000), `send_timeout_ms` (30000) and
  `receive_timeout_ms` (30000), each 1 to 600000. A silent server could
  previously hold a flush, and the checkpoint, indefinitely.
- `port` (1 to 65535), `batch_rows` and `batch_interval_ms` (positive
  integers) are parsed strictly and refused at deploy when malformed. Garbage
  used to fall back to the default, `batch_rows=0` flushed on every row, and
  `port=70000` connected to 4464.
- `records_out` and `bytes_out` count rows only once the server has
  acknowledged them, and the barrier and close flushes record latency and
  errors like the others.

`ClickHouseSink::Options` gains the three timeouts and the class gains two
protected virtual hooks, so a plugin built against the old header must be
rebuilt, as for any release.

**`insert_format` selects the ClickHouse sink, and only there.** A
ClickHouse sink table may set `insert_format='native'` or
`'jsoneachrow'` (the default, today's sink). The value is a closed set, so
a misspelling is refused at `CREATE TABLE`, and so is the key on any other
connector, where it would otherwise have been ignored without a word.

**A sink can declare that its barrier gates the checkpoint ack.**
`Sink::gates_checkpoint_ack()` marks a sink that writes its whole interval out
inside `on_barrier`, whose at-least-once guarantee rests on the ack following
that hook. That ordering holds only while the sink owns its chain's
checkpoint, and a second sink on the chain takes ownership away, so
`Dag::add_sink` now refuses a chain that holds such a sink beside any other
sink, in either order. No existing sink declares it, so no existing job is
affected.

**Labelled histograms render valid Prometheus exposition.** A histogram whose
name carries labels (`clink_connector_commit_latency_ns{connector=...}`) was
rendered with the suffix after the label set, `name{...}_bucket{le=...}`,
which no scraper accepts, so those series were dropped. The suffixes now go on
the base name and `le` joins the label set.

**TIMESTAMP(0) and scale-0 DECIMAL columns can be declared.** A zero type
modifier was refused at `CREATE TABLE` with "typmod expected integer", so
`TIMESTAMP(0)`, `TIMESTAMPTZ(0)` and `DECIMAL(10, 0)` could not be declared
at all. They now map to a timestamp in seconds and a scale-0 decimal.

**A REAL or DECIMAL cell its type cannot hold is null in the Row carriers.**
A finite number beyond float's range was written into a REAL column as
infinity, and a DECIMAL value with more digits than its declared precision
built an array that fails Arrow validation. Both are now null, the rule the
carriers already applied to integers, on the wire, in Parquet and in Iceberg.
An integer read as a decimal is now exact past 2^53, and a double outside
int64 no longer reaches an undefined cast on the way.

**Integers past 2^53 survive the Row columnar carriers.** The Row batcher
read every integer cell through a double, so a BIGINT above 2^53 lost its low
bits on the wire, in Parquet and Iceberg, and a value outside int64 or int32
was undefined behaviour in the cast. Integer cells are now read exactly, and a
value the declared type cannot hold is null. A number rendered into a VARCHAR
column is now written the way the JSON serializer writes it: an integer
exactly, a double in the shortest form that reads back to the same value
(`1e-07`, where it used to keep six decimals and print `0.000000`).

**Operators can see a cancel.** `RuntimeContext::cancel_requested()` is true
once the task is being torn down: a CancelJob, the loss of the control
session, the worker stopping, a final-checkpoint decline, or another operator
of the executor failing. `cancel_signal()` hands out a copyable view that a
thread an operator owns may keep after the context is gone. It is separate
from `stop_requested()`, the stop-with-savepoint signal. A sink that blocks,
retrying a write, polls it to give up promptly instead of holding a restart's
drain.

**The ClickHouse client is pinned and built in Release.** clickhouse-cpp is now
`2.6.2` (`scripts/versions.env`, checksum-pinned), built static with TLS by
`scripts/build-clickhouse-cpp.sh`, into `CLINK_DEPS_PREFIX/clickhouse-cpp` on
the host and into `/usr/local` in the image. The image used to build `2.5.1` shared, without
TLS, and in Debug, because it followed the toolchain's build type; the
connector now takes the pinned build first and the configure log names the
version it took. A new live suite, the server pins
(`impls/clickhouse/tests/test_clickhouse_pins_live.cpp`, run by
`scripts/clickhouse-pins.sh` and the `clickhouse-pins` CI job), checks what the
coming native sink takes as given about the server on 26.3 and 26.8, pinned by
digest: insert-text SETTINGS, token deduplication, block squashing, the
effective `async_insert` from the table, the shard and the server default,
type strictness, FINAL and bounded metadata reads.

**A cancel that races end of input no longer publishes the tail twice.** At
the end of a bounded input a source asks the coordinator for one final
checkpoint, and every way of not getting one fell back to committing the tail
locally, with no checkpoint behind it. A cancel marks the job cancelling
before it broadcasts `CancelJob`, so a source that reached its end in between
was declined and committed its tail; the cancelled run left no `FINISHED`
marker, and a rerun on the same directory resumed from the last completed
checkpoint and published the tail again. The same fallback ran when the
request could not be sent or got no answer within 30 seconds. The reply now
says why it declined: only a job that takes no checkpoints still commits its
tail locally, a job that is stopping stops the source's task at once, and a
lost or unanswered request fails the subtask so the restart replays the tail
under a checkpoint. A sink whose task is already stopping now aborts a
terminal transaction rather than committing it.

**The S3 two-phase sink waits for a commit still in flight.** When a worker
dies in the middle of completing a multipart upload, its request can still be
running at the store when the recovering subtask completes the same upload,
which then answers `NoSuchUpload` or `InvalidPart` before the object exists.
The sink took that as a failed commit and restarted the job, spending restart
budget on every attempt. It now looks for the object for a few seconds before
calling the commit failed.

**An HA takeover before a job's first checkpoint keeps the job.** The new
leader restored each recovered job from its own latest completed checkpoint.
A job that had completed none yet got the id 0 with its checkpoint directory,
a pair the deploy lint refuses, so the recovery threw and the job was dropped;
an operator resubmitting it then lost the savepoint it had been started from.
A job with no checkpoint of its own now keeps the restore point it was
submitted with, or starts fresh when it had none, as a restart already did. A
recovery that still fails is logged at error level, says the job is not
running, and counts `clink_ha_recovery_failed_total`.

**A rescale no longer loses a completed checkpoint's records in an
exactly-once sink.** A two-phase-commit sink keeps each prepared
transaction's handle in operator state under the index of the subtask that
prepared it, and finalises at open whatever its predecessor left pending. Each
subtask looked only under its own index, so a rescale on `rocksdb://` or
`forst://`, which restore a subtask's assigned parents only, left every old
subtask's pending handle but the first uncommitted, and a scale-down on
`file://` those at or above the new parallelism: a checkpoint that had
completed, but whose commit had not run when the job stopped, lost its
records. The coordinator now names each new subtask's succession with the
parent mapping (every old subtask has exactly one successor: at a scale-down
the subtask that inherits it, at a scale-up the first of its children), the
deploy carries it, and `RuntimeContext::restore_succession()` exposes it. The
successor finalises each handle and every other restored copy is erased. This
covers the file, Parquet, object-store Parquet, S3 and Postgres two-phase
sinks. The Postgres sink's transaction ids now carry the topology generation
(`clink_<uid>_g<G>_sub<N>_<ckpt>`), so the subtask reconciling an old
subtask's orphans never rolls back a transaction the new run is preparing
under the same index; ids from earlier releases are still reconciled. The
WebHDFS Parquet two-phase sink is now a `CommittingSink`, and it and the
Iceberg sink keep their handles in operator state: as raw keyed rows a
rescale gave them to whichever subtask owned the key's first byte as a key
group, so they were lost there too. Handles an earlier release kept as raw
rows are still finalised. The Iceberg sink now also declares that it stages
state at the barrier, so a chain with a second such sink is refused as it is
for the others.

**The restart budget bounds a burst of failures, not a job's lifetime.** The
self-heal budget (`max_restarts_on_worker_loss`, 10 under the default) was a
lifetime count that nothing reset, so a long-running job failed at its
eleventh recovery however far apart its recoveries were, and once the budget
was spent every later failure was unrecoverable. A checkpoint that completes
`restart_budget_reset_after` (default 10 minutes) after the job's last restart
now forgives the spent restarts; a crash loop never completes a checkpoint, so
it still exhausts the budget. Deliberate rescales no longer spend it. The job
history still reports the lifetime restart count.

**A failed checkpoint no longer loses its interval when the restart budget is
spent.** A checkpoint that fails (a subtask cannot snapshot) aborts every
exactly-once sink's staged transaction for that interval, and only a rewind
re-emits it. With restart budget left the job rewinds. With none left it used
to carry on: the checkpoint above the failed one completed and committed,
every later one built on it, and the interval's output was lost with every
gate green. The budget is spent by every recovery a job makes, so a
long-running job reached this sooner or later. It now fails, naming the
checkpoint and the cause, with nothing completed above the failure, so a
restore from its last completed checkpoint re-emits the interval. This
affects every barrier-sealed exactly-once sink: Kafka, the file, Parquet, S3
and Postgres two-phase sinks, Iceberg and WebHDFS. The exactly-once
specification now models the restart budget (`MaxJobRestarts`), with a new
model in which it is spent and a `sail_on_without_budget` mutant TLC
refutes.

**The plain Parquet sinks keep their output across a restore.** A Parquet
file cannot be appended to, and its footer is written only when it is
closed, so the plain `parquet` sinks, which wrote one file per subtask, left
nothing readable when their process was killed. A restored run, from a
cluster failover or a resumed `clink run`, started that file again from
empty and lost every row written before the restore point. The sinks now
write a directory of complete part files instead, one per subtask per
checkpoint interval (`ParquetRollingSink`), on the local disk and on S3,
GCS, Azure and WebHDFS.

- A part is closed at every checkpoint barrier and at the end of input, so
  a job's output is readable while it runs, and everything up to the last
  barrier survives a kill.
- A part appears under its `.parquet` name only when it is complete, by a
  route chosen per store. S3 streams to the final key, since an unfinished
  multipart upload is never visible. The local disk and WebHDFS write
  `<name>.inprogress` and rename it; on the local disk the part and the
  directory are fsynced first, so a part is on stable storage before the
  checkpoint that relies on it completes (unless `CLINK_STATE_FSYNC=0`).
  Azure and GCS write `<name>.inprogress` and copy it into place: an Azure
  stream creates its blob as soon as it opens, and a GCS stream cannot
  abort, so streaming to the final key would publish an empty or partial
  part. A kill leaves at most an `.inprogress` object, which readers skip
  and the next run removes.
- A restored run keeps every part, including parts past the restore point,
  whose rows the sources then replay: at-least-once, as before. A run that
  starts from empty state replaces the previous run's parts, touching only
  files named exactly as the sink names its parts. A restore is recognised
  whether or not the job has a checkpoint directory, so a savepoint restore
  into a job that takes no checkpoints keeps the earlier parts too.
- A `parquet` source whose `path` is a directory reads every part in it, and
  resolves columns by name as it does for one file: a table declaring fewer
  columns, or the same columns in another order, reads the parts, and a
  query reads only the columns it uses. A WebHDFS source `path` naming a
  directory is read the same way. A directory holding only an exactly-once
  sink's `staging/` and `committed/` is refused with a pointer to
  `committed/`, rather than read as empty.
- The typed `clink::s3::parquet_sink<T>` helper writes part objects under
  `key` too; with a `bucket_assigner` it still writes one object per
  assigned key, finished only at close.

**Exactly-once WebHDFS Parquet tables deploy.** SQL plans
`delivery_guarantee='exactly_once'` on a `webhdfs_parquet` table to
`webhdfs_parquet_2pc_string_sink`, which nothing registered, so such a job
failed at deploy. The WebHDFS module now registers
`webhdfs_parquet_2pc_{int64,string}_sink`, and each object-store module's
registration test checks the names the planner emits.

**Compatibility.**

- A plain Parquet sink's `path` (or, on S3, GCS and Azure, its `prefix`,
  with `key` still accepted) now names a directory, not a file. Read it
  back with a `parquet` table on the same `path`, with the object-store
  sources' `prefix`, or with any dataset reader (`pyarrow.dataset`,
  DuckDB's `read_parquet('<dir>/*.parquet')`). On S3, GCS and Azure, a
  table that is both written and read (a materialized view's backing, for
  example) must use `prefix`: a source's `key` still reads one object.
- A sink whose path still holds the single file an earlier release wrote
  refuses to start and names the file; move or delete it.
- The sink lists and deletes under its path, so its credentials need list
  and delete permission as well as write (on Azure, a SAS with `sp=rwdl`).
- The programmatic single-file sink classes (`ParquetSink<T>` and the
  object-store equivalents) are unchanged. `MultiObjectParquetSource<T>`
  now reads a file whose columns are a superset of the batcher's, by name,
  where it refused one before. The plain sinks no longer read the WebHDFS
  `overwrite` option.
- `RuntimeContext` gains `set_restore_from_checkpoint_id`, which changes a
  header on the plugin ABI surface: rebuild job modules against this
  release.

## v0.10.0 (September 2026)

An embedded run now resumes from its own checkpoints after a crash, and
file sinks keep their earlier output across a restore. The plugin ABI
fingerprint changes, so job modules must be rebuilt; see Compatibility
below.

**An embedded run resumes after a crash.** With `--checkpoint-dir`, running
the same script again after `clink run` (or a pyclink `Engine`) was killed,
crashed, failed or was stopped with Ctrl-C continues the job from its last
checkpoint. Before, the rerun started from nothing: it re-read its sources
from the beginning and re-published everything its exactly-once sinks had
already committed. Against a real broker, a run SIGKILLed mid-stream and
started again committed every window to a transactional Kafka sink once,
where before the rerun committed the first run's windows a second time. The
restore point is chosen as HA recovery chooses it, including in-doubt
resolution, and new checkpoints number above every id already on disk.

- A job that reached the end of its input cleanly starts over on a rerun
  instead, so a bounded load or a full-refresh materialized view runs again
  rather than publishing nothing. So does a run that died before completing
  a checkpoint of its own: it is never handed the run before it.
- A directory whose checkpoints a different job graph wrote is refused, with
  both fingerprints named, because job ids restart at 1 in every process.
- `--fresh` (pyclink `Engine(fresh=True)`, C `clink_engine_options.fresh`)
  starts from empty state.

**Restored file sinks keep their earlier output.** The plain `file` sink and
the partitioned file sinks truncated their files whenever they opened, a
restore included. After a failover, and now after a resumed rerun, the file
held only the rows written since the restore point. They now append on a
restore (at-least-once, as documented), and an overwrite sink keeps its
staging file. The plain Parquet sink cannot append and still keeps only the
rows written since a restore. The exactly-once Parquet sink is the path that
keeps them.

Only the embedded engine tracks and resumes its runs; a cluster submit neither resumes nor writes these records.

**Compatibility.** `CheckpointConfig` and the coordinator's deploy path
changed on the plugin ABI surface, so the plugin ABI fingerprint rotates:
rebuild job modules against this release. `clink_engine_options` gains
`fresh` at its end (append-only; `struct_size` covers older callers). A
checkpoint directory written by an earlier release has no graph fingerprint,
so resuming from it is refused. Start such a job with `--fresh` or use a new
directory.

## v0.9.1 (September 2026)

pyclink on PyPI, with Linux wheels that include the Kafka connector, and a
SQL correctness fix: a single-instance stage's changes could reach a parallel
downstream stage out of order. There are no state-format or API changes, and
the plugin ABI fingerprint is unchanged, but some SQL jobs now plan their
downstream stages at one instance, so a v0.9.0 savepoint of such a job taken
above parallelism 1 is refused at submit (see the first entry).

**A single-instance SQL stage no longer feeds its changes to a parallel stage
out of order.** Scalar subqueries in `SELECT`, null-aware `IN`/`NOT IN`,
global aggregates, `LIMIT` and top-N run as one instance at any parallelism,
but the stages after them (the sink binding, and in a real job the sink
itself) were fanned back out with no key on the edge between. A row's insert
and its later retraction then took different subtasks and could reach the
sink in either order; a retraction that overtook its insert deleted nothing,
and the row survived. The visible case was a null-aware `NOT IN` returning a
row its NULL-bearing subquery had already made UNKNOWN, intermittently, in
`SqlRuntime.GlobalSubqueryAndNullAwareSemanticsAreParallelismInvariant`: the
captured changelog showed `delete (3,5)` written before `insert (3,5)`. The
planner now marks every stage that reads a single-instance stage without a
keyed exchange as single-instance too, down to the sink, so the existing
parallelism, validation and rescale guards keep it at one; a keyed downstream
stage keeps its fan-out, because a hash exchange sends each key's changes down
one path. That test passed 400 of 400 runs under the CPU load that reproduced
the failure, and now prints the raw changelog when its `NOT IN` result is wrong.
Upgrading: such a job's downstream stages change subtask layout, so its v0.9.0
savepoint taken above parallelism 1 is refused by the restore layout check
rather than restored into the wrong subtasks; resubmit it without the
savepoint.

**libclink links Arrow statically on Linux, so a manylinux wheel is
self-contained.** A new `CLINK_STATIC_ARROW` option makes `clink_core` link
Arrow, Parquet and the compute kernels statically, and `clink_shared` links
with `--as-needed`; `scripts/build-libclink-wheel.sh` turns it on for Linux.
Without it, GNU ld let the shared Arrow on `clink_core`'s interface resolve the
symbols later members needed, so a Linux libclink loaded `libarrow.so` beside
its static copy, auditwheel vendored about 43 MB of Arrow into the wheel, and
the vendored Arrow's `libatomic.so.1` stopped it loading on slim images. With
the option, a libclink built on `manylinux_2_28` with the Kafka connector needs
only glibc, libstdc++ and libgcc_s; its wheel tags `manylinux_2_28_aarch64`,
vendors nothing, and passed its smoke test and a live Kafka round trip on
AlmaLinux 8, Debian bookworm-slim and Ubuntu 22.04. The option refuses to
configure alongside tests or examples, whose job modules would each carry their
own Arrow. `scripts/build-arrow.sh` now also refuses OpenSSL on Linux whenever
the object stores are off, as it already did on macOS.

**Linux wheels with the Kafka connector, built on every tag.** The wheels
workflow now builds x86_64 and aarch64 wheels inside
`quay.io/pypa/manylinux_2_28_<arch>` on each architecture's native runner,
replacing the opt-in build on the Debian toolchain image whose glibc gave the
wheels a floor too high to install on mainstream distributions. The build is
one script, `scripts/build-manylinux-wheel.sh`, so it runs the same way
locally: the pinned Arrow with the object stores off, the pinned librdkafka as
static archives (`scripts/build-librdkafka.sh`, a new `LIBRDKAFKA_VERSION`
pin), libclink with SQL and Kafka only (`CLINK_WHEEL_KAFKA=1`), auditwheel,
and a new gate, `scripts/check-wheel-self-contained.py`, that fails the job if
anything was vendored or libclink needs more than the C and C++ runtime. A
second job installs each wheel on a stock Ubuntu 22.04 runner and runs the
smoke test there, then round-trips Kafka through the wheel against a
Redpanda broker on the runner, observed from outside by rpk. A release tag
then publishes all three wheels to PyPI as `pyclink` through trusted
publishing (the `pypi` environment's OIDC identity, no stored token), after
checking that each carries the tag's version.

## v0.9.0 (September 2026)

The exactly-once protocol under a machine-checked specification, Schema
Registry formats on the Kafka connector, opt-in memory budgets with SQL
state that spills, and the surfaces the 1.x line will hold stable. Upgrading
from v0.8.0: compiled job modules must be rebuilt, C callers of the embedded
ABI recompile against version 2, and a savepoint whose subtask layout the
new plan no longer matches is refused rather than restored; see the release
notes for which SQL jobs that affects.

**SQL job parallelism preserves global query semantics.** Scalar subqueries in
the SELECT list and null-aware `IN` / `NOT IN` joins now retain one global
operator instance, matching existing global aggregates, windows, ranking and
LIMIT stages. Graph validation and coordinator rescale admission prevent an
explicit override or rescale from expanding these stages. Keyed operators keep
their configured parallelism.

**Batched model inference adapts to the execution memory budget.** Retained
`ML_PREDICT` input rows and list storage now share the operator budget. If the
next row is refused while a batch is buffered, clink submits the current batch
and retries that row in a fresh one. A single oversized row fails cleanly.
Feature copies, prediction results and provider-owned inference memory remain
outside this charge.

**More SQL operators support oversized active keys.** GROUP BY stores each
accumulator separately, and splits `COUNT(DISTINCT)` and retractable `MIN`/`MAX`
collections into individual values. Percentile aggregates keep ordered value
cells and find interpolation ranks with streaming scans. `STRING_AGG` uses ordered
multiplicity cells; `ARRAY_AGG` stores arrival-ordered values and scans for
distinct output without a resident index. Fixed windows use the same value cells
inside separately stored panes. Sessions use them too, moving cells to the merged
start when an event bridges sessions. OVER splits pending rows, frame/LAG history
and running accumulators; null-aware joins split exact-key probe buckets. Entry
checkpoints retain ordering, emitted flags and key-group colocation, and migrate
legacy whole-partition and per-accumulator state. Individual rows, value cells,
materialised results and opaque accumulators must still fit.

**Scalar-subquery main buffers are budgeted and spillable.** Uncorrelated scalar
filters and projections retain main-side rows until the scalar side settles. With
a budget and SQL spill directory, they now store those rows as individual scratch
entries and emit the final result one row at a time. Without spill, retained
rows count against the operator budget and fail cleanly on exhaustion. This keeps
the existing bounded end-of-input lifecycle and does not add checkpoint recovery.

**Oversized ranking, last-N and equi/interval join partitions use entry-level storage.**
With a budget and SQL spill directory configured, these operators scan individual
rows and checkpoint them separately, preserving partition colocation on rescale.
Legacy vector snapshots migrate on recovery. Individual rows and growing aggregate
accumulators still need to fit.

**Global top-N and null wildcard collections can spill beyond RAM.** Global
ORDER BY/LIMIT/OFFSET migrates candidates to a disk-backed heap and streams
ordered output with a constant number of decoded rows. Null-aware joins spill
and checkpoint wildcard tuples and null probes individually, retain arrival
order for retractions, and read the previous null-state checkpoint format.
Sibling join maps coordinate pressure relief. Whole keyed partitions and
individual rows still need to fit the configured limit. See
[Memory budgets](docs/internals/memory-management.md).

**SQL window, join, OVER and ranking partitions now spill on budget pressure.**
Fixed/session windows, equi/interval joins, OVER and last-N frames, partitioned
ranking and null-aware join exact-key maps share retained-state accounting and
local scratch spill. Checkpoint recovery preserves their state; interval joins
and null-aware joins now persist their synchronous working data. Each active keyed
partition must fit, and temporary allocations remain outside the limit. See
[Memory budgets](docs/internals/memory-management.md).

**SQL GROUP BY spills working groups under shared-budget pressure.** With
`CLINK_SQL_SPILL_DIR` configured, synchronous aggregate execution moves its
resident groups to private scratch files on exhaustion, then loads and writes
one group at a time. Existing aggregate/changelog codecs, checkpoint slots,
TTL expiry and queryable results are preserved. One group must still fit;
checkpointing large state still needs an appropriate backend. SQL TTL indexes
now charge the operator budget across aggregates, joins, DISTINCT and set
operators. See
[Memory budgets](docs/internals/memory-management.md).

**Blocking exchanges spill on shared-budget pressure.** Retained IPC payloads
and ordering metadata now share the execution/operator memory budget. With a
spill directory configured, a refused payload charge migrates the resident
prefix to disk before the incoming batch, preserving data and control order.
Replay releases retained charges. Without spill, or when ordering metadata
exhausts the budget, the execution fails cleanly. IPC scratch space and decoded
batches remain outside this accounting.

**Opt-in memory budgets share an allowance across covered state, local queues
and checkpoint work.** `JobConfig` accepts a shared domain or byte limit, with
optional operator child limits; `CLINK_EXECUTION_MEMORY_LIMIT_BYTES` supplies a
per-local-execution default for SQL and Workers. The first increment accounts
SQL GROUP BY and tumble/hop/cumulate working state, memory/file backend storage,
queued row and Arrow batches, canonical snapshot allocations and queued captures.
Refusal names the budget and category, and local operator failure closes every
edge to wake blocked peers. Usage, peak and category gauges expose the account.
Coverage is explicit and partial, not an RSS cap or automatic spilling. The new
memory budget and Arrow pool authoring headers are Evolving; public and plugin
include manifests record their reach. See [Memory budgets](docs/internals/memory-management.md).

**The engine's protocol traces are validated against the exactly-once
specification.** Design record 012's increments 3 and 4. With
`CLINK_PROTOCOL_TRACE_DIR` set, every process appends one JSON line per
protocol event (trigger, barrier delivery, prepare, ack, completion, marker,
broadcast, commit delivery and execution, receipt, confirmation, worker
loss, drain, restart, recovery, each in-doubt walk step, sink open, task
placement) to its own file; off, each site is one relaxed atomic load. The
vocabulary (`formal/trace/events.txt`) is a contract with the model:
`scripts/check-protocol-trace-events.py` (CI and pre-commit) fails when an
emitted event, the manifest and the trace module disagree, or when a
specification action is neither reached by an event nor listed as
unobserved. `formal/trace/TraceExactlyOnce.tla` follows a recorded trace
through `ExactlyOnce.tla`, with the constants read off the trace and the
unobservable actions allowed as hidden steps within the fault budgets the
trace implies; `scripts/formal-check.sh --trace` merges a run's files, runs
TLC and names the first event no allowed step produces. The multi-process
harness traces every node it spawns and keeps the run under
`CLINK_PROTOCOL_TRACE_OUT`; the build job uploads what its tests left and a
`trace-validation` job model-checks each one, while the `formal` job
validates the recorded set under `formal/traces/`: a checkpointed run of
the recoverable family and three Kafka runs (a source-worker kill, a kill in
the receipt window, a coordinator failover), every one a behaviour the
specification allows. Writing the module against those traces found three
places where the specification was narrower than the engine, each fixed in
it: a recovered coordinator's id floor counts participant snapshots on disk
as well as markers (`SnapshotIds`); the source's worker can die with no sink
beside it and still restart the job; and the first checkpoint after a
redeploy is triggered before a sink has reopened, the barrier waiting in its
input queue. None is an engine defect, and the last one made the model
stronger: with it TLC refutes the `broadcast_during_drain` mutant on its
own, in the shape of the campaign run that found the defect, where before it
was recorded as guarded by a later rule. The TLA+ tools pin moves from
1.7.4 to 1.8.0 (`formal/tools.env`), the release the pinned CommunityModules
jar is compiled against; the models and mutants check unchanged under it.
Documented under
[Trace validation](https://orhaugh.github.io/clink/internals/exactly-once-specification/#trace-validation).

**The Confluent Schema Registry wire format on the Kafka connector.** A Kafka
table can now declare `format='avro'`, `'protobuf'` or `'json-schema'` with a
`schema_registry_url` and read and write registry-framed values (magic byte,
schema id, encoded payload) against Confluent Schema Registry or any
registry speaking its REST API (Redpanda, Karapace, Apicurio). The new
`impls/schema_registry` library carries the registry client (basic or bearer
auth, TLS, path prefixes, id caching), the framing including Protobuf message
indexes, and the three value formats; the Kafka connector links it and offers
the formats on the source and on the plain, transactional and upsert sinks.
The engine's JSON path is untouched: a source decodes each message into one
JSON object text ahead of the existing `json_string_to_row_columnar` bridge,
a sink encodes `row_to_json_string`'s rows as the last step before the
producer, and the planner only keeps the Row channel and forwards the format.
Avro rides avro-cpp's generic API with the logical types mapped (decimals as
exact strings, dates and times as ISO text, every timestamp as epoch
milliseconds); Protobuf parses `.proto` text at runtime with the compiler
library, resolves registry references, and walks messages through reflection
so `int64` stays a JSON integer and field names stay as declared; JSON Schema
is the header. Sinks derive a schema from the declared columns and register
it under `<topic>-value` (idempotent; `schema_registry_auto_register='false'`
writes against the subject's own schema instead), fail at deploy rather than
on the first record when the registry refuses, and keep decimal digits
exact. `decode_error='fail'|'skip'` is the poison-message policy. Formats a
build lacks are refused by name; `clink --capabilities-json` lists the ones
compiled in. Tested against the reference implementations (payloads produced
by avro-cpp and libprotobuf's dynamic messages), through librdkafka's mock
broker, and live against Redpanda with its built-in registry. Documented on
[Schema Registry formats](https://orhaugh.github.io/clink/connectors/schema-registry/).

**An MCP server over the diagnostic surface.** `clink-mcp`
(`python/clink-mcp`, Python 3.10+, depends only on the MCP SDK) exposes
what the engine already ships for diagnosing a pipeline as tools any MCP
client can call: `checkpoint_verify`, `state_cat`, `state_diff`,
`state_query`, `capture_cat` (with a regex filter over the epoch dump),
`replay` (`op`, `verify`, `out`, `plugin`, `emit_test`), `replay_diff`,
`search_file`, `explain`, `lint`, `clink_capabilities`, and, given a
coordinator URL, `jobs`, `job`, `job_graph`, `job_operators`, `job_lineage`,
`cluster`, `health`, `logs`, `queryable_state_lookup` and
`queryable_state_scan`; plus a `diagnose_incident` prompt. It adds no engine
feature and is read-only by construction: nothing in it submits, cancels,
stops, rescales, savepoints or commits, and `replay` writes only to the paths
its caller names. The server absorbs two CLI ergonomics (a checkpoint root
where the CLI wants the generation directory; `op-<id>` names where it wants
the bare id) and returns exit code, stdout and stderr with every answer.
Tested end to end over stdio against a real embedded run
(`python/clink-mcp/tests`). The client-neutral walkthrough,
[Diagnosing a pipeline with an agent](https://orhaugh.github.io/clink/guides/diagnosing-a-pipeline-with-an-agent/),
takes a fat-fingered order from a wrong running total to the record, the
exact emission, a determinism check and a frozen regression bundle; its
generator is `examples/agent-diagnosis/make_incident.py`. The claim is
diagnosable by agents, not self-healing.

**The exactly-once protocol is a machine-checked specification.** Design
record 012 states the protocol that four parts of the engine implement
between them - barrier checkpoints with ack-after-durable snapshots, the
two-phase-commit sinks, the coordinator's COMPLETED and CONFIRMED markers,
and recovery with in-doubt resolution, receipts and unresolved markers - as
a TLA+ model under `formal/`, with exactly-once written down as invariants
over the positions each sink has published: no position twice, every
position below the vouched-for frontier exactly once, a restore that never
reads snapshots of mixed vintage. TLC model-checks it on every push
(`scripts/formal-check.sh`, the `formal` CI job, tools pinned by SHA-256 in
`formal/tools.env`) over bounded configurations that enumerate every
interleaving of protocol steps and faults within their bounds, and checks
the liveness property that a run with bounded faults settles with every
vouched-for position published once. Every defect the qualification
campaigns found and fixed is a named mutant of the specification: thirteen
of fifteen are refuted by TLC (twelve when this landed; the trace-validation
change below made the withheld broadcast's mutant refutable), and the two
that are not are recorded as rules a later rule now guards as well, so the
model is known to see what the rigs saw and the rules that are not
load-bearing on their own are named. The published
page is [Exactly-once specification](https://orhaugh.github.io/clink/internals/exactly-once-specification/).

**Three exactly-once defects found by the model, fixed before any rig paid
for them.** Each was an interleaving no gate had enumerated. The refusal
wall: in-doubt resolution stopped at the first checkpoint it refused, so a
commit that had executed without its receipt in a completed checkpoint
above it was never proven, the redeploy fenced it blind and the replay
published its interval twice - and the refused checkpoint stood as a wall
every later walk stopped at. The walk now leaves an `.unresolved` marker for
every unreceipted handle above its stop, and the sink's pre-fence describe
settles each one (`ResolutionFixture.ARefusalMarksTheUnreceiptedHandlesAboveIt`).
The rewind floor: a checkpoint above a FAILED one, already collecting acks
when the failure began its rewind, could complete during the drain; its
marker made it a restore point, resolution confirmed it, and the job
restored past the aborted interval below it. A checkpoint above the failed
id is now discarded like the failed one until the restart redeploys
(`CheckpointCompletion.ACheckpointAboveAFailedOneIsDiscardedDuringTheRewind`).
The restore point ahead of its marker: `latest_completed_checkpoint_id`
advanced under the lock at completion with the COMPLETED marker written
after it, and a restart deciding in that window redeployed from a
checkpoint the next coordinator could not see; the recoverable sinks had
already re-committed its handles at open. Memory now advances with the
durable write. None of the three needs a fault the campaigns do not already
inject; the two-hour QUAL-01 run did not happen to hit them.

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

**A compiled plugin's compatibility with a cluster is a declared surface,
checked completely.** Design record 010, closing the "stable extension model"
item on the road to 1.0. The plugin ABI gate hashed all 310 public headers,
so about a fifth of commits invalidated every deployed plugin binary over
changes a plugin could never observe, while the toolchain identity, sanitizer
instrumentation, the pinned Arrow version and layout-changing defines were
not material at all. The fingerprint now hashes exactly the declared surface:
the tracked manifest `scripts/plugin-abi-surface.txt`, generated by
`scripts/gen-plugin-abi-surface.py` as the include closure of the authoring
entry points plus the exception types that cross the boundary, kept fresh by
a `--check` in CI and the pre-commit hook, with every first-party plugin
source held to the declared set. The material folds in the options the
surface uses and the Arrow pin; a toolchain identity gates the stdlib,
dual-ABI and sanitizer mismatches the build system cannot hash; and a
per-header manifest baked into every plugin lets a refusal name the headers
that differ instead of printing two opaque hashes. Refusals also move
earlier: the submitter advertises each plugin's identity on the
references-only exchange and the coordinator refuses an incompatible plugin
before any bytes ship, returning its own manifest so the client names the
diff locally; a worker-side refusal (reachable only mid rolling upgrade) is
fatal rather than fed to the restart loop; and an HA-recovery skip over a
stale plugin is an error with a counter instead of a silent drop.
`clink_add_job_module()` replaces the seventeen hand-rolled module blocks and
ships with the CMake package, so out-of-tree jobs build on the recipe the
tree's fixtures are tested with. Driving a plugin job through the in-process
cluster exposed two pre-existing defects, fixed here: the per-subtask retry
path appended an attempt marker to `extra_config`, corrupting the planner's
chain-spec JSON so every retried generic subtask died parsing its own chain
with the true cause masked; and generic subtasks are now excluded from
per-subtask retry, since a lone retry of a mid-chain subtask can only wedge
on peers that already finished. Documented under
[design record 010](https://orhaugh.github.io/clink/design/010-stable-extension-model/).

**One `CLINK_FIELDS` declaration describes a type everywhere, and a shape
fingerprint gates restores.** Design record 009, implemented. A described
struct needed a hand-written `Codec<T>` beside its field list, and that
codec's body was nothing but the field walk the description already stated.
`derived_codec<T>()` now folds the descriptors into the codec, matching the
hand-written idiom byte for byte (little-endian fixed width, u32-length
strings) so migrating an existing type is a deletion, not a format change.
The layout is a durability contract from day one: specified in the header,
pinned as golden bytes in the tests, frozen in
`tests/fixtures/derived-codec-v1.bin`; decode consumes the buffer exactly and
fails closed on malformed bytes. Measured on the serde bench: 49.2 ns per
record derived against 71.4 hand-written. Both registries gain argument-free
overloads, `TypeRegistry::register_typed<T>()` and
`PluginRegistry::register_type<T>()`, that use the declared type name, the
derived codec and the auto-selected batcher. `CLINK_FIELDS` is the primary
spelling (`CLINK_ARROW_FIELDS` stays as a deprecated alias), guarded by a
completeness assert: for an aggregate the field list must name every member,
because a forgotten field is silently absent from the wire, from every
snapshot and from every Parquet file; `CLINK_FIELDS_SUBSET` is the explicit
opt-out. The field metadata lives in the Arrow-free `clink/core/fields.hpp`.

The declaration also carries a shape fingerprint (a compile-time FNV-1a64
over the ordered field-name and wire-kind sequence, recursive into
composites), and `KeyedState`'s bind refuses a restored slot whose stored
fingerprint differs from the live type's, naming the slot, both fingerprints
and the remedy. A declared version bump's migration rewrites the slot's
values and clears its stamp, so the supported evolution path passes; an
absent stamp gates nothing. Fingerprints ride a new snapshot metadata key
beside the version map, an additive change the format contract permits.
Wiring the stamps through every backend found a defect: the `file://`
backend, the default local durable scheme, dropped both version and
fingerprint stamps, so a restore read the absent-stamp default of v1 and
re-ran already-applied migrations over current bytes. Fixed, and the
sharded, RocksDB, ForSt and remote-read backends (hot tier; pool mode
deferred, as before) now persist fingerprints too. Snapshots written before
this carry no stamps and keep the absent-stamp contract, so cycle a
savepoint after upgrading before bumping any schema version.

The pre-deploy twin of that gate: `expect_state_shape<V>(uid, slot)` records
the declared fingerprint in the job graph (an additive `JobGraphSpec`
field), `CLINK_REGISTER_JOB` emits an optional
`clink_job_check_restore_fingerprints` export, and the coordinator's submit
gate and HA recovery read both stamp maps from the savepoint they already
restore and refuse an undeclared shape change under an unchanged version,
naming the slot. An older job `.so` simply lacks the export and stays
version-checked only. `clink check-savepoint` prints the savepoint's
shape-fingerprint table beside the version table and, under `--expected`,
runs the same check, exiting 3 on a mismatch. Consumer example 11 walks the
whole surface. Documented under
[Declared types](https://orhaugh.github.io/clink/internals/derived-types/) and
[Fault tolerance, rescale and schema evolution](https://orhaugh.github.io/clink/internals/fault-tolerance-and-rescale/).

**A shard dying mid-checkpoint can no longer hang the sharded keyed stage.**
`ShardedKeyedStage::checkpoint()` broadcasts the barrier to every shard and
waits for each to deliver; a shard whose operator threw delivered a failure on
its own behalf and then closed its queue. In that order there was a window:
the dying shard's delivery ran before the round was active and so counted for
nothing, `checkpoint()` then activated the round and pushed the barrier into
the still-open queue, and the close stranded it with nobody left to deliver.
The checkpoint waited forever, which is the outcome the stage exists to
prevent, and `ShardedKeyedStage.WorkerDeathDoesNotHangCheckpoint` timed out
in CI once in a few hundred runs and every few runs on a laptop. The shard now
closes its queue first and delivers second, so a push after the close fails
and the coordinator delivers on the shard's behalf, and a push before it means
the round is active when the shard's own delivery lands; both deliveries stay
idempotent per shard per round. Repetition does not reach the losing schedule
on purpose (four thousand runs of the old order under load did not), so the
pin is a fault point, `sharded_stage.death_before_delivery`, between the
shard's close and its delivery: a new test parks the dying shard there,
broadcasts a barrier meanwhile, and requires `checkpoint()` to return while
the shard is still parked.

**The Redeploy protocol event is recorded once per restart.** The
coordinator emitted it from inside the loop that builds one deploy frame per
worker, so a restart that redeployed onto two workers recorded two identical
`Redeploy` events a few milliseconds apart. The specification takes that step
once (deploying to running), so the first run of the trace-validation job on
`main` reported the trace of every multi-worker restart, and every rescale
replan, as diverging at the second event: eleven of the fifty-eight runs the
build's tests left. The restore point is now decided once per restart, every
frame carries the same one, and the event is emitted once, after the frames
are built and only when at least one was. The engine's behaviour is
unchanged; only the recording and one duplicated log line are. Regenerated,
those traces walk past the restart and then diverge at two shapes the
specification does not yet allow, both open: a second worker lost while the
first redeploy's placements are still landing, and a sink preparing the
final checkpoint before the periodic one ahead of it has completed.

**The capability manifest no longer claims a SQL surface for API-only
connectors.** The `mqtt`, `mongo` and `generator` records declared
`available_in_sql` although the SQL planner binds no `connector='mqtt'`,
`'mongo'` or `'generator'`, so `clink --capabilities` promised what a
`CREATE TABLE` then refused, and the connector pages contradicted the binary.
The three records now say `sql: no`, and the manifest gate grows a SQL arm:
for every record that claims a SQL surface it compiles a probe statement per
declared direction through the planner (a variant record such as `kafka_2pc`
through its base name; `s3` and `http` through the longer vocabulary names
their impls also register), so a claim no `connector='...'` reaches fails the
test step instead of reaching a reader.

**Two worker-loss gates could pass without proving what they claim.** Both
fault-recovery tests that kill a worker twice submit a job whose source is
bounded, and neither made sure the job outlived the sequence. In the
restart-budget test that produced a real CI failure blaming the restart gate
for a job that had simply finished: the assertion reads a clean exit as an
unenforced budget, and it failed faster than it passes. Its sibling had the
same hazard in the quieter direction, since it expects the job to survive, so
an early finish let the second worker loss land on an idle worker and the test
passed without ever observing the second recovery it is named for. Both now
give the source enough runtime to outlast the kill sequence, and both assert
the job is still running before the second kill, so that outcome fails naming
its own cause instead of being read as a verdict on the engine. The mechanism
was demonstrated rather than inferred: standing a five-second sleep in for a
slow machine at the point the race lives reproduces the CI failure exactly and
makes the sibling's silent pass reproducible, and neither survives the same
forcing once the fix is in. Nothing in the engine changed.

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

**A hot cutover no longer stalls when an old subtask's exit reaches the
coordinator before the cutover checkpoint completes.** The rescaled
operator's old subtasks end at the cutover checkpoint C the moment they
forward it, while C completes only on the last participant's ack, the
sink's, so an exit can land first. The coordinator counted an exit as a
drained ack only once C had completed, and one that came earlier fell
through to the ordinary completion accounting: the drain tally never
reached the old parallelism, the cut sat at its phase deadline, and sixty
seconds later the cutover aborted to the replan. The job still rescaled,
but by stopping, and the `HotRescaleTest` hold-open cases failed that way
twice in CI (exits 160 to 545 ms after the trigger) and never on demand.
An exit that beats the completion is now held on the cutover and counted
the moment C completes, with the rebind dispatched from there when that
closes the drain, so the order between the two no longer decides the
outcome; if the cutover aborts first, the held exits are released to the
replan rather than waited for, which the worker-loss case of the same
family checks. The schedule is pinned rather than repeated: a fault point,
`rescale.hot_cut_ack`, is reached before the coordinator lock on an ack for C
from a task the operator feeds, and
`HotRescaleTest.OldSubtasksEndingBeforeTheCutCompletesStillCutOver` holds the
sink's ack back for two seconds so an exit lands while C is still open and
requires the cutover to complete anyway. Mutation-checked: with the pin and
without the fix, the cut times out and the test fails.

**Trace validation fits its budget and gates again.** The job that model-checks
every protocol trace a build's tests leave never passed until now. Its time
went into TLC re-deriving the model's constants from the trace on every
reference, quadratic in trace length; the merge script now writes them as a
generated module of literals and the module reads each event once, so a
1,500-event trace validates in seconds and the recorded set in two. Of the
divergences it then reported, four shapes were the engine's account of
itself, not its behaviour, and are corrected: the checkpoint ack is recorded
where the sink sends it, a takeover once per job per leadership with the
redeploy only after the submit that deploys, and the completed marker's
event before the fault point that models a death after it. Four shapes were
behaviours the specification lacked and now has: any worker of the job may
die, whatever it hosts; `RestartOnError`, the whole-job restart the
coordinator begins for a subtask error or an unattributed transport failure;
a sink with more than one ack outstanding, since a barrier arriving right
behind another is prepared before the earlier snapshot's ack is sent; and a
recoverable-family sink preparing while an older checkpoint's commit is
still executing, which only the Kafka family cannot do.
Runs that rescale an operator carry a `Rescale` scope marker and are skipped
and counted rather than judged, since the model fixes the sink set and its
hosts for the run. Documented under
[Trace validation](https://orhaugh.github.io/clink/internals/exactly-once-specification/#trace-validation).

Its first gating run then found two more, both the model's: a worker's death
was taken to kill the sinks its fixed hosts assign to it, so a sink that had
moved to another worker on a redeploy died with a machine it was no longer
on, and validation now follows the placements the run recorded; and a sink
the last redeploy had placed but that had not finished opening could not be
a survivor at all, though a run records exactly that drain. It may now be
one or not, according to whether its deploy had landed when the loss was
declared, which is the shape the mutant suite insisted on: made mandatory
rather than possible, it left `broadcast_during_drain` unrefutable, the
model having lost the interleaving the defect needs. Forty-six of the
fifty-eight traces a build leaves are now accepted and twelve skipped as
rescaled, in under a minute.

The specification's own checks are split in two, the models in one job and
the mutants in another, because the two want opposite shapes: one model
dominates the models and TLC's workers are worth more to it than
concurrency is to the short ones, while the fifteen mutants are independent
and none dominates. `CHECK_JOBS` runs that many at a time, as `TRACE_JOBS`
already did for traces. The pinned TLA+ tools are mirrored on this repo's
own `deps` release: upstream's v1.8.0 is a rolling pre-release whose asset
is re-cut from master, which broke the checksum and took both jobs down
without them checking anything.

A later gating run diverged on a trace that was missing a line rather than
showing a wrong step. The coordinator records `WriteCompleted` after the
COMPLETED marker's durable write, so the line never claims a marker that is
not on disk, and a kill landing between the two left the marker without its
line: the next coordinator recovered from a checkpoint the trace never saw
written, and its redeploy read as impossible. The trace module now admits
that marker as a hidden step only when the next event is a takeover whose
own read of the disk found exactly that checkpoint, and the run is kept
under `formal/traces/` so the step stays exercised. The engine was correct
throughout: the test's own exactly-once assertion passed on the same run.

**A savepoint restore into a different subtask layout is refused.** State
is restored by job-global subtask index, and a submit-time restore (a
savepoint, an explicit checkpoint id, an HA recovery) deploys every task on
"restore from my own index". A plan whose index set differs from the one
that took the checkpoint therefore handed its operators each other's state,
with a worker's `restore discarded N keyed entries` warning as the only
trace. Upgrading made it likely: SQL scalar subqueries in `SELECT` and
null-aware `IN`/`NOT IN` now run as one instance, so a v0.8.0 savepoint of
such a job at parallelism 2 records 16 subtasks where the job now plans 15,
and restoring it brought a downstream `GROUP BY` back without any of its
groups. The coordinator now compares the participant set in the
checkpoint's `COMPLETED` marker with the new plan before anything deploys
and refuses a mismatch, naming the checkpoint, both layouts, the plan's
single-instance operators and the remedy. It blocks only on a definite
verdict: no identifiable marker, or one older than the participant set,
proceeds as before. An HA recovery replans the graph as submitted, since a
rescale does not rewrite the persisted manifest, so a job rescaled and then
failed over is now refused recovery with the reason logged, where before it
recovered with misrouted state. Documented under
[Restore addressing](https://orhaugh.github.io/clink/internals/fault-tolerance-and-rescale/)
and in the runbook.

**The Kafka connector runs under ThreadSanitizer.** librdkafka starts its
threads with C11 `thrd_create` and synchronises them with `mtx_*` and
`cnd_*`, which glibc implements without going through the pthread entry
points TSan intercepts. TSan never registered those threads and crashed in
its own runtime on the first one that allocated, so every test that opened a
producer, a consumer or a mock cluster had been excluded from the TSan pass
by name, 22 of them by the end, and the source, sink, transaction and
registry-format paths had no race checking at all. TSan builds on Linux now
compile a shim into every target that links `clink::kafka`, forwarding each
C11 thread, mutex and condition-variable call to the pthread function glibc
would have used, through the entry point TSan does intercept. The Kafka suite
runs under TSan with nothing excluded, so clink's own code on either side of
librdkafka is race-checked; reports whose stacks pass through the
uninstrumented library itself stay suppressed, as before. Every definition is
weak, because an executable that also links an object library consuming
`clink::kafka` compiles the file twice. See the Kafka connector's
[testing notes](https://orhaugh.github.io/clink/connectors/kafka/#testing).

**Plugin bytes cross the wire in bulk.** The cluster protocol encoded and
decoded a job module's bytes one call per byte, three times before a submit
was acknowledged: the client's encode, the coordinator's decode and the
Deploy encode. An optimised build barely noticed, but a sanitizer build of a
64 MiB module spent 2.5 to 4 s on each pass, enough to time out the client's
10 s acknowledgement on a slow runner, which is what kept the UBSan nightly
red. The blob's framing is a string's, so it now moves through the string
primitives in one call each way. The wire bytes are unchanged.

**A project can pull clink in with FetchContent.** A consumer that builds
clink's source inside its own tree had no example and no test, and two
things were broken on that route: `clink::FlatMap`'s header was not a usage
requirement of `clink::core` in the build tree, so any translation unit
including a window or keyed-state operator failed to find
`ankerl/unordered_dense.h`, and `CLINK_BUILD_TESTS` and
`CLINK_BUILD_EXAMPLES` defaulted on as a subproject, building googletest and
the whole suite. The header is now a build-interface usage requirement and
both options default to `PROJECT_IS_TOP_LEVEL`, leaving clink's own build
unchanged. `docs/consumer-examples/fetchcontent` is the example, and CI
builds it from the commit under test.

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
