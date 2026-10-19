#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "clink/connectors/cdc_event.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink {

// One snapshot SELECT to run before streaming begins. Each row is emitted
// as an Insert CdcEvent with `table` set to the supplied name and `lsn`
// set to the replication position streaming starts from.
struct PostgresCdcSnapshotQuery {
    std::string table;  // schema-qualified, e.g. "public.users"
    std::string query;  // SELECT against the snapshot. Returns rows.
};

// PostgresCdcSource subscribes to a Postgres logical replication slot and
// emits a stream of CdcEvents - transaction boundaries plus row-level
// INSERT/UPDATE/DELETE changes - optionally preceded by a snapshot of the
// existing rows.
//
// Plugins: `pgoutput` (the server's built-in protocol, version 1) and
// `test_decoding` (text output, for tests and demos).
//
// Position and recovery. The cursor a checkpoint records is the end LSN of
// the last transaction whose changes were all emitted, plus, when the
// checkpoint falls inside a transaction, that transaction's commit LSN and
// how many of its changes were already emitted. A restore restarts the
// stream at that end LSN; the server sends every transaction committed from
// there, the interrupted one first, and the source skips the changes the
// checkpoint already holds. Replay from a checkpoint is therefore exact:
// nothing before the cut is emitted twice by the source and nothing after it
// is skipped.
//
// Acknowledgement. The slot is confirmed (the Standby Status Update's flush
// position) only up to the position recorded by a checkpoint the runtime has
// reported complete (notify_checkpoint_complete). An aborted checkpoint
// confirms nothing. So the server keeps every change a restore can still
// need, and a job that never completes a checkpoint never releases WAL.
//
// Refusals. Before streaming starts, and again after every reconnect, the
// source checks that the slot exists, still has its WAL and is not confirmed
// beyond the position the source resumes from. PostgreSQL would otherwise
// start silently from its own later position; the source refuses instead.
//
// Initial snapshot. With `enable_initial_snapshot`, a newly created slot
// exports its snapshot and the snapshot rows are read through a server-side
// cursor bound to it, `snapshot_batch_rows` at a time, before streaming
// starts at the slot's consistent point: no change is lost or seen twice at
// the boundary. When no such boundary can exist - the slot already existed,
// or a checkpoint taken during the snapshot is restored - the snapshot is
// taken again from a fresh repeatable-read transaction and streaming starts
// at the slot's retained position, before that snapshot. Changes the fresh
// snapshot already reflects are then replayed after it. Every row and change
// carries the whole row image, so a consumer that applies them by key
// converges on the table's state once the stream passes the snapshot point,
// though it may briefly hold an older version of a row on the way.
//
// Cancellation. Every wait on the server (connect, slot creation, snapshot
// fetches, streaming) observes cancel() within about 100 ms.
class PostgresCdcSource final : public Source<CdcEvent> {
public:
    // What to do when a change event cannot be decoded (unknown relation,
    // truncated tuple) or, under `strict`, cannot be represented (an unchanged
    // TOAST value with no old row to take it from, a TRUNCATE of the table).
    // Drop = count it and continue: at-most-once for that event. Fail = throw,
    // so the job restarts from its last checkpoint and meets the same event
    // again; it never moves past it. Fail is a hard stop - a permanently
    // undecodable event fails every restart - so choose Drop only where
    // continuing matters more than the lost change.
    enum class DecodeErrorPolicy { Drop, Fail };

    struct Options {
        // libpq connection string or URI. The source adds the replication
        // parameter itself and connects with application_name, TimeZone=UTC
        // and DateStyle=ISO unless the string sets them.
        std::string conninfo;
        // Replication slot name. If `create_slot` is true and the slot
        // doesn't already exist, it's created at open() time.
        std::string slot_name;
        // Logical decoding plugin: "test_decoding" (text output, easy
        // parsing, fine for tests and demos) or "pgoutput" (binary output, the
        // production-grade native protocol). pgoutput needs
        // `publication_names`.
        std::string plugin{"test_decoding"};
        // Create the slot on open() if missing. Never used to replace a slot
        // that a checkpoint's position depends on: a restore that finds its
        // slot missing refuses.
        bool create_slot{true};
        // How long produce() waits for new data before it returns.
        std::chrono::milliseconds poll_interval{50};

        // ----- pgoutput-only -----
        // Comma-separated publication name(s) the slot subscribes to.
        // Required when plugin == "pgoutput".
        std::string publication_names{};
        // pgoutput protocol version. Only 1 is supported under `strict`: the
        // source does not request streamed or two-phase transactions.
        int proto_version{1};

        // ----- Keepalives -----
        // Period for Standby Status Update messages back to the server. They
        // carry the confirmed position and ask the server to answer, which is
        // how a silent connection is detected. Set to 0 to send only the
        // replies the server asks for and the confirmations checkpoints make.
        std::chrono::milliseconds standby_status_interval{std::chrono::seconds{10}};

        // ----- Snapshot-then-stream -----
        // When true, the source emits the rows of each `initial_snapshot`
        // query as Insert events before streaming (see the class comment for
        // the exact and convergent cases). When false, it streams changes only.
        // With `table` set and `initial_snapshot` left empty, the source reads
        // the table itself at open: the declared columns (every column when
        // none are declared), filtered by the publications' row filter so the
        // snapshot holds the rows the stream publishes. A row filter cannot be
        // combined with queries given here, which open() refuses.
        bool enable_initial_snapshot{false};
        std::vector<PostgresCdcSnapshotQuery> initial_snapshot{};
        // Rows per snapshot FETCH; also the most rows one produce() emits
        // during the snapshot.
        std::size_t snapshot_batch_rows{1000};

        // ----- Slot lifecycle -----
        // When true, close() drops the replication slot via
        // pg_drop_replication_slot on a fresh admin connection. Use for
        // ephemeral / per-job slots (CI, integration tests). A job that drops
        // its slot cannot be restored from its checkpoints afterwards.
        bool drop_slot_on_close{false};

        // Behaviour when a change event cannot be decoded (see DecodeErrorPolicy).
        DecodeErrorPolicy decode_error_policy{DecodeErrorPolicy::Drop};

        // ----- Connection management -----
        // How long one connection attempt may take.
        std::chrono::milliseconds connect_timeout{std::chrono::seconds{30}};
        // How long the source keeps trying to re-establish a lost replication
        // connection, resuming from its position, before it fails the task.
        // 0 fails the task at once.
        std::chrono::milliseconds reconnect_timeout{std::chrono::seconds{60}};
        // The most change events one produce() call emits.
        std::size_t max_batch_events{1024};

        // ----- Single-table capture -----
        // Schema-qualified table to capture, ordinary or partitioned (through
        // its root, published with publish_via_partition_root). When set,
        // changes to other tables of the publication are skipped, and open()
        // checks that the table exists, is in the publication with inserts,
        // updates and deletes published under one row filter (or none), has
        // `key_columns` as its primary key and a replica identity that carries
        // it (each partition's, for a partitioned table), and holds every name
        // in `declared_columns`.
        std::string table{};
        // The table's primary key columns. Required with `table`.
        std::vector<std::string> key_columns{};
        // Columns the consumer reads, with their SQL types (used by open()'s
        // checks and by `strict` decoding; empty means every column).
        std::vector<std::pair<std::string, std::string>> declared_columns{};
        // Strict decoding, for consumers that apply rows by key:
        //   * an UPDATE that leaves a declared column's out-of-line (TOAST)
        //     value unchanged takes it from the old row image, which only
        //     REPLICA IDENTITY FULL sends; without it the event is a decode
        //     error rather than a row with that value missing;
        //   * an UPDATE that changes the key is emitted as a Delete of the old
        //     key followed by an Update carrying the new row;
        //   * a TRUNCATE of the table is a decode error, because it cannot be
        //     expressed as row changes.
        bool strict{false};
        // With `strict`: the consumer keeps the latest row of every key (the
        // SQL contract's keep_rows). An unchanged out-of-line value the change
        // does not carry is then passed on marked for the consumer to fill in,
        // rather than refused, and a TRUNCATE of the table is emitted as a
        // Truncate event for the consumer to expand into deletes.
        bool consumer_keeps_rows{false};
    };

    explicit PostgresCdcSource(Options opts);
    ~PostgresCdcSource() override;

    // Drop the underlying replication slot via a fresh admin connection.
    // Idempotent: succeeds silently if the slot doesn't exist. Throws if
    // the slot exists but is currently in use by another consumer.
    //
    // Postgres won't drop a slot while its session is still open, so this
    // either has to be called *after* close() (preferred) or with the
    // help of `Options::drop_slot_on_close=true` which sequences the
    // operations correctly.
    void drop_slot();

    PostgresCdcSource(const PostgresCdcSource&) = delete;
    PostgresCdcSource& operator=(const PostgresCdcSource&) = delete;
    PostgresCdcSource(PostgresCdcSource&&) = delete;
    PostgresCdcSource& operator=(PostgresCdcSource&&) = delete;

    void open() override;
    bool produce(Emitter<CdcEvent>& out) override;
    void close() override;
    void cancel() override;

    // Logical replication is an endless change stream: unbounded by nature, so
    // it never triggers the end-of-input drain or the batch execution path
    // (BATCH-1). A lost connection is reconnected or fails the task; it never
    // ends the stream.
    [[nodiscard]] bool is_bounded() const noexcept override { return false; }

    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId ckpt) override;
    bool restore_offset(StateBackend& backend, OperatorId op_id) override;
    void notify_checkpoint_complete(CheckpointId ckpt) override;
    void notify_checkpoint_aborted(CheckpointId ckpt) override;

    std::string name() const override { return "postgres_cdc_source"; }

    // The LSN passed to the last START_REPLICATION: the restored resume
    // position, the slot's consistent point after a fresh create, or the
    // slot's confirmed position. Empty until streaming has started.
    [[nodiscard]] std::string start_position() const;

    // True while the source is emitting the initial snapshot. One produce()
    // call emits rows of one phase only, so a caller that reads this before
    // the call knows which phase the call's rows belong to.
    [[nodiscard]] bool snapshot_in_progress() const noexcept;

    // The position the slot has been confirmed at by this source (0 = none).
    [[nodiscard]] std::uint64_t confirmed_lsn() const noexcept;

    // After a snapshot taken fresh (a restore during the snapshot, or a slot
    // that already existed), the WAL position that snapshot was read at:
    // a streamed change before it replays what the snapshot already holds,
    // possibly for a row the snapshot no longer has. 0 once the stream has
    // passed it, or when the snapshot was bound to the slot's export.
    [[nodiscard]] std::uint64_t converging_until() const noexcept;

    static bool is_real_implementation();

private:
    struct Impl;

    void begin_snapshot_(const std::string& snapshot_name);
    void start_streaming_();
    void start_or_retry_();
    void connection_lost_(const std::string& reason);
    bool reconnect_step_();
    bool produce_snapshot_(Emitter<CdcEvent>& out);
    bool produce_stream_(Emitter<CdcEvent>& out);
    void send_status_(bool reply_requested);
    void seed_from_restore_();

    std::unique_ptr<Impl> impl_;
};

}  // namespace clink
