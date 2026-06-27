#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "clink/connectors/cdc_event.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink {

// One snapshot SELECT to run before streaming begins. Each row is emitted
// as an Insert CdcEvent with `table` set to the supplied name and `lsn`
// set to the slot's consistent_point.
struct PostgresCdcSnapshotQuery {
    std::string table;  // schema-qualified, e.g. "public.users"
    std::string query;  // SELECT against the snapshot. Returns rows.
};

// PostgresCdcSource subscribes to a Postgres logical replication slot and
// emits a stream of CdcEvents - transaction boundaries plus row-level
// INSERT/UPDATE/DELETE changes.
//
// The connection must be made with `replication=database` (the source
// adds it to the supplied conninfo) and the server must be running with
// `wal_level=logical`. This is verified at open() time.
//
// Plugin: only `test_decoding` is supported in the MVP. It emits text
// output that's easy to parse and is shipped with every Postgres image.
// `pgoutput` (binary) and `wal2json` will plug in here later.
//
// Cancellation: produce() polls async with a small sleep; this->cancel()
// is observed within ~50ms.
class PostgresCdcSource final : public Source<CdcEvent> {
public:
    struct Options {
        // libpq connection string. The class adds `replication=database`
        // automatically; the user supplies host/port/user/password/dbname.
        std::string conninfo;
        // Replication slot name. If `create_slot` is true and the slot
        // doesn't already exist, it's created at open() time.
        std::string slot_name;
        // Logical decoding plugin. Two are supported: "test_decoding"
        // (text output, easy parsing, fine for tests/demos) and "pgoutput"
        // (binary output, the production-grade native protocol). For
        // pgoutput, `publication_names` is required.
        std::string plugin{"test_decoding"};
        // Create the slot on open() if missing. Idempotent (existing slot
        // reused). Must be true on first run to bootstrap the slot.
        bool create_slot{true};
        // How long produce() may block waiting for new data. Smaller values
        // give snappier cancel response at a small CPU cost.
        std::chrono::milliseconds poll_interval{50};

        // ----- pgoutput-only -----
        // Comma-separated publication name(s) the slot subscribes to.
        // Required when plugin == "pgoutput". Create publications via
        // `CREATE PUBLICATION p FOR TABLE ...` or `... FOR ALL TABLES`.
        std::string publication_names{};
        // pgoutput protocol version. 1 covers basic INSERT/UPDATE/DELETE
        // and is supported by every Postgres release that ships pgoutput.
        // Higher versions add streaming-in-progress, two-phase commit, etc.
        int proto_version{1};

        // ----- Keepalives -----
        // Period for periodic Standby Status Update messages back to the
        // server. The slot's confirmed_flush_lsn advances as the client
        // acknowledges receipt, allowing WAL cleanup. Set to 0 to disable
        // periodic sends (the source still replies immediately when the
        // server requests one).
        std::chrono::milliseconds standby_status_interval{std::chrono::seconds{10}};

        // ----- Snapshot-then-stream -----
        // When true, the slot is created with EXPORT_SNAPSHOT. For each
        // entry in `initial_snapshot`, the source runs the SELECT bound to
        // the slot's exported snapshot on a separate libpq connection and
        // emits each returned row as an Insert CdcEvent. Streaming starts
        // at the slot's consistent_point afterwards, so no events between
        // the snapshot and the stream are lost or duplicated.
        //
        // Only effective on first slot creation; if the slot already
        // exists the snapshot phase is skipped and the source warns.
        bool enable_initial_snapshot{false};
        std::vector<PostgresCdcSnapshotQuery> initial_snapshot{};

        // ----- Slot lifecycle -----
        // When true, close() drops the replication slot via
        // pg_drop_replication_slot on a fresh admin connection. Use for
        // ephemeral / per-job slots (CI, integration tests). Default false
        // because production slots are typically long-lived and shared.
        bool drop_slot_on_close{false};
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
    // (BATCH-1). Matches the Source default; stated explicitly for the contract.
    [[nodiscard]] bool is_bounded() const noexcept override { return false; }

    // #60: source replay. The cursor is the received WAL LSN; persisting it
    // lets a restart resume the logical-replication stream from the checkpointed
    // position via START_REPLICATION rather than the slot's default. This is
    // exactly-once at the source boundary for the DECODABLE change stream,
    // provided the replication slot still retains WAL from that LSN (its
    // restart_lsn has not advanced past it); records between the last checkpoint
    // and a crash are replayed, which the downstream changelog/2PC sink
    // reconciles. open() uses a restored LSN as the START_REPLICATION start
    // position when present.
    //
    // Carve-out: an I/U/D change event that the pgoutput decoder cannot decode
    // (unknown relation from a missed Relation message, or a truncated tuple) is
    // DROPPED, not replayed - the checkpointed LSN advances past it regardless of
    // decode success, so it is at-most-once. Such drops are counted + flagged via
    // dropped_events_total{connector="postgres_cdc"} + errors_total rather than
    // lost silently. Granularity note: the cursor is the received WAL LSN
    // (max dataStart/walEnd seen), not the commit LSN of the last decoded row, so
    // a resume can re-read from the start of an in-flight transaction; the
    // downstream 2PC sink dedupes the overlap.
    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId ckpt) override;
    bool restore_offset(StateBackend& backend, OperatorId op_id) override;

    std::string name() const override { return "postgres_cdc_source"; }

    // Observability/test accessor: the LSN passed to the last START_REPLICATION -
    // the restored checkpoint LSN when resuming, the slot consistent_point on a
    // fresh create, or "0/0" for the slot default. Lets a test prove a checkpoint
    // restore (not the slot default) actually drove the resume position. Empty
    // until open() has run.
    [[nodiscard]] std::string start_position() const;

    static bool is_real_implementation();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink
