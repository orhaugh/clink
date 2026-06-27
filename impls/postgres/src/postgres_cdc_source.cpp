#include "clink/connectors/postgres_cdc_source.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>

#include "clink/connectors/pg_cdc_test_seam.hpp"
#include "clink/connectors/postgres_decoder.hpp"
#include "clink/metrics/connector_metrics.hpp"

#ifdef CLINK_HAS_POSTGRES
#include <libpq-fe.h>
#endif

namespace clink {

#ifdef CLINK_HAS_POSTGRES

namespace {

// All wire/text decoders live in clink::pg (postgres_decoder.hpp). The
// names are imported here so the rest of this file reads the same as
// before the extraction.
using pg::format_lsn;
using pg::lookup_builtin_type_name;
using pg::parse_test_decoding;
using pg::postgres_epoch_us_now;
using pg::read_be16;
using pg::read_be32;
using pg::read_be64;
using pg::read_cstring;
using pg::write_be64;

// =====================================================================
// pgoutput parser - binary protocol with relation + type cache.
// =====================================================================
//
// Message types handled:
//   B (Begin), C (Commit), R (Relation), I/U/D (row changes),
//   T (Truncate), O (Origin, ignored), Y (Type, populates user-type cache),
//   M (Logical Message, ignored).
//
// Streaming (proto_version >= 2) and two-phase commit (>= 3) messages
// (S/E/c/A/P/p/K) are recognised and skipped silently - they only appear
// when the user explicitly enables those START_REPLICATION options, which
// this MVP does not.
class PgOutputState {
public:
    // Count of I/U/D change events that could NOT be decoded and were dropped
    // (unknown relation, missing/truncated tuple). Surfaced by the source as
    // dropped_events_total so this otherwise-silent data loss is observable.
    // Benign skips (begin/commit/relation metadata, stream/2pc control messages)
    // are NOT counted.
    std::uint64_t dropped_data_events{0};

    std::optional<CdcEvent> on_message(std::string_view payload, std::string lsn) {
        if (payload.empty()) {
            return std::nullopt;
        }
        const char type = payload[0];
        const std::string_view rest = payload.substr(1);
        switch (type) {
            case 'B':
                return on_begin(rest, std::move(lsn));
            case 'C':
                return on_commit(rest, std::move(lsn));
            case 'R':
                on_relation(rest);
                return std::nullopt;
            case 'Y':
                on_type(rest);
                return std::nullopt;
            case 'I':
                return on_insert(rest, std::move(lsn));
            case 'U':
                return on_update(rest, std::move(lsn));
            case 'D':
                return on_delete(rest, std::move(lsn));
            case 'T':
                return on_truncate(rest, std::move(lsn));
            case 'O':  // Origin - replicated origin name; we currently ignore
            case 'M':  // Logical Message (pg_logical_emit_message); ignore
            case 'S':  // Stream Start (proto >= 2)
            case 'E':  // Stream Stop
            case 'c':  // Stream Commit
            case 'A':  // Stream Abort
            case 'P':  // Prepare (proto >= 3)
            case 'p':  // Stream Prepare
            case 'K':  // Stream Prepare in proto >= 3
                return std::nullopt;
            default:
                return std::nullopt;
        }
    }

private:
    // Record a dropped data change (returns nullopt for the caller's convenience).
    std::optional<CdcEvent> drop_() {
        ++dropped_data_events;
        return std::nullopt;
    }

    struct Column {
        std::uint8_t flags{};  // bit 0 = part of replica identity key
        std::string name;
        std::uint32_t type_oid{};
        std::int32_t type_modifier{};
    };
    struct Relation {
        std::uint32_t id{};
        std::string ns;
        std::string name;
        std::vector<Column> columns;
    };

    std::unordered_map<std::uint32_t, Relation> relations_;
    // User-defined type names from Y messages: oid → "ns.name". Built-in
    // types are not sent as Y messages; resolve those via
    // lookup_builtin_type_name().
    std::unordered_map<std::uint32_t, std::string> user_types_;

    std::string resolve_type_name(std::uint32_t oid) const {
        if (auto it = user_types_.find(oid); it != user_types_.end()) {
            return it->second;
        }
        if (const char* b = lookup_builtin_type_name(oid); b != nullptr) {
            return b;
        }
        return {};  // unknown - leave field type empty
    }

    void on_type(std::string_view rest) {
        if (rest.size() < 4) {
            return;
        }
        const std::uint32_t oid = read_be32(rest.data());
        rest.remove_prefix(4);
        const std::string ns = read_cstring(rest);
        const std::string name = read_cstring(rest);
        if (ns.empty() && name.empty()) {
            return;
        }
        user_types_[oid] = ns.empty() ? name : (ns + "." + name);
    }

    std::optional<CdcEvent> on_truncate(std::string_view rest, std::string lsn) {
        // Format: nrelations (4) + flags (1) + relation_id[] (4 × n).
        if (rest.size() < 5) {
            return std::nullopt;
        }
        const std::uint32_t n = read_be32(rest.data());
        rest.remove_prefix(5);  // ncols + flags byte
        if (rest.size() < 4 * n) {
            return std::nullopt;
        }
        CdcEvent ev;
        ev.op = CdcEvent::Op::Truncate;
        ev.lsn = std::move(lsn);
        // We pick the first relation as the event's "table"; if the
        // TRUNCATE batched several, the rest are added as values entries
        // so users still see them.
        for (std::uint32_t i = 0; i < n; ++i) {
            const std::uint32_t rel_id = read_be32(rest.data() + (i * 4));
            auto it = relations_.find(rel_id);
            const std::string tbl = (it != relations_.end())
                                        ? (it->second.ns + "." + it->second.name)
                                        : ("oid=" + std::to_string(rel_id));
            if (i == 0) {
                ev.table = tbl;
            } else {
                ev.values.push_back(CdcField{.name = "also", .value = tbl});
            }
        }
        return ev;
    }

    static std::optional<CdcEvent> on_begin(std::string_view rest, std::string lsn) {
        if (rest.size() < 20) {
            return std::nullopt;
        }
        CdcEvent ev;
        ev.op = CdcEvent::Op::Begin;
        ev.lsn = std::move(lsn);
        // final_lsn (8) + commit_ts (8) + xid (4)
        ev.xid = static_cast<std::int64_t>(read_be32(rest.data() + 16));
        return ev;
    }

    static std::optional<CdcEvent> on_commit(std::string_view rest, std::string lsn) {
        if (rest.size() < 25) {
            return std::nullopt;
        }
        CdcEvent ev;
        ev.op = CdcEvent::Op::Commit;
        ev.lsn = std::move(lsn);
        // flags (1) + commit_lsn (8) + end_lsn (8) + commit_ts (8)
        return ev;
    }

    void on_relation(std::string_view rest) {
        if (rest.size() < 4) {
            return;
        }
        Relation rel;
        rel.id = read_be32(rest.data());
        rest.remove_prefix(4);
        rel.ns = read_cstring(rest);
        rel.name = read_cstring(rest);
        if (rest.empty()) {
            return;
        }
        rest.remove_prefix(1);  // replica identity byte
        if (rest.size() < 2) {
            return;
        }
        const std::uint16_t ncols = read_be16(rest.data());
        rest.remove_prefix(2);
        rel.columns.reserve(ncols);
        for (std::uint16_t i = 0; i < ncols; ++i) {
            if (rest.empty()) {
                return;
            }
            Column c;
            c.flags = static_cast<std::uint8_t>(rest[0]);
            rest.remove_prefix(1);
            c.name = read_cstring(rest);
            if (rest.size() < 8) {
                return;
            }
            c.type_oid = read_be32(rest.data());
            c.type_modifier = static_cast<std::int32_t>(read_be32(rest.data() + 4));
            rest.remove_prefix(8);
            rel.columns.push_back(std::move(c));
        }
        relations_[rel.id] = std::move(rel);
    }

    // Decode a TupleData block. Each column produces one CdcField with
    // name, value, type, and is_null populated using the relation's
    // cached schema and the surrounding parser's type cache.
    // `truncated` is set if the tuple ran short of its declared column count or a
    // column value overran the buffer - the caller must then DROP the change
    // rather than emit a partial (column-missing) event, which would be silent
    // data corruption.
    std::vector<CdcField> parse_tuple(std::string_view& cursor,
                                      const Relation& rel,
                                      bool& truncated) const {
        std::vector<CdcField> out;
        if (cursor.size() < 2) {
            truncated = true;
            return out;
        }
        const std::uint16_t ncols = read_be16(cursor.data());
        cursor.remove_prefix(2);
        out.reserve(ncols);
        for (std::uint16_t i = 0; i < ncols; ++i) {
            if (cursor.empty()) {
                truncated = true;  // fewer columns present than declared
                break;
            }
            const char kind = cursor[0];
            cursor.remove_prefix(1);

            CdcField field;
            if (i < rel.columns.size()) {
                field.name = rel.columns[i].name;
                field.type = resolve_type_name(rel.columns[i].type_oid);
            }

            switch (kind) {
                case 'n':
                    field.is_null = true;
                    break;
                case 'u':
                    // unchanged TOASTed value - leave value empty,
                    // is_null=false. Users distinguish via is_null.
                    break;
                case 't':
                case 'b': {
                    if (cursor.size() < 4) {
                        truncated = true;
                        return out;
                    }
                    const std::uint32_t len = read_be32(cursor.data());
                    cursor.remove_prefix(4);
                    if (cursor.size() < len) {
                        truncated = true;
                        return out;
                    }
                    field.value = std::string{cursor.substr(0, len)};
                    cursor.remove_prefix(len);
                    break;
                }
                default:
                    // Unknown column-kind byte - bail out to avoid running
                    // off the end of the buffer.
                    truncated = true;
                    return out;
            }
            out.push_back(std::move(field));
        }
        return out;
    }

    std::optional<CdcEvent> on_insert(std::string_view rest, std::string lsn) {
        if (rest.size() < 5) {
            return drop_();
        }
        const std::uint32_t rel_id = read_be32(rest.data());
        rest.remove_prefix(4);
        if (rest[0] != 'N') {
            return drop_();  // INSERT must have an N (new) tuple
        }
        rest.remove_prefix(1);
        auto it = relations_.find(rel_id);
        if (it == relations_.end()) {
            return drop_();  // unknown relation; can happen if R was missed
        }
        CdcEvent ev;
        ev.op = CdcEvent::Op::Insert;
        ev.lsn = std::move(lsn);
        ev.table = it->second.ns + "." + it->second.name;
        bool truncated = false;
        ev.values = parse_tuple(rest, it->second, truncated);
        if (truncated) {
            return drop_();  // partial tuple: drop rather than emit a corrupt row
        }
        return ev;
    }

    std::optional<CdcEvent> on_update(std::string_view rest, std::string lsn) {
        if (rest.size() < 5) {
            return drop_();
        }
        const std::uint32_t rel_id = read_be32(rest.data());
        rest.remove_prefix(4);
        // Optional 'K' (key only) or 'O' (old tuple) preamble before 'N'.
        if (!rest.empty() && (rest[0] == 'K' || rest[0] == 'O')) {
            rest.remove_prefix(1);
            auto rel_it = relations_.find(rel_id);
            if (rel_it != relations_.end()) {
                bool old_trunc = false;
                (void)parse_tuple(rest, rel_it->second, old_trunc);  // discard old image
                if (old_trunc) {
                    // A truncated OLD image leaves the cursor at an arbitrary
                    // offset, so the NEW image can no longer be located: a stray
                    // 'N' byte would let a structurally-valid-but-WRONG NEW tuple
                    // be emitted uncounted. Treat it as unrecoverable framing loss
                    // and drop rather than emit corruption.
                    return drop_();
                }
            } else {
                return drop_();
            }
        }
        if (rest.empty() || rest[0] != 'N') {
            return drop_();
        }
        rest.remove_prefix(1);
        auto it = relations_.find(rel_id);
        if (it == relations_.end()) {
            return drop_();
        }
        CdcEvent ev;
        ev.op = CdcEvent::Op::Update;
        ev.lsn = std::move(lsn);
        ev.table = it->second.ns + "." + it->second.name;
        bool truncated = false;
        ev.values = parse_tuple(rest, it->second, truncated);
        if (truncated) {
            return drop_();
        }
        return ev;
    }

    std::optional<CdcEvent> on_delete(std::string_view rest, std::string lsn) {
        if (rest.size() < 5) {
            return drop_();
        }
        const std::uint32_t rel_id = read_be32(rest.data());
        rest.remove_prefix(4);
        if (rest.empty() || (rest[0] != 'K' && rest[0] != 'O')) {
            return drop_();
        }
        rest.remove_prefix(1);
        auto it = relations_.find(rel_id);
        if (it == relations_.end()) {
            return drop_();
        }
        CdcEvent ev;
        ev.op = CdcEvent::Op::Delete;
        ev.lsn = std::move(lsn);
        ev.table = it->second.ns + "." + it->second.name;
        bool truncated = false;
        ev.values = parse_tuple(rest, it->second, truncated);
        if (truncated) {
            return drop_();
        }
        return ev;
    }
};

}  // namespace

// Test-only seam (declared in pg_cdc_test_seam.hpp). Defined here so it can reach
// the anonymous-namespace PgOutputState. Drives the decoder over raw pgoutput
// message payloads and reports the decoded events + the drop count, so the F1
// silent-data-loss accounting is unit-testable without a live Postgres.
namespace pg_cdc_testing {

PgDecodeResult decode_pgoutput_messages(const std::vector<std::string>& messages) {
    PgOutputState state;
    PgDecodeResult result;
    for (const auto& msg : messages) {
        std::optional<CdcEvent> ev = state.on_message(msg, "0/0");
        if (ev.has_value()) {
            result.events.push_back(std::move(*ev));
        }
    }
    result.dropped = state.dropped_data_events;
    return result;
}

}  // namespace pg_cdc_testing

// =====================================================================
// PostgresCdcSource implementation
// =====================================================================

struct PostgresCdcSource::Impl {
    Options opts;
    PGconn* conn{nullptr};
    bool started{false};
    std::atomic<bool> cancel{false};
    std::uint64_t received_lsn{0};
    std::chrono::steady_clock::time_point last_status_send{std::chrono::steady_clock::now()};
    PgOutputState pgoutput_state;
    std::uint64_t last_dropped{0};  // pgoutput_state.dropped_data_events at last surfacing
    // Snapshot rows queued before streaming begins. Drained from produce()
    // before any libpq replication reads.
    std::deque<CdcEvent> snapshot_backlog;
    std::string consistent_point_lsn;
    // #60: set by restore_offset() when a checkpointed LSN is loaded, so open()
    // resumes START_REPLICATION from received_lsn instead of the slot default.
    bool restored_from_checkpoint{false};
    // The LSN actually passed to the last START_REPLICATION: the restored
    // checkpoint LSN when resuming, the slot consistent_point on a fresh create,
    // or "0/0" for the slot default. Surfaced via start_position() so a test can
    // prove a checkpoint restore (not the slot default) drove the resume.
    std::string start_position_lsn;
};

bool PostgresCdcSource::is_real_implementation() {
    return true;
}

PostgresCdcSource::PostgresCdcSource(Options opts) : impl_(std::make_unique<Impl>()) {
    impl_->opts = std::move(opts);
}

PostgresCdcSource::~PostgresCdcSource() {
    if (impl_ && impl_->conn != nullptr) {
        PQfinish(impl_->conn);
    }
}

namespace {

// Build a Standby Status Update payload and send it via the COPY stream.
// Layout (per PG docs §52.5.2): 'r' + walWritten + walFlushed + walApplied +
// clientTime + replyRequested.
bool send_standby_status_update(PGconn* conn, std::uint64_t received_lsn, bool reply_requested) {
    std::array<char, 1 + 8 + 8 + 8 + 8 + 1> msg{};
    msg[0] = 'r';
    const std::uint64_t apply_to = received_lsn + 1;  // server expects "next byte" semantics
    write_be64(msg.data() + 1, apply_to);
    write_be64(msg.data() + 9, apply_to);
    write_be64(msg.data() + 17, apply_to);
    write_be64(msg.data() + 25, static_cast<std::uint64_t>(postgres_epoch_us_now()));
    msg[33] = reply_requested ? 1 : 0;
    if (PQputCopyData(conn, msg.data(), static_cast<int>(msg.size())) != 1) {
        return false;
    }
    return PQflush(conn) == 0;
}

// Run snapshot SELECTs on a side connection bound to the slot's exported
// snapshot. Returns the rows already wrapped as Insert CdcEvents.
std::deque<CdcEvent> read_initial_snapshot(const std::string& conninfo,
                                           const std::string& snapshot_name,
                                           const std::string& consistent_lsn,
                                           const std::vector<PostgresCdcSnapshotQuery>& queries) {
    std::deque<CdcEvent> out;
    PGconn* snap_conn = PQconnectdb(conninfo.c_str());
    if (PQstatus(snap_conn) != CONNECTION_OK) {
        const std::string err = PQerrorMessage(snap_conn);
        PQfinish(snap_conn);
        throw std::runtime_error("PostgresCdcSource::snapshot: side connection failed: " + err);
    }

    auto run = [&](const std::string& sql) -> PGresult* {
        PGresult* r = PQexec(snap_conn, sql.c_str());
        if (PQresultStatus(r) != PGRES_COMMAND_OK && PQresultStatus(r) != PGRES_TUPLES_OK) {
            const std::string err = PQerrorMessage(snap_conn);
            PQclear(r);
            PQfinish(snap_conn);
            throw std::runtime_error("PostgresCdcSource::snapshot: " + sql + " failed: " + err);
        }
        return r;
    };

    PQclear(run("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"));
    PQclear(run("SET TRANSACTION SNAPSHOT '" + snapshot_name + "'"));

    for (const auto& q : queries) {
        PGresult* r = run(q.query);
        const int nrows = PQntuples(r);
        const int ncols = PQnfields(r);
        std::vector<std::string> col_names;
        std::vector<std::string> col_types;
        col_names.reserve(static_cast<std::size_t>(ncols));
        col_types.reserve(static_cast<std::size_t>(ncols));
        for (int c = 0; c < ncols; ++c) {
            col_names.emplace_back(PQfname(r, c));
            const auto oid = static_cast<std::uint32_t>(PQftype(r, c));
            const char* nm = lookup_builtin_type_name(oid);
            col_types.emplace_back(nm != nullptr ? nm : std::string{});
        }
        for (int row = 0; row < nrows; ++row) {
            CdcEvent ev;
            ev.op = CdcEvent::Op::Insert;
            ev.table = q.table;
            ev.lsn = consistent_lsn;
            ev.values.reserve(static_cast<std::size_t>(ncols));
            for (int c = 0; c < ncols; ++c) {
                CdcField f;
                f.name = col_names[static_cast<std::size_t>(c)];
                f.type = col_types[static_cast<std::size_t>(c)];
                if (PQgetisnull(r, row, c) != 0) {
                    f.is_null = true;
                } else {
                    f.value = std::string{PQgetvalue(r, row, c),
                                          static_cast<std::size_t>(PQgetlength(r, row, c))};
                }
                ev.values.push_back(std::move(f));
            }
            out.push_back(std::move(ev));
        }
        PQclear(r);
    }

    PQclear(run("COMMIT"));
    PQfinish(snap_conn);
    return out;
}

}  // namespace

void PostgresCdcSource::open() {
    if (impl_->opts.plugin != "test_decoding" && impl_->opts.plugin != "pgoutput") {
        throw std::runtime_error("PostgresCdcSource: unsupported plugin '" + impl_->opts.plugin +
                                 "' (supported: test_decoding, pgoutput)");
    }
    if (impl_->opts.plugin == "pgoutput" && impl_->opts.publication_names.empty()) {
        throw std::runtime_error("PostgresCdcSource: pgoutput requires Options::publication_names");
    }

    const std::string conninfo_repl = impl_->opts.conninfo + " replication=database";
    impl_->conn = PQconnectdb(conninfo_repl.c_str());
    if (PQstatus(impl_->conn) != CONNECTION_OK) {
        const std::string err = PQerrorMessage(impl_->conn);
        PQfinish(impl_->conn);
        impl_->conn = nullptr;
        throw std::runtime_error("PostgresCdcSource::open: connect failed: " + err);
    }

    bool slot_was_created = false;
    if (impl_->opts.create_slot) {
        std::string create_sql = "CREATE_REPLICATION_SLOT \"" + impl_->opts.slot_name +
                                 "\" LOGICAL " + impl_->opts.plugin;
        if (impl_->opts.enable_initial_snapshot) {
            create_sql += " EXPORT_SNAPSHOT";
        }

        PGresult* r = PQexec(impl_->conn, create_sql.c_str());
        const auto status = PQresultStatus(r);
        if (status == PGRES_TUPLES_OK) {
            // Columns: slot_name, consistent_point, snapshot_name, output_plugin
            const int ncols = PQnfields(r);
            std::string snapshot_name;
            for (int c = 0; c < ncols; ++c) {
                const std::string_view name = PQfname(r, c);
                if (PQntuples(r) > 0) {
                    if (name == "consistent_point") {
                        impl_->consistent_point_lsn = PQgetvalue(r, 0, c);
                    } else if (name == "snapshot_name") {
                        snapshot_name = PQgetvalue(r, 0, c);
                    }
                }
            }
            PQclear(r);
            slot_was_created = true;

            if (impl_->opts.enable_initial_snapshot && !snapshot_name.empty()) {
                impl_->snapshot_backlog = read_initial_snapshot(impl_->opts.conninfo,
                                                                snapshot_name,
                                                                impl_->consistent_point_lsn,
                                                                impl_->opts.initial_snapshot);
            }
        } else {
            const std::string err = PQerrorMessage(impl_->conn);
            PQclear(r);
            if (err.find("already exists") == std::string::npos) {
                PQfinish(impl_->conn);
                impl_->conn = nullptr;
                throw std::runtime_error("PostgresCdcSource: CREATE_REPLICATION_SLOT failed: " +
                                         err);
            }
            // Slot existed; if user asked for a snapshot they don't get one.
        }
    }

    // #60: a checkpointed LSN (restored before open()) takes priority - resume
    // the stream from exactly where the last checkpoint left off. Otherwise a
    // freshly-created slot streams from its consistent_point; an existing slot
    // resumes from its server-side confirmed_flush_lsn ("0/0" asks for that).
    std::string start_lsn;
    if (impl_->restored_from_checkpoint && impl_->received_lsn > 0) {
        start_lsn = format_lsn(impl_->received_lsn);
    } else if (slot_was_created && !impl_->consistent_point_lsn.empty()) {
        start_lsn = impl_->consistent_point_lsn;
    } else {
        start_lsn = "0/0";
    }
    impl_->start_position_lsn = start_lsn;
    std::string start_sql =
        "START_REPLICATION SLOT \"" + impl_->opts.slot_name + "\" LOGICAL " + start_lsn;
    if (impl_->opts.plugin == "pgoutput") {
        start_sql += " (proto_version '" + std::to_string(impl_->opts.proto_version) +
                     "', publication_names '" + impl_->opts.publication_names + "')";
    }
    PGresult* r = PQexec(impl_->conn, start_sql.c_str());
    if (PQresultStatus(r) != PGRES_COPY_BOTH) {
        const std::string err = PQerrorMessage(impl_->conn);
        PQclear(r);
        PQfinish(impl_->conn);
        impl_->conn = nullptr;
        throw std::runtime_error("PostgresCdcSource: START_REPLICATION failed: " + err);
    }
    PQclear(r);
    impl_->started = true;
    impl_->last_status_send = std::chrono::steady_clock::now();
}

bool PostgresCdcSource::produce(Emitter<CdcEvent>& out) {
    if (this->cancelled() || impl_->cancel.load() || impl_->conn == nullptr) {
        return false;
    }

    Batch<CdcEvent> batch;

    // Drain any snapshot rows queued during open() first.
    while (!impl_->snapshot_backlog.empty() && batch.size() < std::size_t{256}) {
        batch.emplace(std::move(impl_->snapshot_backlog.front()));
        impl_->snapshot_backlog.pop_front();
    }
    if (!impl_->snapshot_backlog.empty()) {
        const auto sz = batch.size();
        out.emit_data(std::move(batch));
        clink::metrics::connector::records_in_inc("postgres_cdc", sz);
        return true;  // back to top of loop next iter
    }

    // Drain whatever is currently available on the replication stream
    // without blocking.
    std::uint64_t bytes = 0;
    // Surface whatever we decoded THIS call (emitted batch + byte/drop counters).
    // Used both on the normal loop exit and before an EOF/connection-drop early
    // return, so a partially-drained batch and its metrics are never discarded.
    auto surface_decoded = [&]() {
        if (!batch.empty()) {
            const auto sz = batch.size();
            out.emit_data(std::move(batch));
            clink::metrics::connector::records_in_inc("postgres_cdc", sz);
        }
        if (bytes > 0) {
            clink::metrics::connector::bytes_in_inc("postgres_cdc", bytes);
        }
        // Surface any change events the decoder had to DROP (unknown relation /
        // truncated tuple) - otherwise-silent data loss is now counted + flagged.
        if (impl_->pgoutput_state.dropped_data_events > impl_->last_dropped) {
            const std::uint64_t delta =
                impl_->pgoutput_state.dropped_data_events - impl_->last_dropped;
            impl_->last_dropped = impl_->pgoutput_state.dropped_data_events;
            clink::metrics::connector::dropped_events_inc("postgres_cdc", delta);
            clink::metrics::connector::error_inc("postgres_cdc", "source");
        }
    };
    while (true) {
        if (this->cancelled() || impl_->cancel.load()) {
            break;
        }
        char* buffer = nullptr;
        const int n = PQgetCopyData(impl_->conn, &buffer, /*async*/ 1);
        if (n == 0) {
            if (PQconsumeInput(impl_->conn) == 0) {
                surface_decoded();  // flush what we decoded before the EOF
                return false;
            }
            break;
        }
        if (n == -1) {
            if (buffer != nullptr) {
                PQfreemem(buffer);
            }
            surface_decoded();  // flush what we decoded before the stream end
            return false;
        }
        if (n == -2) {
            const std::string err = PQerrorMessage(impl_->conn);
            if (buffer != nullptr) {
                PQfreemem(buffer);
            }
            throw std::runtime_error("PostgresCdcSource: PQgetCopyData failed: " + err);
        }

        if (n >= 1) {
            const char type_byte = buffer[0];
            if (type_byte == 'w' && n >= 25) {
                // XLogData: dataStart (8), walEnd (8), sendTime (8), payload.
                const std::uint64_t data_start = read_be64(buffer + 1);
                impl_->received_lsn = std::max(impl_->received_lsn, data_start);
                const std::string_view payload(buffer + 25, static_cast<std::size_t>(n) - 25);
                bytes += payload.size();
                std::optional<CdcEvent> ev;
                if (impl_->opts.plugin == "test_decoding") {
                    ev = parse_test_decoding(payload, format_lsn(data_start));
                } else {
                    ev = impl_->pgoutput_state.on_message(payload, format_lsn(data_start));
                }
                if (ev.has_value()) {
                    batch.emplace(std::move(*ev));
                }
            } else if (type_byte == 'k' && n >= 18) {
                // PrimaryKeepalive: walEnd (8), sendTime (8), replyRequested (1).
                const std::uint64_t wal_end = read_be64(buffer + 1);
                impl_->received_lsn = std::max(impl_->received_lsn, wal_end);
                const bool reply_requested = buffer[17] != 0;
                if (reply_requested) {
                    if (!send_standby_status_update(impl_->conn, impl_->received_lsn, false)) {
                        // A failed feedback stalls confirmed_flush_lsn and bloats
                        // server WAL - make it observable rather than swallow it.
                        clink::metrics::connector::error_inc("postgres_cdc", "source");
                    }
                    impl_->last_status_send = std::chrono::steady_clock::now();
                }
            }
        }
        PQfreemem(buffer);
    }

    surface_decoded();
    // received_lsn doubles as a consumer-lag-proxy: how far behind WAL
    // the source is. There's no server-side current LSN cheaply
    // available from libpq's replication protocol, so we publish the
    // confirmed received LSN as the gauge and dashboards diff against
    // a separate server probe.
    clink::metrics::connector::consumer_lag_set("postgres_cdc",
                                                static_cast<std::int64_t>(impl_->received_lsn));

    // Periodic Standby Status Update - keeps the slot's confirmed_flush_lsn
    // moving forward so the server can free old WAL.
    if (impl_->opts.standby_status_interval.count() > 0) {
        const auto now = std::chrono::steady_clock::now();
        if (now - impl_->last_status_send >= impl_->opts.standby_status_interval) {
            if (!send_standby_status_update(impl_->conn, impl_->received_lsn, false)) {
                clink::metrics::connector::error_inc("postgres_cdc", "source");
            }
            impl_->last_status_send = now;
        }
    }

    std::this_thread::sleep_for(impl_->opts.poll_interval);
    return true;
}

void PostgresCdcSource::cancel() {
    Source<CdcEvent>::cancel();
    impl_->cancel.store(true);
}

std::string PostgresCdcSource::start_position() const {
    return impl_ != nullptr ? impl_->start_position_lsn : std::string{};
}

void PostgresCdcSource::close() {
    if (impl_ != nullptr && impl_->conn != nullptr) {
        PQfinish(impl_->conn);
        impl_->conn = nullptr;
        impl_->started = false;
    }
    // Auto-drop the slot if requested. Done after PQfinish so the server
    // has released the slot's session lock.
    if (impl_ != nullptr && impl_->opts.drop_slot_on_close) {
        try {
            drop_slot();
        } catch (...) {
            // Best-effort on close - swallow and let the runner's error
            // bookkeeping note any leak via metrics.
        }
    }
}

namespace {
constexpr const char* kPostgresCdcLsnKey = "__postgres_cdc_lsn__";
}  // namespace

void PostgresCdcSource::snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId) {
    const std::uint64_t cursor = impl_->received_lsn;
    std::array<std::byte, 8> bytes{};
    for (int i = 0; i < 8; ++i) {
        bytes[static_cast<std::size_t>(i)] = static_cast<std::byte>((cursor >> (i * 8)) & 0xFF);
    }
    backend.put_operator_state(
        op_id,
        StateBackend::KeyView{kPostgresCdcLsnKey, std::strlen(kPostgresCdcLsnKey)},
        StateBackend::ValueView{reinterpret_cast<const char*>(bytes.data()), bytes.size()});
}

bool PostgresCdcSource::restore_offset(StateBackend& backend, OperatorId op_id) {
    auto v = backend.get_operator_state(
        op_id, StateBackend::KeyView{kPostgresCdcLsnKey, std::strlen(kPostgresCdcLsnKey)});
    if (!v.has_value() || v->size() < 8) {
        return false;
    }
    std::uint64_t restored = 0;
    for (int i = 0; i < 8; ++i) {
        restored |= static_cast<std::uint64_t>(static_cast<std::uint8_t>((*v)[i])) << (i * 8);
    }
    impl_->received_lsn = restored;
    impl_->restored_from_checkpoint = true;
    return true;
}

void PostgresCdcSource::drop_slot() {
    PGconn* admin = PQconnectdb(impl_->opts.conninfo.c_str());
    if (PQstatus(admin) != CONNECTION_OK) {
        const std::string err = PQerrorMessage(admin);
        PQfinish(admin);
        throw std::runtime_error("PostgresCdcSource::drop_slot: connect failed: " + err);
    }

    // Postgres briefly reports the slot as "in use" right after the
    // session that held it disconnects; retry a few times before giving
    // up. This is the same pattern Debezium uses for slot teardown.
    const std::string sql = "SELECT pg_drop_replication_slot('" + impl_->opts.slot_name + "')";
    constexpr int attempts = 10;
    std::string last_err;
    for (int i = 0; i < attempts; ++i) {
        PGresult* r = PQexec(admin, sql.c_str());
        const auto status = PQresultStatus(r);
        if (status == PGRES_TUPLES_OK || status == PGRES_COMMAND_OK) {
            PQclear(r);
            PQfinish(admin);
            return;
        }
        last_err = PQerrorMessage(admin);
        PQclear(r);

        // Slot doesn't exist → treat as success (idempotent).
        if (last_err.find("does not exist") != std::string::npos) {
            PQfinish(admin);
            return;
        }
        if (last_err.find("is active") == std::string::npos) {
            // Some other error - bail out, no point retrying.
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{50});
    }

    PQfinish(admin);
    throw std::runtime_error("PostgresCdcSource::drop_slot: " + last_err);
}

#else  // !CLINK_HAS_POSTGRES

struct PostgresCdcSource::Impl {};

bool PostgresCdcSource::is_real_implementation() {
    return false;
}

PostgresCdcSource::PostgresCdcSource(Options /*opts*/) {
    throw std::runtime_error(
        "PostgresCdcSource: built without libpq. Install postgresql or libpq "
        "and reconfigure with CLINK_WITH_POSTGRES=ON.");
}

PostgresCdcSource::~PostgresCdcSource() = default;
void PostgresCdcSource::open() {}
bool PostgresCdcSource::produce(Emitter<CdcEvent>& /*out*/) {
    return false;
}
void PostgresCdcSource::cancel() {
    Source<CdcEvent>::cancel();
}
std::string PostgresCdcSource::start_position() const {
    return {};
}
void PostgresCdcSource::close() {}
void PostgresCdcSource::drop_slot() {}
void PostgresCdcSource::snapshot_offset(StateBackend&, OperatorId, CheckpointId) {}
bool PostgresCdcSource::restore_offset(StateBackend&, OperatorId) {
    return false;
}

#endif

}  // namespace clink
