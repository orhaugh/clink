// MySQL binlog CDC source: the client-library side of the stream (the
// connections, the dump session, the conversion of mariadb_rpl events for the
// processor), the strict reading of a restored checkpoint, the checks of the
// server and of the binlog coordinate it starts from, the initial snapshot, and
// the barrier hold that keeps a checkpoint off an open transaction or an
// unfinished snapshot. Event processing is BinlogEventProcessor's
// (mysql_cdc_processor.cpp); the contract is in mysql_cdc_source.hpp and
// docs/connectors/mysql.md.

#include "clink/mysql/mysql_cdc_source.hpp"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include <mariadb/mariadb_rpl.h>

#include "clink/connectors/cdc_json.hpp"
#include "clink/fault/fault_injection.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/mysql/mysql_cdc_rules.hpp"
#include "clink/mysql/mysql_snapshot.hpp"
#include "clink/runtime/log_buffer.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/state_backend.hpp"

#include "fault_points.hpp"

namespace clink::mysql {

namespace {

constexpr const char* kLabel = "mysql_cdc";
constexpr const char* kLogComponent = "mysql.cdc";
constexpr const char* kPosKey = "__mysql_cdc_pos__";
constexpr int kMaxEventsPerProduce = 2048;  // bound work per produce() call
constexpr std::size_t kMaxBatch = 1024;     // emit batch size cap

std::string upper(std::string s) {
    for (auto& c : s) {
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }
    return s;
}

std::string str_of(const MARIADB_STRING& s) {
    return s.str != nullptr ? std::string(s.str, s.length) : std::string{};
}

std::vector<std::uint8_t> bytes_of(const void* p, std::size_t n) {
    const auto* b = static_cast<const std::uint8_t*>(p);
    return p != nullptr ? std::vector<std::uint8_t>(b, b + n) : std::vector<std::uint8_t>{};
}

bool is_rows_type(int t) {
    return (t >= 23 && t <= 25) || (t >= 30 && t <= 32) || (t >= 166 && t <= 171);
}

// The parts of a mariadb_rpl event the processor reads. Row data stays in the
// event (a view, valid until the event is freed); everything else is copied.
BinlogEvent to_binlog_event(const MARIADB_RPL_EVENT* ev) {
    BinlogEvent b;
    b.type = static_cast<int>(ev->event_type);
    b.next_pos = ev->next_event_pos;
    switch (b.type) {
        case ROTATE_EVENT:
            b.rotate_file = str_of(ev->event.rotate.filename);
            b.rotate_pos = ev->event.rotate.position;
            break;
        case GTID_LOG_EVENT:
        case ANONYMOUS_GTID_LOG_EVENT:
            b.gtid_seq = static_cast<std::int64_t>(ev->event.gtid_log.sequence_nr);
            break;
        case GTID_EVENT:
            b.mariadb_gtid = true;
            b.gtid_seq = static_cast<std::int64_t>(ev->event.gtid.sequence_nr);
            b.gtid_standalone = (ev->event.gtid.flags & 1U) != 0;  // FL_STANDALONE
            break;
        case QUERY_EVENT:
            b.query_db = str_of(ev->event.query.database);
            b.query = str_of(ev->event.query.statement);
            break;
        case TABLE_MAP_EVENT: {
            const auto& tm = ev->event.table_map;
            b.table_id = tm.table_id;
            b.db = str_of(tm.database);
            b.table = str_of(tm.table);
            b.column_types = bytes_of(tm.column_types.str, tm.column_types.length);
            b.metadata = bytes_of(tm.metadata.str, tm.metadata.length);
            if (tm.column_names.data != nullptr && tm.column_names.length > 0) {
                b.column_names =
                    parse_column_names(reinterpret_cast<const std::uint8_t*>(tm.column_names.data),
                                       tm.column_names.length,
                                       tm.column_count);
            }
            if (tm.enum_values.data != nullptr) {
                b.enum_values = parse_str_value_lists(
                    reinterpret_cast<const std::uint8_t*>(tm.enum_values.data),
                    tm.enum_values.length);
            }
            if (tm.set_values.data != nullptr) {
                b.set_values =
                    parse_str_value_lists(reinterpret_cast<const std::uint8_t*>(tm.set_values.data),
                                          tm.set_values.length);
            }
            // The bitmap carries no length of its own; it lies inside the
            // event, so copy at most one bit per column, never past the event.
            if (tm.signed_indicator != nullptr && ev->raw_data != nullptr &&
                tm.signed_indicator >= ev->raw_data &&
                tm.signed_indicator < ev->raw_data + ev->raw_data_size) {
                const auto left = static_cast<std::size_t>(ev->raw_data + ev->raw_data_size -
                                                           tm.signed_indicator);
                const auto want = (static_cast<std::size_t>(tm.column_count) + 7) / 8;
                b.signedness = bytes_of(tm.signed_indicator, std::min(left, want));
            }
            break;
        }
        default:
            if (is_rows_type(b.type)) {
                const auto& r = ev->event.rows;
                b.table_id = r.table_id;
                b.column_count = r.column_count;
                const auto bitmap = (static_cast<std::size_t>(r.column_count) + 7) / 8;
                b.present_columns = bytes_of(r.column_bitmap, bitmap);
                if (is_v1_update_rows_event(b.type)) {
                    b.update_present_columns = bytes_of(r.column_update_bitmap, bitmap);
                }
                b.row_data = static_cast<const std::uint8_t*>(r.row_data);
                b.row_data_size = r.row_data_size;
            }
            break;
    }
    return b;
}

// The captured tables' definitions as information_schema gives them now, in
// the checkpoint's form ("db.table" -> schema_fingerprint). Every column in
// ordinal order, generated ones included: a row image under
// binlog_row_image=FULL carries VIRTUAL and STORED generated columns too.
std::map<std::string, std::string> read_definitions(Connection& c,
                                                    const std::vector<std::string>& tables) {
    const std::string system = "('mysql','sys','information_schema','performance_schema')";
    std::string where;
    if (tables.empty()) {
        where = "TABLE_SCHEMA NOT IN " + system;
    }
    for (const auto& t : tables) {
        where += where.empty() ? "" : " OR ";
        const auto dot = t.find('.');
        if (dot == std::string::npos) {
            where += "(TABLE_NAME='" + c.escape(t) + "' AND TABLE_SCHEMA NOT IN " + system + ")";
        } else {
            where += "(TABLE_SCHEMA='" + c.escape(t.substr(0, dot)) + "' AND TABLE_NAME='" +
                     c.escape(t.substr(dot + 1)) + "')";
        }
    }
    Result r = c.query(
        "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE FROM "
        "information_schema.COLUMNS WHERE " +
        where + " ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION");
    std::map<std::string, std::vector<ResolvedColumnSpec>> columns;
    while (MYSQL_ROW row = r.fetch_row()) {
        const auto cell = [&](int i) {
            return row[i] != nullptr ? std::string{row[i]} : std::string{};
        };
        columns[cell(0) + "." + cell(1)].push_back(
            resolve_column(InformationSchemaColumn{cell(2), cell(3), cell(4)}));
    }
    std::map<std::string, std::string> out;
    for (const auto& [key, cols] : columns) {
        auto text = schema_fingerprint(cols);
        if (text.size() > kMaxDefinitionBytes) {
            text = std::string(1, kUnknownDefinition) +
                   "its definition is longer than a checkpoint carries";
        }
        out.emplace(key, std::move(text));
    }
    return out;
}

// The server's current binlog coordinate. MySQL 8.4 removed SHOW MASTER STATUS
// for SHOW BINARY LOG STATUS (8.2 and later); earlier MySQL and MariaDB have
// only the first.
std::pair<std::string, std::uint64_t> binlog_head(Connection& c, const std::string& who) {
    std::optional<Result> r;
    try {
        r.emplace(c.query("SHOW BINARY LOG STATUS"));
    } catch (const std::runtime_error&) {
        r.emplace(c.query("SHOW MASTER STATUS"));
    }
    MYSQL_ROW row = r->fetch_row();
    if (row == nullptr || row[0] == nullptr || row[1] == nullptr) {
        throw std::runtime_error(who +
                                 ": the server reports no binlog position (is the binary log on?)");
    }
    return {row[0], std::strtoull(row[1], nullptr, 10)};
}

class MysqlCdcSource final : public Source<std::string> {
public:
    explicit MysqlCdcSource(MysqlCdcOptions opts)
        : opts_(std::move(opts)), dormant_(opts_.subtask_idx != 0) {}

    ~MysqlCdcSource() override { teardown_(); }

    void open() override {
        if (dormant_) {
            return;
        }
        schema_conn_ = std::make_unique<Connection>(opts_.conn);
        check_server_();
        processor_ = std::make_unique<BinlogEventProcessor>(
            ProcessorOptions{opts_.tables, opts_.decode_error_policy});
        if (restored_) {
            resume_from_checkpoint_();
            return;
        }
        if (opts_.enable_initial_snapshot) {
            setup_snapshot_();
            start_position_ = pos_str_();
            phase_ = Phase::Snapshot;
            return;
        }
        if (!opts_.start_file.empty()) {
            check_coordinate_(opts_.start_file, opts_.start_pos, "start_file");
            cur_file_ = opts_.start_file;
            cur_pos_ = opts_.start_pos;
            unrecorded_ = "the source started from start_file=" + pos_str_() +
                          ", and information_schema gives the table's definition today, not the "
                          "one in force there";
            begin_stream_("start_file");
            return;
        }
        definitions_around_(*schema_conn_, [&] {
            std::tie(cur_file_, cur_pos_) = binlog_head(*schema_conn_, opts_.name);
        });
        unrecorded_ = "it had no definition when the stream started at " + pos_str_() +
                      " (it did not exist then)";
        begin_stream_("the binlog head");
    }

    bool produce(Emitter<std::string>& out) override {
        if (dormant_) {
            if (this->cancelled()) {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds{200});
            return !this->cancelled();
        }
        if (this->cancelled()) {
            return false;
        }
        if (phase_ == Phase::Snapshot) {
            return produce_snapshot_(out);
        }
        if (rpl_ == nullptr) {
            return false;
        }
        Batch<std::string> batch;
        std::uint64_t bytes = 0;
        std::vector<std::string> rows;
        const auto emit = [&] {
            if (batch.empty()) {
                return;
            }
            clink::metrics::connector::records_in_inc(kLabel, batch.size());
            clink::metrics::connector::bytes_in_inc(kLabel, bytes);
            clink::metrics::connector::consumer_lag_set(
                kLabel, static_cast<std::int64_t>(processor_->pos()));
            out.emit_data(std::move(batch));
            batch = Batch<std::string>{};
            bytes = 0;
        };
        for (int events = 0; events < kMaxEventsPerProduce && !this->cancelled(); ++events) {
            MARIADB_RPL_EVENT* ev = mariadb_rpl_fetch(rpl_, nullptr);
            if (ev == nullptr) {
                if (mariadb_rpl_errno(rpl_) != 0) {
                    clink::metrics::connector::error_inc(kLabel, "source");
                    const std::string e =
                        mariadb_rpl_error(rpl_) != nullptr ? mariadb_rpl_error(rpl_) : "unknown";
                    emit();  // what came before the failure is whole events
                    throw std::runtime_error(read_failure_(e));
                }
                break;  // a clean end, only in non-blocking mode
            }
            const bool heartbeat = ev->event_type == HEARTBEAT_LOG_EVENT;
            rows.clear();
            try {
                processor_->process(to_binlog_event(ev), rows);
            } catch (...) {
                mariadb_free_rpl_event(ev);
                report_dropped_();
                clink::metrics::connector::error_inc(kLabel, "source");
                // The events before the one that failed are passed on, so the
                // source stops at the failure, not at the start of the call.
                emit();
                throw;
            }
            mariadb_free_rpl_event(ev);
            for (auto& r : rows) {
                bytes += r.size();
                batch.emplace(std::move(r));
            }
            report_dropped_();
            if (heartbeat || batch.size() >= kMaxBatch) {
                break;  // idle, or a full batch: return promptly so barriers and cancel are seen
            }
        }
        const bool emitted = !batch.empty();
        emit();
        if (emitted && !processor_->between_transactions()) {
            (void)CLINK_FAULT_POINT(points::kInTransactionAfterEmit);
        }
        return !this->cancelled();
    }

    // A barrier passes only between transactions and never during the snapshot,
    // so a checkpoint covers whole transactions and either all of the snapshot
    // or none of it.
    [[nodiscard]] bool barrier_ready() const override {
        return dormant_ ||
               (phase_ == Phase::Stream && processor_ && processor_->between_transactions());
    }

    void close() override { teardown_(); }

    [[nodiscard]] bool is_bounded() const noexcept override { return false; }

    std::string name() const override { return opts_.name; }

    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId /*ckpt*/) override {
        if (dormant_ || !processor_) {
            return;
        }
        MysqlCdcCheckpoint cp;
        cp.server_uuid = server_uuid_;
        if (phase_ == Phase::Snapshot) {
            // Never reached while barriers are held through the snapshot; if it
            // were, the row says so and a restore refuses it.
            cp.snapshot_unfinished = true;
            cp.file = cur_file_;
            cp.pos = cur_pos_;
            cp.schemas = definitions_;
        } else {
            cp.file = processor_->committed_file();
            cp.pos = processor_->committed_pos();
            cp.schemas = processor_->definitions();
        }
        const auto blob = encode_mysql_cdc_checkpoint(cp);
        backend.put_operator_state(
            op_id, std::string(kPosKey), StateBackend::ValueView{blob.data(), blob.size()});
    }

    bool restore_offset(StateBackend& backend, OperatorId op_id) override {
        if (dormant_) {
            return false;
        }
        bool found = false;
        std::string corrupt;
        backend.scan_operator_state(
            op_id, [&](StateBackend::KeyView key, StateBackend::ValueView value) {
                if (std::string_view(key) != std::string_view(kPosKey)) {
                    return;
                }
                found = true;
                try {
                    restored_cp_ =
                        decode_mysql_cdc_checkpoint(std::string_view{value.data(), value.size()});
                } catch (const MysqlCdcCheckpointError& e) {
                    corrupt = e.what();
                }
            });
        if (!corrupt.empty()) {
            throw std::runtime_error(
                opts_.name + ": the restored checkpoint's binlog position is corrupt (" + corrupt +
                "). The source does not guess where the stream was, and never starts from the "
                "binlog head in its place: restore another checkpoint, or start the job afresh");
        }
        restored_ = found;
        return found;
    }

    [[nodiscard]] std::string start_position() const { return start_position_; }

private:
    enum class Phase { Snapshot, Stream };

    void teardown_() {
        if (rpl_ != nullptr) {
            mariadb_rpl_close(rpl_);
            rpl_ = nullptr;
        }
        release_snapshot_();
        stream_conn_.reset();
        schema_conn_.reset();
    }

    // Closing the snapshot connection ends, on the server, the global read lock
    // and the consistent-snapshot transaction it may hold. The result goes
    // first: its mysql_free_result must precede mysql_close.
    void release_snapshot_() {
        cur_snap_result_.reset();
        snapshot_conn_.reset();
    }

    std::string pos_str_() const { return cur_file_ + ":" + std::to_string(cur_pos_); }

    void check_server_() {
        {
            // The change stream is verified against MySQL's binlog only. A
            // MariaDB server writes different rows, GTID and compressed events,
            // and is refused rather than streamed on unverified decoding.
            Result r = schema_conn_->query("SELECT VERSION()");
            MYSQL_ROW row = r.fetch_row();
            const std::string version = row != nullptr && row[0] != nullptr ? row[0] : "";
            if (upper(version).find("MARIADB") != std::string::npos) {
                throw std::runtime_error(
                    opts_.name + ": the server is MariaDB (" + version +
                    "); the change stream supports MySQL servers only (the client library being "
                    "mariadb-connector-c does not make a MariaDB server's binlog supported)");
            }
        }
        {
            Result r =
                schema_conn_->query("SELECT @@global.binlog_format, @@global.binlog_row_image");
            MYSQL_ROW row = r.fetch_row();
            const std::string format = row != nullptr && row[0] != nullptr ? upper(row[0]) : "";
            const std::string image = row != nullptr && row[1] != nullptr ? upper(row[1]) : "";
            if (format != "ROW" || image != "FULL") {
                throw std::runtime_error(
                    opts_.name + ": the server writes binlog_format=" + format +
                    " and binlog_row_image=" + image +
                    "; the source needs ROW and FULL, so that every change is in the binlog as "
                    "whole rows");
            }
        }
        try {
            Result r = schema_conn_->query("SELECT @@server_uuid");
            if (MYSQL_ROW row = r.fetch_row(); row != nullptr && row[0] != nullptr) {
                server_uuid_ = row[0];
            }
        } catch (const std::runtime_error&) {
            server_uuid_.clear();  // MariaDB has no server_uuid
        }
        try {
            Result r = schema_conn_->query("SELECT @@global.binlog_row_metadata");
            if (MYSQL_ROW row = r.fetch_row(); row != nullptr && row[0] != nullptr) {
                row_metadata_ = upper(row[0]);
            }
        } catch (const std::runtime_error&) {
            row_metadata_ = "(not supported)";
        }
    }

    // A coordinate must name a binlog file the server still has, at a position
    // inside it.
    void check_coordinate_(const std::string& file, std::uint64_t pos, const std::string& what) {
        Result r = schema_conn_->query("SHOW BINARY LOGS");
        while (MYSQL_ROW row = r.fetch_row()) {
            if (row[0] == nullptr || file != row[0]) {
                continue;
            }
            const auto size = row[1] != nullptr ? std::strtoull(row[1], nullptr, 10) : 0ULL;
            if (pos < 4 || pos > size) {
                throw std::runtime_error(opts_.name + ": " + what + " names position " +
                                         std::to_string(pos) + " in binlog file " + file +
                                         ", which holds " + std::to_string(size) +
                                         " bytes; the source does not start at a position the "
                                         "file does not have");
            }
            return;
        }
        throw std::runtime_error(
            opts_.name + ": " + what + " names binlog file " + file +
            ", which the server no longer has (purged, or another server's binlog), so the changes "
            "after it cannot be read. The source does not start; restart it from a new snapshot");
    }

    void resume_from_checkpoint_() {
        const auto& cp = restored_cp_;
        if (cp.snapshot_unfinished) {
            throw std::runtime_error(
                opts_.name +
                ": the restored checkpoint was taken during the initial snapshot, so the state "
                "restored with it holds part of the snapshot and the source cannot continue it. "
                "Start the job afresh");
        }
        if (!cp.server_uuid.empty() && cp.server_uuid != server_uuid_) {
            throw std::runtime_error(
                opts_.name + ": the restored checkpoint's binlog coordinate belongs to server " +
                cp.server_uuid + ", and this server is " +
                (server_uuid_.empty() ? std::string{"one without a server_uuid"} : server_uuid_) +
                ". A binlog coordinate names a position only on the server that wrote it (after a "
                "failover, restart the source from a new snapshot)");
        }
        check_coordinate_(cp.file, cp.pos, "the restored checkpoint");
        cur_file_ = cp.file;
        cur_pos_ = cp.pos;
        if (cp.version == 0) {
            definitions_ = read_definitions(*schema_conn_, opts_.tables);
            check_definition_count_();
            unrecorded_ = "it had no definition when the source restored the checkpoint";
            clink::log::warn(
                kLogComponent,
                opts_.name +
                    ": the restored checkpoint was written by an earlier release and "
                    "records no table definitions, so the source takes today's from "
                    "information_schema as those in force at " +
                    pos_str_() +
                    ", as earlier releases did; a change to a captured table's columns made "
                    "since that checkpoint goes unnoticed for events without column names "
                    "(binlog_row_metadata=MINIMAL). Every checkpoint from now on records them");
        } else {
            definitions_ = cp.schemas;
            unrecorded_ =
                "the restored checkpoint records no definition for it (it was not "
                "captured, or did not exist, when the checkpoint was taken)";
        }
        begin_stream_("the restored checkpoint");
    }

    // Reads the captured tables' definitions on both sides of `mark`, which
    // reads the coordinate the stream starts from: when the two reads agree, no
    // DDL statement changed them in between, so they are the definitions in
    // force at that coordinate.
    template <typename Mark>
    void definitions_around_(Connection& c, Mark&& mark) {
        constexpr int kAttempts = 5;
        for (int attempt = 0; attempt < kAttempts; ++attempt) {
            auto before = read_definitions(c, opts_.tables);
            mark();
            auto after = read_definitions(c, opts_.tables);
            if (before == after) {
                definitions_ = std::move(after);
                check_definition_count_();
                return;
            }
        }
        throw std::runtime_error(
            opts_.name +
            ": the captured tables' definitions changed while the source read the "
            "coordinate it starts from, " +
            std::to_string(kAttempts) +
            " times running. DDL is running against them: start the source when it has finished");
    }

    void check_definition_count_() const {
        if (definitions_.size() > kMaxCheckpointDefinitions) {
            throw std::runtime_error(
                opts_.name + ": the allowlist covers " + std::to_string(definitions_.size()) +
                " tables, more than the " + std::to_string(kMaxCheckpointDefinitions) +
                " whose definitions a checkpoint carries; narrow 'tables'");
        }
    }

    void begin_stream_(const std::string& from) {
        processor_->start(cur_file_, cur_pos_, definitions_, unrecorded_);
        start_position_ = pos_str_();
        open_stream_();
        phase_ = Phase::Stream;
        clink::log::info(
            kLogComponent,
            opts_.name + ": streaming from " + start_position_ + " (" + from + "), server " +
                (server_uuid_.empty() ? "(no uuid)" : server_uuid_) +
                ", binlog_row_metadata=" + row_metadata_ + ", on_decode_error=" +
                (opts_.decode_error_policy == MysqlDecodeErrorPolicy::Drop ? "drop" : "fail"));
        if (row_metadata_ != "FULL") {
            clink::log::warn(
                kLogComponent,
                opts_.name +
                    ": the server's TABLE_MAPs carry no column names (binlog_row_metadata=" +
                    row_metadata_ +
                    "), so names come from information_schema and a change to a captured table's "
                    "columns stops the source; with binlog_row_metadata=FULL each event is decoded "
                    "with its own definition");
        }
    }

    // Open the binlog dump stream at (cur_file_, cur_pos_), with the master's
    // checksum setting and the heartbeat period on the dump session.
    void open_stream_() {
        stream_conn_ = std::make_unique<Connection>(opts_.conn);
        std::string checksum = "NONE";
        {
            Result r = stream_conn_->query("SELECT @@global.binlog_checksum");
            if (MYSQL_ROW row = r.fetch_row(); row != nullptr && row[0] != nullptr) {
                checksum = row[0];
            }
        }
        stream_conn_->exec("SET @master_binlog_checksum = '" + checksum + "'");
        const long long hb_ns = (opts_.heartbeat_ms > 0 ? opts_.heartbeat_ms : 1000) * 1000000LL;
        stream_conn_->exec("SET @master_heartbeat_period = " + std::to_string(hb_ns));

        rpl_ = mariadb_rpl_init(stream_conn_->native());
        if (rpl_ == nullptr) {
            throw std::runtime_error(opts_.name + ": mariadb_rpl_init failed");
        }
        mariadb_rpl_optionsv(
            rpl_, MARIADB_RPL_SERVER_ID, static_cast<unsigned int>(opts_.server_id));
        if (!cur_file_.empty()) {
            mariadb_rpl_optionsv(rpl_,
                                 MARIADB_RPL_FILENAME,
                                 cur_file_.c_str(),
                                 static_cast<std::size_t>(cur_file_.size()));
        }
        mariadb_rpl_optionsv(rpl_, MARIADB_RPL_START, static_cast<unsigned long>(cur_pos_));
        mariadb_rpl_optionsv(rpl_, MARIADB_RPL_FLAGS, static_cast<unsigned int>(0));  // blocking
        mariadb_rpl_optionsv(rpl_, MARIADB_RPL_VERIFY_CHECKSUM, static_cast<unsigned int>(1));
        // MARIADB_RPL_EXTRACT_VALUES stays off: the library's value decoder
        // garbles most MySQL types, and the processor decodes the raw row image
        // itself (mysql_row_decode.hpp).
        if (mariadb_rpl_open(rpl_) != 0) {
            const std::string e =
                mariadb_rpl_error(rpl_) != nullptr ? mariadb_rpl_error(rpl_) : "unknown";
            throw std::runtime_error(opts_.name + ": mariadb_rpl_open failed: " + e);
        }
    }

    // The client library's read error, with what the source knows of it. The
    // library refuses an event type it does not know whose header says replicas
    // may not ignore it (a compressed transaction, a partial JSON update), so
    // such an event fails the source under either decode policy.
    std::string read_failure_(const std::string& library_error) const {
        std::string msg = opts_.name + ": binlog read failed after " + processor_->file() + ":" +
                          std::to_string(processor_->pos()) + ": " + library_error;
        static constexpr std::string_view kUnknown = "Unknown event type (";
        if (const auto at = library_error.find(kUnknown); at != std::string::npos) {
            const int type = std::atoi(library_error.c_str() + at + kUnknown.size());
            std::string why;
            if (classify_binlog_event(type, &why) == BinlogEventClass::Unsupported) {
                msg += ". The event is " + why +
                       "; the client library cannot pass it on, so the source stops whatever "
                       "on_decode_error says";
            }
        }
        return msg;
    }

    void report_dropped_() {
        for (auto& d : processor_->take_dropped()) {
            ++dropped_;
            clink::metrics::connector::dropped_events_inc(kLabel, 1);
            const std::string why = std::string{decode_fault_name(d.fault)} + ": " + d.why;
            if (auto* rt = this->runtime(); rt != nullptr) {
                rt->report_bad_record(clink::BadRecord{
                    .payload = std::move(d.payload),
                    .error = why,
                    .connector = kLabel,
                    .direction = "source",
                    .location = d.table.empty() ? d.location : d.table + "@" + d.location});
            }
            if ((dropped_ & (dropped_ - 1)) == 0) {
                clink::log::warn(kLogComponent,
                                 opts_.name + ": dropped " + std::to_string(dropped_) +
                                     " event(s) under on_decode_error='drop', losing their "
                                     "changes; last at " +
                                     d.location + (d.table.empty() ? "" : " in " + d.table) + ": " +
                                     why);
            }
        }
    }

    // ---- the initial snapshot ----------------------------------------------------------

    // Establishes the snapshot point P and the read view the scans use. With the
    // lock, FLUSH TABLES WITH READ LOCK freezes writers while P is read and the
    // consistent-snapshot transaction opens, so both describe one point; the
    // lock is released at once. Without it, P is read first and the view opens
    // after, at or past P. Any failure closes the connection, which ends the
    // lock and the transaction on the server.
    void setup_snapshot_() {
        for (const auto& entry : opts_.tables) {
            snapshot_queue_.push_back(qualify_table(entry, opts_.conn.database));
        }
        snapshot_conn_ = std::make_unique<Connection>(opts_.conn);
        try {
            for (const auto& t : snapshot_queue_) {
                check_snapshot_table_(t);
            }
            snapshot_conn_->exec("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            if (opts_.snapshot_lock) {
                snapshot_conn_->exec("SET SESSION lock_wait_timeout = " +
                                     std::to_string(opts_.snapshot_lock_wait_s));
                try {
                    snapshot_conn_->exec("FLUSH TABLES WITH READ LOCK");
                } catch (const std::runtime_error& e) {
                    throw std::runtime_error(
                        opts_.name +
                        ": the snapshot could not take FLUSH TABLES WITH READ LOCK "
                        "within snapshot_lock_wait_s=" +
                        std::to_string(opts_.snapshot_lock_wait_s) +
                        " (a long-running statement or a table lock holds it; while it waits it "
                        "blocks the server's writers, so it gives up): " +
                        e.what());
                }
                // The lock holds DDL off too, so one read of the definitions is
                // the definitions in force at P.
                std::tie(cur_file_, cur_pos_) = binlog_head(*snapshot_conn_, opts_.name);
                definitions_ = read_definitions(*snapshot_conn_, opts_.tables);
                check_definition_count_();
                snapshot_conn_->exec("START TRANSACTION WITH CONSISTENT SNAPSHOT");
                snapshot_conn_->exec("UNLOCK TABLES");
            } else {
                // No global lock, but a read lock on the snapshot tables. MySQL
                // writes a transaction to the binlog before it commits in InnoDB,
                // so without a lock P can include a change the view opened next
                // does not show, which neither the snapshot nor the stream would
                // carry. LOCK TABLES ... READ waits until every transaction that
                // changed one of them has committed, and holds new ones (and DDL)
                // off while P and the definitions are read. START TRANSACTION
                // releases it; a change committed between that and the view is
                // in both, the boundary's only duplicates.
                std::string lock = "LOCK TABLES ";
                for (std::size_t i = 0; i < snapshot_queue_.size(); ++i) {
                    lock += (i > 0 ? ", " : "") + quote_ident(snapshot_queue_[i].db) + "." +
                            quote_ident(snapshot_queue_[i].table) + " READ";
                }
                snapshot_conn_->exec("SET SESSION lock_wait_timeout = " +
                                     std::to_string(opts_.snapshot_lock_wait_s));
                try {
                    snapshot_conn_->exec(lock);
                } catch (const std::runtime_error& e) {
                    throw std::runtime_error(
                        opts_.name +
                        ": the snapshot could not take LOCK TABLES ... READ on its tables within "
                        "snapshot_lock_wait_s=" +
                        std::to_string(opts_.snapshot_lock_wait_s) +
                        " (snapshot_lock='false' needs the LOCK TABLES privilege on them, and "
                        "waits for transactions that changed them to commit): " +
                        e.what());
                }
                std::tie(cur_file_, cur_pos_) = binlog_head(*snapshot_conn_, opts_.name);
                definitions_ = read_definitions(*snapshot_conn_, opts_.tables);
                check_definition_count_();
                snapshot_conn_->exec("START TRANSACTION WITH CONSISTENT SNAPSHOT");
            }
            unrecorded_ = "it had no definition at the snapshot point " + pos_str_() +
                          " (it did not exist then)";
        } catch (...) {
            release_snapshot_();
            throw;
        }
        clink::log::info(kLogComponent,
                         opts_.name + ": initial snapshot of " +
                             std::to_string(snapshot_queue_.size()) + " table(s) as of " +
                             pos_str_() + (opts_.snapshot_lock ? " (locked)" : " (unlocked)"));
    }

    // A snapshot table must exist and be InnoDB: no other engine gives a
    // consistent-snapshot read view.
    void check_snapshot_table_(const QualifiedTable& t) {
        Result r = snapshot_conn_->query(
            "SELECT ENGINE, TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA='" +
            snapshot_conn_->escape(t.db) + "' AND TABLE_NAME='" + snapshot_conn_->escape(t.table) +
            "'");
        MYSQL_ROW row = r.fetch_row();
        const std::string qualified = t.db + "." + t.table;
        if (row == nullptr) {
            throw std::runtime_error(opts_.name + ": snapshot table " + qualified +
                                     " does not exist");
        }
        const std::string engine = row[0] != nullptr ? row[0] : "";
        if (upper(engine) != "INNODB") {
            throw std::runtime_error(opts_.name + ": snapshot table " + qualified + " is " +
                                     (engine.empty() ? std::string{"a view or has no engine"}
                                                     : "a " + engine + " table") +
                                     "; only InnoDB tables have the consistent-snapshot view the "
                                     "snapshot reads through, "
                                     "so leave it out of 'tables' or convert it");
        }
    }

    // Emits one chunk of snapshot rows; when every table is read, commits the
    // snapshot transaction and opens the stream at P.
    bool produce_snapshot_(Emitter<std::string>& out) {
        Batch<std::string> batch;
        std::uint64_t bytes = 0;
        try {
            const std::string lsn = pos_str_();
            for (int rows_this_call = 0; rows_this_call < kMaxEventsPerProduce;) {
                if (this->cancelled()) {
                    break;
                }
                if (!cur_snap_result_) {
                    if (snapshot_queue_.empty()) {
                        finish_snapshot_();
                        break;
                    }
                    start_next_snapshot_table_();
                    continue;
                }
                MYSQL_ROW row = cur_snap_result_.fetch_row();
                if (row == nullptr) {
                    // The table's end - or a mid-stream error, which use_result
                    // surfaces here.
                    if (snapshot_conn_->last_errno() != 0) {
                        throw std::runtime_error(opts_.name + ": snapshot scan of " +
                                                 cur_snap_table_ +
                                                 " failed: " + snapshot_conn_->last_error());
                    }
                    cur_snap_result_.reset();
                    cur_snap_fields_.clear();
                    continue;
                }
                const unsigned long* lens = cur_snap_result_.lengths();
                std::vector<std::optional<std::string>> cells;
                cells.reserve(cur_snap_fields_.size());
                for (std::size_t i = 0; i < cur_snap_fields_.size(); ++i) {
                    if (row[i] == nullptr) {
                        cells.emplace_back(std::nullopt);
                    } else {
                        cells.emplace_back(std::string(row[i], lens != nullptr ? lens[i] : 0));
                    }
                }
                CdcEvent ev = snapshot_row_to_event(cur_snap_table_, lsn, cur_snap_fields_, cells);
                if (auto js = clink::cdc::cdc_event_to_json_row(ev)) {
                    bytes += js->size();
                    batch.emplace(std::move(*js));
                }
                ++rows_this_call;
                ++snapshot_rows_;
                if (batch.size() >= kMaxBatch) {
                    break;
                }
            }
        } catch (...) {
            clink::metrics::connector::error_inc(kLabel, "source");
            release_snapshot_();
            throw;
        }
        if (!batch.empty()) {
            clink::metrics::connector::records_in_inc(kLabel, batch.size());
            clink::metrics::connector::bytes_in_inc(kLabel, bytes);
            out.emit_data(std::move(batch));
            if (phase_ == Phase::Snapshot) {
                (void)CLINK_FAULT_POINT(points::kSnapshotAfterEmit);
            }
        }
        return !this->cancelled();
    }

    void start_next_snapshot_table_() {
        const QualifiedTable t = snapshot_queue_.front();
        snapshot_queue_.pop_front();
        cur_snap_table_ = t.db + "." + t.table;
        cur_snap_result_ = snapshot_conn_->query_unbuffered(build_snapshot_select(t));
        cur_snap_fields_.clear();
        MYSQL_FIELD* fields = cur_snap_result_.fields();
        const unsigned int n = cur_snap_result_.num_fields();
        cur_snap_fields_.reserve(n);
        for (unsigned int i = 0; i < n; ++i) {
            cur_snap_fields_.emplace_back(fields[i].name != nullptr ? fields[i].name : "");
        }
        ++snapshot_tables_;
    }

    void finish_snapshot_() {
        cur_snap_result_.reset();
        if (snapshot_conn_) {
            snapshot_conn_->exec("COMMIT");
            snapshot_conn_.reset();
        }
        clink::log::info(kLogComponent,
                         opts_.name + ": initial snapshot finished, " +
                             std::to_string(snapshot_rows_) + " row(s) from " +
                             std::to_string(snapshot_tables_) + " table(s)");
        begin_stream_("the snapshot point");
    }

    MysqlCdcOptions opts_;
    bool dormant_{false};
    Phase phase_{Phase::Stream};
    std::unique_ptr<Connection> schema_conn_;  // information_schema lookups and server checks
    std::unique_ptr<Connection> stream_conn_;  // the binlog dump session
    std::unique_ptr<BinlogEventProcessor> processor_;
    MARIADB_RPL* rpl_{nullptr};

    std::unique_ptr<Connection> snapshot_conn_;
    std::deque<QualifiedTable> snapshot_queue_;
    Result cur_snap_result_;
    std::string cur_snap_table_;
    std::vector<std::string> cur_snap_fields_;
    std::uint64_t snapshot_rows_{0};
    std::uint64_t snapshot_tables_{0};

    std::string cur_file_;  // the coordinate the stream opens at
    std::uint64_t cur_pos_{4};
    std::string server_uuid_;
    std::string row_metadata_;
    bool restored_{false};
    MysqlCdcCheckpoint restored_cp_;
    std::string start_position_;
    // The definitions in force at the coordinate the stream starts from, and
    // why a captured table they do not list has none.
    std::map<std::string, std::string> definitions_;
    std::string unrecorded_;
    std::uint64_t dropped_{0};
};

}  // namespace

std::shared_ptr<Source<std::string>> make_mysql_cdc_source(const MysqlCdcOptions& opts) {
    if (opts.server_id == 0) {
        throw std::runtime_error(opts.name + ": 'server_id' is required and must be non-zero");
    }
    if (!opts.start_file.empty() && !valid_binlog_file_name(opts.start_file)) {
        throw std::runtime_error(opts.name + ": 'start_file' is not a binlog file name");
    }
    if (opts.start_pos < 4) {
        throw std::runtime_error(opts.name + ": 'start_pos' must be at least 4");
    }
    if (opts.snapshot_lock_wait_s < 1 || opts.snapshot_lock_wait_s > 3600) {
        throw std::runtime_error(opts.name + ": 'snapshot_lock_wait_s' must be 1 to 3600");
    }
    if (opts.heartbeat_ms < 1 || opts.heartbeat_ms > 3600000) {
        throw std::runtime_error(opts.name + ": 'heartbeat_ms' must be 1 to 3600000");
    }
    if (opts.enable_initial_snapshot) {
        // The snapshot scans an explicit table list; "all tables" would mean
        // reading the whole server, system tables included.
        if (opts.tables.empty()) {
            throw std::runtime_error(
                opts.name + ": enable_initial_snapshot requires a non-empty 'tables' list");
        }
        // Fully-qualified db.table entries: the stream matches a bare name in
        // every database, but the snapshot scans one table, so a bare name would
        // stream same-named tables whose rows the snapshot never read.
        for (const auto& t : opts.tables) {
            if (t.find('.') == std::string::npos) {
                throw std::runtime_error(
                    opts.name +
                    ": enable_initial_snapshot requires fully-qualified 'db.table' "
                    "entries so the snapshot and the change stream cover the same "
                    "tables (got bare '" +
                    t + "')");
            }
        }
    }
    return std::make_shared<MysqlCdcSource>(opts);
}

}  // namespace clink::mysql
