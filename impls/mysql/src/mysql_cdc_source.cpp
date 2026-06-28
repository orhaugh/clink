// MySQL binlog CDC source implementation. Streams the master's binary log via
// the mariadb-connector-c replication API and emits each row change as a flat
// JSON object string (clink::cdc::cdc_event_to_json_row), mirroring the Postgres
// CDC source's contract.
//
// DELIVERY = AT-LEAST-ONCE. The checkpoint cursor is (binlog_file, position),
// persisted on snapshot_offset and restored before open(). On restart the stream
// resumes from the checkpointed coordinate; records between the last checkpoint
// and a crash replay (the downstream changelog/2PC sink reconciles duplicates).
//
// CAVEATS (honest):
//  - An undecodable row event (no table-map seen for its table_id, or a column
//    count that disagrees with the resolved schema) is DROPPED + counted
//    (dropped_events_total + errors_total), never emitted half-populated -> that
//    one event is at-most-once. Same class as the Postgres CDC poison drop.
//  - The CHECKPOINT position is transaction-boundary-aligned (advanced only on
//    XID / ROTATE / heartbeat), so a resume always lands at a transaction start
//    whose TABLE_MAP is re-streamed - never inside a TABLE_MAP/rows pair (which
//    would orphan the map and drop rows). In-flight-transaction rows replay on
//    restart (at-least-once); the downstream sink dedupes.
//  - Column names + signedness come from information_schema at the CURRENT table
//    definition, so a DDL between the event and the lookup can mislabel columns
//    (a column-count change is caught and dropped; a rename within the same count
//    is not). MySQL DDL-tracking from the binlog is a future enhancement.
//  - Requires binlog_format=ROW + binlog_row_image=FULL on the master and
//    REPLICATION SLAVE/CLIENT on the user. Resume is file+position, not GTID.

#include "clink/mysql/mysql_cdc_source.hpp"

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <mariadb/mariadb_rpl.h>

#include "clink/connectors/cdc_json.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/mysql/mysql_row_decode.hpp"
#include "clink/mysql/mysql_snapshot.hpp"
#include "clink/state/state_backend.hpp"

namespace clink::mysql {

namespace {

constexpr const char* kLabel = "mysql_cdc";
constexpr const char* kPosKey = "__mysql_cdc_pos__";
constexpr int kMaxEventsPerProduce = 2048;  // bound work per produce() call
constexpr std::size_t kMaxBatch = 1024;     // emit batch size cap

bool is_rows_event(enum mariadb_rpl_event t) {
    switch (t) {
        case WRITE_ROWS_EVENT:
        case WRITE_ROWS_EVENT_V1:
        case UPDATE_ROWS_EVENT:
        case UPDATE_ROWS_EVENT_V1:
        case DELETE_ROWS_EVENT:
        case DELETE_ROWS_EVENT_V1:
            return true;
        default:
            return false;
    }
}

void put_u32(std::string& b, std::uint32_t v) {
    for (int i = 0; i < 4; ++i) {
        b.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
    }
}
void put_u64(std::string& b, std::uint64_t v) {
    for (int i = 0; i < 8; ++i) {
        b.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
    }
}
std::uint32_t get_u32(const unsigned char* p) {
    std::uint32_t v = 0;
    for (int i = 0; i < 4; ++i) {
        v |= static_cast<std::uint32_t>(p[i]) << (8 * i);
    }
    return v;
}
std::uint64_t get_u64(const unsigned char* p) {
    std::uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v |= static_cast<std::uint64_t>(p[i]) << (8 * i);
    }
    return v;
}

// Hex-encode a raw binlog row image for the DLQ payload (the bytes are binary, so
// hex keeps them legible in the log and reversible for a sink-backed DLQ).
std::string to_hex(const std::uint8_t* p, std::size_t n) {
    static const char* k = "0123456789abcdef";
    std::string out;
    out.reserve(n * 2);
    for (std::size_t i = 0; i < n; ++i) {
        out.push_back(k[p[i] >> 4]);
        out.push_back(k[p[i] & 0x0F]);
    }
    return out;
}

// Parse the member list out of an ENUM/SET column's information_schema
// COLUMN_TYPE, e.g. "enum('active','closed')" -> {"active","closed"}. MySQL
// doubles an embedded quote (''), which we collapse. Returns {} for non-enum/set.
std::vector<std::string> parse_enum_set_labels(const std::string& coltype) {
    std::vector<std::string> labels;
    const std::size_t lp = coltype.find('(');
    if (lp == std::string::npos) {
        return labels;
    }
    std::size_t i = lp + 1;
    while (i < coltype.size()) {
        if (coltype[i] == ')') {
            break;
        }
        if (coltype[i] != '\'') {
            ++i;  // skip separators / whitespace between members
            continue;
        }
        ++i;  // past the opening quote
        std::string label;
        while (i < coltype.size()) {
            if (coltype[i] == '\'') {
                if (i + 1 < coltype.size() && coltype[i + 1] == '\'') {
                    label.push_back('\'');  // doubled quote -> literal '
                    i += 2;
                    continue;
                }
                ++i;  // closing quote
                break;
            }
            label.push_back(coltype[i]);
            ++i;
        }
        labels.push_back(std::move(label));
    }
    return labels;
}

// Per-captured-table state: the binlog column metadata (parsed + copied out of
// the TABLE_MAP, so the rpl event need not be retained) + the resolved column
// schema. `filtered` marks a table excluded by the allowlist (its row events are
// skipped, not dropped).
struct TableState {
    std::vector<ColumnMeta> metas;
    CdcTableSchema schema;
    bool filtered{false};
};

class MysqlCdcSource final : public Source<std::string> {
public:
    explicit MysqlCdcSource(MysqlCdcOptions opts)
        : opts_(std::move(opts)), dormant_(opts_.subtask_idx != 0) {
        for (const auto& t : opts_.tables) {
            tables_filter_.insert(t);
        }
    }

    ~MysqlCdcSource() override { teardown_(); }

    void open() override {
        if (dormant_) {
            return;
        }
        // First start with a snapshot requested: capture a consistent point + read
        // the existing rows before streaming. committed_* is deliberately NOT set
        // here (snapshot_offset is a no-op until streaming begins, so a crash during
        // the snapshot re-snapshots from scratch rather than resuming mid-way).
        if (!restored_ && opts_.enable_initial_snapshot) {
            setup_snapshot_();
            start_position_ = pos_str_();
            phase_ = Phase::Snapshot;
            return;
        }
        stream_conn_ = std::make_unique<Connection>(opts_.conn);  // needed to resolve the head
        resolve_start_position_();
        committed_file_ = cur_file_;  // the start position is itself a txn boundary
        committed_pos_ = cur_pos_;
        start_position_ = pos_str_();
        open_stream_();
        phase_ = Phase::Stream;
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
        int events_this_call = 0;
        while (events_this_call < kMaxEventsPerProduce) {
            if (this->cancelled()) {
                break;
            }
            MARIADB_RPL_EVENT* ev = mariadb_rpl_fetch(rpl_, nullptr);
            if (ev == nullptr) {
                if (mariadb_rpl_errno(rpl_) != 0) {
                    clink::metrics::connector::error_inc(kLabel, "source");
                    const std::string e =
                        mariadb_rpl_error(rpl_) ? mariadb_rpl_error(rpl_) : "unknown";
                    throw std::runtime_error(opts_.name + ": binlog fetch failed: " + e);
                }
                break;  // clean EOF (only in non-blocking mode; not expected here)
            }
            ++events_this_call;
            if (ev->next_event_pos > 0) {
                cur_pos_ = ev->next_event_pos;
            }
            const enum mariadb_rpl_event t = ev->event_type;
            bool idle = false;
            if (t == ROTATE_EVENT) {
                if (ev->event.rotate.filename.str != nullptr &&
                    ev->event.rotate.filename.length > 0) {
                    cur_file_.assign(ev->event.rotate.filename.str,
                                     ev->event.rotate.filename.length);
                }
                if (ev->event.rotate.position > 0) {
                    cur_pos_ = ev->event.rotate.position;
                }
                mark_commit_boundary_();  // file rotation is between transactions
                mariadb_free_rpl_event(ev);
            } else if (t == TABLE_MAP_EVENT) {
                cache_table_map_(ev);  // copies metadata out and frees ev itself
            } else if (t == GTID_LOG_EVENT) {
                cur_xid_ = static_cast<std::int64_t>(ev->event.gtid_log.sequence_nr);
                mariadb_free_rpl_event(ev);
            } else if (t == GTID_EVENT) {
                cur_xid_ = static_cast<std::int64_t>(ev->event.gtid.sequence_nr);
                mariadb_free_rpl_event(ev);
            } else if (is_rows_event(t)) {
                handle_rows_(ev, batch, bytes);
                mariadb_free_rpl_event(ev);
            } else if (t == XID_EVENT) {
                mark_commit_boundary_();  // transaction commit: safe checkpoint point
                mariadb_free_rpl_event(ev);
            } else if (t == HEARTBEAT_LOG_EVENT) {
                mark_commit_boundary_();  // master idle: all sent events are complete txns
                mariadb_free_rpl_event(ev);
                idle = true;  // idle signal: return promptly so cancel is observed
            } else {
                mariadb_free_rpl_event(ev);
            }
            if (idle || batch.size() >= kMaxBatch) {
                break;
            }
        }

        if (dropped_ > last_dropped_) {
            const std::uint64_t delta = dropped_ - last_dropped_;
            last_dropped_ = dropped_;
            clink::metrics::connector::dropped_events_inc(kLabel, delta);
            clink::metrics::connector::error_inc(kLabel, "source");
        }
        if (!batch.empty()) {
            clink::metrics::connector::records_in_inc(kLabel, batch.size());
            clink::metrics::connector::bytes_in_inc(kLabel, bytes);
            clink::metrics::connector::consumer_lag_set(kLabel,
                                                        static_cast<std::int64_t>(cur_pos_));
            out.emit_data(std::move(batch));
        }
        return !this->cancelled();
    }

    void cancel() override { Source<std::string>::cancel(); }

    void close() override { teardown_(); }

    [[nodiscard]] bool is_bounded() const noexcept override { return false; }

    std::string name() const override { return opts_.name; }

    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId /*ckpt*/) override {
        if (dormant_) {
            return;
        }
        // During the initial snapshot, persist NOTHING: the snapshot is not
        // resumable mid-way, so a crash before it completes must re-snapshot from
        // scratch (at-least-once) rather than resume streaming and skip the rest of
        // the existing rows. committed_* is only meaningful once streaming begins.
        if (phase_ == Phase::Snapshot) {
            return;
        }
        // Checkpoint the last TRANSACTION-BOUNDARY position, never the raw
        // received position: resuming mid-pair (after a TABLE_MAP, before its rows)
        // would orphan the map and silently drop those rows. committed_* only
        // advances on XID / ROTATE / heartbeat, all guaranteed inter-transaction.
        std::string blob;
        put_u32(blob, static_cast<std::uint32_t>(committed_file_.size()));
        blob.append(committed_file_);
        put_u64(blob, committed_pos_);
        backend.put_operator_state(
            op_id, std::string(kPosKey), StateBackend::ValueView{blob.data(), blob.size()});
    }

    bool restore_offset(StateBackend& backend, OperatorId op_id) override {
        bool found = false;
        backend.scan_operator_state(
            op_id, [&](StateBackend::KeyView key, StateBackend::ValueView value) {
                if (std::string_view(key) != std::string_view(kPosKey)) {
                    return;
                }
                const auto* p = reinterpret_cast<const unsigned char*>(value.data());
                const std::size_t n = value.size();
                if (n < 12) {
                    return;
                }
                const std::uint32_t flen = get_u32(p);
                if (static_cast<std::size_t>(4) + flen + 8 != n) {
                    return;
                }
                cur_file_.assign(reinterpret_cast<const char*>(p + 4), flen);
                cur_pos_ = get_u64(p + 4 + flen);
                committed_file_ = cur_file_;
                committed_pos_ = cur_pos_;
                restored_ = true;
                found = true;
            });
        return found;
    }

    // Observability/test accessor: the file:pos actually passed to the stream
    // start (restored checkpoint, explicit option, or the current master head).
    [[nodiscard]] std::string start_position() const { return start_position_; }

private:
    void teardown_() {
        tables_.clear();  // nothing to free: table-map metadata is copied, not retained
        if (rpl_ != nullptr) {
            mariadb_rpl_close(rpl_);
            rpl_ = nullptr;
        }
        // Free the snapshot result (may be mid-stream) BEFORE its connection: the
        // Result dtor runs mysql_free_result, which must precede mysql_close.
        cur_snap_result_.reset();
        snapshot_conn_.reset();
        stream_conn_.reset();
        schema_conn_.reset();
    }

    std::string pos_str_() const { return cur_file_ + ":" + std::to_string(cur_pos_); }

    // Open the binlog dump stream at the resolved (cur_file_, cur_pos_). Creates the
    // streaming connections if absent (the snapshot path defers them to here), sets
    // the checksum + heartbeat session vars, then opens the rpl stream.
    void open_stream_() {
        if (!schema_conn_) {
            schema_conn_ = std::make_unique<Connection>(opts_.conn);
        }
        if (!stream_conn_) {
            stream_conn_ = std::make_unique<Connection>(opts_.conn);
        }
        // Match the master's checksum setting on this dump session so the library
        // strips the trailing CRC correctly, then ask for periodic heartbeats so an
        // idle fetch returns within heartbeat_ms (bounds cancel + checkpoint).
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
        // NOTE: we deliberately do NOT set MARIADB_RPL_EXTRACT_VALUES - the
        // library's value decoder is unusable against a MySQL master (garbles
        // CHAR/TEXT/DATETIME/DECIMAL/ENUM/JSON/...); we decode the raw row image
        // ourselves (mysql_row_decode.hpp).
        if (mariadb_rpl_open(rpl_) != 0) {
            const std::string e = mariadb_rpl_error(rpl_) ? mariadb_rpl_error(rpl_) : "unknown";
            throw std::runtime_error(opts_.name + ": mariadb_rpl_open failed: " + e);
        }
    }

    // Establish a consistent snapshot point and the read view to scan it. With the
    // lock (default): FTWRL freezes writes, so the SHOW MASTER STATUS coordinate and
    // the consistent-snapshot read view are the SAME point P (exact, no overlap);
    // UNLOCK lets writers resume while the transaction's view persists. Without the
    // lock: capture P first, THEN start the snapshot view (>= P), so streaming from
    // P overlaps the snapshot by a few rows (at-least-once, no gap). Either way the
    // open transaction on snapshot_conn_ is the consistent view the SELECTs read.
    void setup_snapshot_() {
        snapshot_conn_ = std::make_unique<Connection>(opts_.conn);
        // Force REPEATABLE READ for this session so CONSISTENT SNAPSHOT gives a
        // stable cross-table view even if the server default was changed.
        snapshot_conn_->exec("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ");
        if (opts_.snapshot_lock) {
            snapshot_conn_->exec("FLUSH TABLES WITH READ LOCK");
            capture_master_pos_();
            snapshot_conn_->exec("START TRANSACTION WITH CONSISTENT SNAPSHOT");
            snapshot_conn_->exec("UNLOCK TABLES");
        } else {
            capture_master_pos_();
            snapshot_conn_->exec("START TRANSACTION WITH CONSISTENT SNAPSHOT");
        }
        for (const auto& entry : opts_.tables) {
            snapshot_queue_.push_back(qualify_table(entry, opts_.conn.database));
        }
    }

    // Read SHOW MASTER STATUS on snapshot_conn_ into cur_file_/cur_pos_ (the binlog
    // coordinate streaming will resume from).
    void capture_master_pos_() {
        Result r = snapshot_conn_->query("SHOW MASTER STATUS");
        MYSQL_ROW row = r.fetch_row();
        if (row == nullptr || row[0] == nullptr || row[1] == nullptr) {
            throw std::runtime_error(
                opts_.name + ": SHOW MASTER STATUS returned no position (is the binary log on?)");
        }
        cur_file_ = row[0];
        cur_pos_ = std::strtoull(row[1], nullptr, 10);
    }

    // Emit one chunk of snapshot rows. Streams the current table's result row by
    // row; when a table is exhausted it advances to the next, and when all are done
    // it commits the snapshot transaction and opens the binlog stream (finish_
    // snapshot_), after which produce() takes the streaming path.
    bool produce_snapshot_(Emitter<std::string>& out) {
        Batch<std::string> batch;
        std::uint64_t bytes = 0;
        int rows_this_call = 0;
        const std::string lsn = pos_str_();
        while (rows_this_call < kMaxEventsPerProduce) {
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
                // End of this table - or a mid-stream error (use_result surfaces it
                // here, not at query time).
                if (snapshot_conn_->last_errno() != 0) {
                    clink::metrics::connector::error_inc(kLabel, "source");
                    throw std::runtime_error(opts_.name + ": snapshot scan of " + cur_snap_table_ +
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
            if (batch.size() >= kMaxBatch) {
                break;
            }
        }
        if (!batch.empty()) {
            clink::metrics::connector::records_in_inc(kLabel, batch.size());
            clink::metrics::connector::bytes_in_inc(kLabel, bytes);
            out.emit_data(std::move(batch));
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
    }

    // All tables scanned: commit the consistent-snapshot transaction, release the
    // snapshot connection, and switch to streaming from the captured coordinate. Now
    // it is safe to checkpoint (committed_* set), so streaming resumes here on a
    // restart instead of re-snapshotting.
    void finish_snapshot_() {
        cur_snap_result_.reset();
        if (snapshot_conn_) {
            snapshot_conn_->exec("COMMIT");
            snapshot_conn_.reset();
        }
        committed_file_ = cur_file_;
        committed_pos_ = cur_pos_;
        open_stream_();
        phase_ = Phase::Stream;
    }

    // Advance the durable checkpoint position to the current received position.
    // Called only at guaranteed inter-transaction boundaries (XID / ROTATE /
    // heartbeat), so a restored checkpoint never lands inside a TABLE_MAP/rows
    // pair (which would orphan the map and drop rows).
    void mark_commit_boundary_() {
        committed_file_ = cur_file_;
        committed_pos_ = cur_pos_;
    }

    void resolve_start_position_() {
        if (restored_) {
            return;  // cur_file_/cur_pos_ already set by restore_offset
        }
        if (!opts_.start_file.empty()) {
            cur_file_ = opts_.start_file;
            cur_pos_ = opts_.start_pos;
            return;
        }
        // Fresh start = the current master head, so CDC captures changes from now
        // on rather than replaying the whole retained binlog history.
        Result r = stream_conn_->query("SHOW MASTER STATUS");
        if (MYSQL_ROW row = r.fetch_row();
            row != nullptr && row[0] != nullptr && row[1] != nullptr) {
            cur_file_ = row[0];
            cur_pos_ = std::strtoull(row[1], nullptr, 10);
        } else {
            cur_pos_ = opts_.start_pos;  // empty/unknown: server resolves the oldest file
        }
    }

    std::vector<CdcColumn> resolve_schema_(const std::string& db, const std::string& table) {
        std::vector<CdcColumn> cols;
        // Include EVERY column in ordinal order, generated columns included: MySQL's
        // binlog row image (under binlog_row_image=FULL) carries all columns - both
        // VIRTUAL and STORED generated ones - so the TABLE_MAP column_count matches
        // the full information_schema list (verified by probe: a 4-base-column table
        // with one VIRTUAL column reports column_count=5). Filtering generated
        // columns here would mis-count and drop every row. If a server config does
        // exclude them, the column-count guard drops+counts loudly rather than
        // emitting misaligned data.
        const std::string q =
            "SELECT COLUMN_NAME, COLUMN_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='" +
            schema_conn_->escape(db) + "' AND TABLE_NAME='" + schema_conn_->escape(table) +
            "' ORDER BY ORDINAL_POSITION";
        Result r = schema_conn_->query(q);
        while (MYSQL_ROW row = r.fetch_row()) {
            CdcColumn c;
            c.name = row[0] != nullptr ? row[0] : "";
            const std::string coltype = row[1] != nullptr ? row[1] : "";
            c.is_unsigned = coltype.find("unsigned") != std::string::npos;
            if (coltype.rfind("enum(", 0) == 0 || coltype.rfind("set(", 0) == 0) {
                c.enum_set_labels = parse_enum_set_labels(coltype);
            }
            cols.push_back(std::move(c));
        }
        return cols;
    }

    // Copies the binlog column metadata out of the TABLE_MAP and FREES the event
    // (we decode row images ourselves, so the rpl event need not be retained).
    void cache_table_map_(MARIADB_RPL_EVENT* ev) {
        const auto& tm = ev->event.table_map;
        const std::uint64_t id = tm.table_id;
        std::string db(tm.database.str != nullptr ? tm.database.str : "", tm.database.length);
        std::string table(tm.table.str != nullptr ? tm.table.str : "", tm.table.length);
        const std::uint32_t ncols = tm.column_count;
        std::vector<std::uint8_t> types(
            reinterpret_cast<const std::uint8_t*>(tm.column_types.str),
            reinterpret_cast<const std::uint8_t*>(tm.column_types.str) + tm.column_types.length);
        std::vector<std::uint8_t> metabytes(
            reinterpret_cast<const std::uint8_t*>(tm.metadata.str),
            reinterpret_cast<const std::uint8_t*>(tm.metadata.str) + tm.metadata.length);
        mariadb_free_rpl_event(ev);  // done with the event; nothing retained

        const std::string key = db + "." + table;
        const bool included = tables_filter_.empty() || tables_filter_.count(key) > 0 ||
                              tables_filter_.count(table) > 0;
        TableState st;
        st.metas = parse_table_metadata(types.data(), ncols, metabytes.data(), metabytes.size());
        st.filtered = !included;
        if (included) {
            auto cit = cols_cache_.find(key);
            if (cit == cols_cache_.end() || cit->second.size() != ncols) {
                cols_cache_[key] = resolve_schema_(db, table);
            }
            st.schema = CdcTableSchema{db, table, cols_cache_[key]};
        }
        tables_[id] = std::move(st);
    }

    void handle_rows_(MARIADB_RPL_EVENT* ev, Batch<std::string>& batch, std::uint64_t& bytes) {
        const std::uint64_t id = ev->event.rows.table_id;
        const auto* rd = reinterpret_cast<const std::uint8_t*>(ev->event.rows.row_data);
        const std::size_t rsz = ev->event.rows.row_data_size;
        auto it = tables_.find(id);
        if (it == tables_.end()) {
            ++dropped_;  // a row event with no preceding table-map -> poison
            report_poison_(
                rd,
                rsz,
                "row event for table_id " + std::to_string(id) + " with no preceding TABLE_MAP");
            return;
        }
        if (it->second.filtered) {
            return;  // excluded by the allowlist -> benign skip (not a drop)
        }
        if (rd == nullptr || rsz == 0) {
            return;
        }
        // Build lsn only past the filtered-skip fast path (the common case for an
        // allowlisted source on a busy master is a skipped non-allowlisted table).
        const std::string lsn = pos_str_();
        const RowOp op = ev->event.rows.type == UPDATE_ROWS   ? RowOp::Update
                         : ev->event.rows.type == DELETE_ROWS ? RowOp::Delete
                                                              : RowOp::Insert;
        MysqlDecodeResult res =
            decode_rows_payload(rd, rsz, op, it->second.metas, it->second.schema, lsn, cur_xid_);
        dropped_ += res.dropped;
        if (res.dropped > 0) {
            // The raw event blob holds every row of the event; report it once with
            // the count rather than per-row (the per-row bytes are not surfaced by
            // the decoder). The metric still counts each dropped row.
            report_poison_(
                rd, rsz, std::to_string(res.dropped) + " row(s) in the event failed to decode");
        }
        for (const auto& cev : res.events) {
            if (auto js = clink::cdc::cdc_event_to_json_row(cev)) {
                bytes += js->size();
                batch.emplace(std::move(*js));
            }
        }
    }

    // Route a poison binlog row event to the DLQ (best-effort; no-op if no runtime
    // context is attached, e.g. a unit test driving the source directly). Formats
    // the lsn lazily, so a no-op report costs nothing.
    void report_poison_(const std::uint8_t* rd, std::size_t rsz, std::string why) {
        auto* rt = this->runtime();
        if (rt == nullptr) {
            return;
        }
        rt->report_bad_record(clink::BadRecord{
            .payload = (rd != nullptr && rsz > 0) ? to_hex(rd, rsz) : std::string{},
            .error = std::move(why),
            .connector = kLabel,
            .direction = "source",
            .location = pos_str_()});
    }

    enum class Phase { Snapshot, Stream };

    MysqlCdcOptions opts_;
    bool dormant_{false};
    Phase phase_{Phase::Stream};
    std::unique_ptr<Connection> schema_conn_;  // information_schema lookups
    std::unique_ptr<Connection> stream_conn_;  // the binlog dump session
    MARIADB_RPL* rpl_{nullptr};

    // Initial-snapshot phase state (only used when enable_initial_snapshot + no
    // restored checkpoint). The snapshot connection holds the consistent-snapshot
    // transaction; cur_snap_result_ streams the current table row by row.
    std::unique_ptr<Connection> snapshot_conn_;
    std::deque<QualifiedTable> snapshot_queue_;  // tables still to scan
    Result cur_snap_result_;                     // open use_result for the current table
    std::string cur_snap_table_;                 // "db.table" of the current scan
    std::vector<std::string> cur_snap_fields_;   // column names of the current scan

    std::map<std::uint64_t, TableState> tables_;                // table_id -> retained map + schema
    std::map<std::string, std::vector<CdcColumn>> cols_cache_;  // "db.table" -> columns
    std::set<std::string> tables_filter_;

    std::string cur_file_;  // live received position (lsn metadata + lag)
    std::uint64_t cur_pos_{4};
    std::string committed_file_;  // last transaction-boundary position (checkpointed)
    std::uint64_t committed_pos_{4};
    std::int64_t cur_xid_{0};
    bool restored_{false};
    std::string start_position_;
    std::uint64_t dropped_{0};
    std::uint64_t last_dropped_{0};
};

}  // namespace

std::shared_ptr<Source<std::string>> make_mysql_cdc_source(const MysqlCdcOptions& opts) {
    if (opts.server_id == 0) {
        throw std::runtime_error(opts.name + ": 'server_id' is required and must be non-zero");
    }
    if (opts.enable_initial_snapshot) {
        // The snapshot scans an explicit table list; "all tables" would require
        // enumerating + reading the whole schema (system tables included), which is
        // never what is wanted. Require the same allowlist the stream filters on.
        if (opts.tables.empty()) {
            throw std::runtime_error(
                opts.name + ": enable_initial_snapshot requires a non-empty 'tables' list");
        }
        // Require FULLY-QUALIFIED db.table entries: the stream filter matches a bare
        // name across EVERY database, but the snapshot can only scan one qualified
        // table - so a bare name would stream same-named tables in other DBs whose
        // existing rows the snapshot never bootstraps (a silent partial capture).
        // Qualified entries make the snapshot and stream cover exactly the same set.
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
