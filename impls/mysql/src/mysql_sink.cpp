// MySQL append and insert-or-update sink. See mysql_sink.hpp.

#include "clink/mysql/mysql_sink.hpp"

#include <algorithm>
#include <stdexcept>
#include <utility>

#include "clink/fault/fault_injection.hpp"

#include "fault_points.hpp"

namespace clink::mysql {

namespace {

MysqlSinkOptions checked(MysqlSinkOptions o) {
    const auto refuse = [&](const std::string& why) {
        throw std::runtime_error(o.name + ": " + why);
    };
    if (o.table.empty()) {
        refuse("'table' is required");
    }
    if (o.columns.empty()) {
        refuse("'columns' is required (the projection)");
    }
    // Identifiers are checked here, so a bad one fails the build, not a flush.
    (void)quote_table_ident(o.table);
    for (const auto& c : o.columns) {
        (void)quote_ident(c);
    }
    if (!o.upsert && !o.update_columns.empty()) {
        refuse("'update_columns' is the SET list of on_duplicate='update', which is not set");
    }
    for (const auto& c : o.update_columns) {
        (void)quote_ident(c);
        if (std::find(o.columns.begin(), o.columns.end(), c) == o.columns.end()) {
            refuse("update column '" + c + "' is not one of the columns the sink writes");
        }
    }
    if (o.batch_records == 0) {
        o.batch_records = 1;
    }
    if (o.max_bytes == 0) {
        o.max_bytes = MysqlSink::kDefaultMaxBytes;
    }
    if (o.max_statement_bytes < 1024 || o.max_record_bytes < 64 ||
        o.max_record_bytes > o.max_statement_bytes) {
        refuse(
            "max_record_bytes must be at least 64 and at most max_statement_bytes, which must "
            "be at least 1024");
    }
    if (o.lock_wait_timeout.count() < 1 || o.lock_wait_timeout.count() > 3600) {
        refuse("the lock wait timeout must be 1 to 3600 seconds");
    }
    if (o.max_retries < 0 || o.max_retries > 10) {
        refuse("max_retries must be 0 to 10");
    }
    return o;
}

SinkCoreOptions core_options(const MysqlSinkOptions& o) {
    SinkCoreOptions c;
    c.conn = o.conn;
    c.target.table = o.table;
    c.target.columns = o.columns;
    c.target.timestamp_columns = o.timestamp_columns;
    c.target.mode = o.upsert ? SinkMode::InsertOrUpdate : SinkMode::Append;
    c.target.update_columns = o.update_columns;
    c.max_statement_bytes = o.max_statement_bytes;
    c.lock_wait_timeout = o.lock_wait_timeout;
    c.max_retries = o.max_retries;
    c.name = o.name;
    c.server_factory = o.server_factory;
    return c;
}

}  // namespace

MysqlSink::MysqlSink(MysqlSinkOptions opts)
    : opts_(checked(std::move(opts))), core_(core_options(opts_)) {}

void MysqlSink::open() {
    pending_.clear();
    pending_bytes_ = 0;
    core_.open(runtime());
    head_ = insert_head(opts_.table, opts_.columns);
    tail_ = opts_.upsert
                ? upsert_tail(opts_.update_columns.empty() ? opts_.columns : opts_.update_columns)
                : std::string{};
    const auto fixed = head_.size() + tail_.size();
    if (core_.statement_bytes() <= fixed) {
        core_.fail("a statement of at most " + std::to_string(core_.statement_bytes()) +
                   " bytes cannot hold the INSERT's " + std::to_string(fixed) +
                   " bytes of column names");
    }
    row_capacity_ = std::min(opts_.max_record_bytes, core_.statement_bytes() - fixed);
}

void MysqlSink::on_data(const Batch<std::string>& batch) {
    core_.require_usable("a record");
    const sqljson::EscapeFn esc = [this](std::string_view s) { return core_.escape(s); };
    for (const auto& rec : batch) {
        JsonMembers m;
        if (auto why = read_json_object(rec.value(), m); !why.empty()) {
            core_.fail("a record cannot be written: " + why);
        }
        std::string row;
        try {
            // A retraction cannot be appended: written as a row it would add
            // what it removes.
            if (change_kind_of(m) == ChangeKind::Delete) {
                throw RecordRefused(
                    "it is a retraction (__row_kind delete or update_before), "
                    "which this sink cannot apply: it writes rows and never "
                    "removes them, and mode='upsert' maintains a table by key");
            }
            row = render_row(m, opts_.columns, core_.schema(), opts_.timestamp_columns, esc);
        } catch (const RecordRefused& e) {
            core_.fail(std::string{"a record cannot be written: "} + e.what());
        }
        if (row.size() > row_capacity_) {
            core_.fail("a record's row is " + std::to_string(row.size()) +
                       " bytes as SQL, more than the " + std::to_string(row_capacity_) +
                       " one statement holds beside its head (max_record_bytes, "
                       "max_statement_bytes and the server's max_allowed_packet bound it)");
        }
        if (pending_.empty()) {
            first_buffered_at_ = std::chrono::steady_clock::now();
        }
        pending_bytes_ += row.size();
        pending_.push_back(std::move(row));
        if (pending_.size() >= opts_.batch_records || pending_bytes_ >= opts_.max_bytes ||
            linger_elapsed_()) {
            flush();
        }
    }
}

void MysqlSink::on_barrier(CheckpointBarrier /*b*/) {
    core_.require_usable("a checkpoint barrier");
    const bool writes = !pending_.empty();
    flush();
    if (writes) {
        (void)CLINK_FAULT_POINT(points::kSinkAfterCommit);
    }
}

void MysqlSink::flush() {
    // A failed sink refuses even an empty flush, so a clean end of input cannot
    // pass over its failure; one never opened has nothing to flush.
    core_.require_not_failed("a flush");
    if (pending_.empty()) {
        return;
    }
    std::vector<std::string> statements;
    try {
        statements = pack_statements(head_, pending_, tail_, core_.statement_bytes());
    } catch (const std::length_error& e) {
        core_.fail(e.what());
    }
    const auto records = pending_.size();
    const auto bytes = pending_bytes_;
    pending_.clear();
    pending_bytes_ = 0;
    core_.write(statements, records, bytes);
}

void MysqlSink::close() {
    pending_.clear();
    pending_bytes_ = 0;
    core_.close();
}

bool MysqlSink::linger_elapsed_() const {
    return opts_.max_age.count() > 0 && !pending_.empty() &&
           std::chrono::steady_clock::now() - first_buffered_at_ >= opts_.max_age;
}

}  // namespace clink::mysql
