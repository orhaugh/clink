// MySQL changelog upsert sink. See mysql_json_upsert_sink.hpp.

#include <algorithm>
#include <stdexcept>
#include <utility>

#include "clink/fault/fault_injection.hpp"
#include "clink/mysql/mysql_json_upsert_sink.hpp"
#include "clink/mysql/mysql_sql.hpp"

#include "fault_points.hpp"

namespace clink::mysql {

namespace {

MysqlJsonUpsertSinkOptions checked(MysqlJsonUpsertSinkOptions o) {
    const auto refuse = [&](const std::string& why) {
        throw std::runtime_error(o.name + ": " + why);
    };
    if (o.table.empty()) {
        refuse("'table' is required");
    }
    if (o.columns.empty()) {
        refuse("'columns' is required (the projection)");
    }
    if (o.key_columns.empty()) {
        refuse("'key_columns' (the primary key) is required");
    }
    (void)quote_table_ident(o.table);
    const auto in_columns = [&](const std::string& c) {
        return std::find(o.columns.begin(), o.columns.end(), c) != o.columns.end();
    };
    for (const auto& c : o.columns) {
        (void)quote_ident(c);
    }
    for (const auto& c : o.key_columns) {
        (void)quote_ident(c);
        if (!in_columns(c)) {
            refuse("key column '" + c + "' is not one of the columns the sink writes");
        }
    }
    for (const auto& c : o.update_columns) {
        (void)quote_ident(c);
        if (!in_columns(c)) {
            refuse("update column '" + c + "' is not one of the columns the sink writes");
        }
    }
    if (o.batch_records == 0) {
        o.batch_records = 1;
    }
    if (o.max_bytes == 0) {
        o.max_bytes = MysqlJsonUpsertSink::kDefaultMaxBytes;
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
    if (o.parallelism == 0) {
        o.parallelism = 1;
    }
    return o;
}

SinkCoreOptions core_options(const MysqlJsonUpsertSinkOptions& o) {
    SinkCoreOptions c;
    c.conn = o.conn;
    c.target.table = o.table;
    c.target.columns = o.columns;
    c.target.timestamp_columns = o.timestamp_columns;
    c.target.mode = SinkMode::Changelog;
    c.target.update_columns = o.update_columns;
    c.target.key_columns = o.key_columns;
    c.target.parallelism = o.parallelism;
    c.max_statement_bytes = o.max_statement_bytes;
    c.lock_wait_timeout = o.lock_wait_timeout;
    c.max_retries = o.max_retries;
    c.name = o.name;
    c.server_factory = o.server_factory;
    return c;
}

}  // namespace

MysqlJsonUpsertSink::MysqlJsonUpsertSink(MysqlJsonUpsertSinkOptions opts)
    : opts_(checked(std::move(opts))), core_(core_options(opts_)) {}

void MysqlJsonUpsertSink::open() {
    buffer_.clear();
    core_.open(runtime());
    insert_head_ = insert_head(opts_.table, opts_.columns);
    upsert_tail_ = upsert_tail(opts_.update_columns.empty() ? opts_.columns : opts_.update_columns);
    delete_head_ = delete_head(opts_.table, opts_.key_columns);
    const auto upsert_fixed = insert_head_.size() + upsert_tail_.size();
    const auto delete_fixed = delete_head_.size() + kDeleteTail.size();
    if (core_.statement_bytes() <= std::max(upsert_fixed, delete_fixed)) {
        core_.fail("a statement of at most " + std::to_string(core_.statement_bytes()) +
                   " bytes cannot hold the upsert's or delete's column names");
    }
    row_capacity_ = std::min(opts_.max_record_bytes, core_.statement_bytes() - upsert_fixed);
    key_capacity_ = std::min(opts_.max_record_bytes, core_.statement_bytes() - delete_fixed);
}

void MysqlJsonUpsertSink::on_data(const Batch<std::string>& batch) {
    core_.require_usable("a record");
    const sqljson::EscapeFn esc = [this](std::string_view s) { return core_.escape(s); };
    for (const auto& rec : batch) {
        JsonMembers m;
        if (auto why = read_json_object(rec.value(), m); !why.empty()) {
            core_.fail("a changelog record cannot be applied: " + why);
        }
        ChangelogBuffer::Change change;
        std::string key;
        try {
            change.kind = change_kind_of(m);
            if (opts_.parallelism > 1) {
                check_key_spelling(m, opts_.key_columns, core_.schema());
            }
            key = stream_key(m, opts_.key_columns);
            if (change.kind == ChangeKind::Upsert) {
                change.row =
                    render_row(m, opts_.columns, core_.schema(), opts_.timestamp_columns, esc);
            } else {
                change.key =
                    render_key(m, opts_.key_columns, core_.schema(), opts_.timestamp_columns, esc);
            }
        } catch (const RecordRefused& e) {
            core_.fail(std::string{"a changelog record cannot be applied: "} + e.what());
        }
        const auto size = change.row.size() + change.key.size();
        const auto capacity = change.kind == ChangeKind::Upsert ? row_capacity_ : key_capacity_;
        if (size > capacity) {
            core_.fail("a changelog record's " +
                       std::string{change.kind == ChangeKind::Upsert ? "row" : "key"} + " is " +
                       std::to_string(size) + " bytes as SQL, more than the " +
                       std::to_string(capacity) +
                       " one statement holds beside its head (max_record_bytes, "
                       "max_statement_bytes and the server's max_allowed_packet bound it)");
        }
        buffer_.add(key, std::move(change));
        if (buffer_.size() >= opts_.batch_records || buffer_.bytes() >= opts_.max_bytes) {
            flush();
        }
    }
}

void MysqlJsonUpsertSink::on_barrier(CheckpointBarrier /*b*/) {
    core_.require_usable("a checkpoint barrier");
    const bool writes = !buffer_.empty();
    flush();
    if (writes) {
        (void)CLINK_FAULT_POINT(points::kSinkAfterCommit);
    }
}

void MysqlJsonUpsertSink::flush() {
    // A failed sink refuses even an empty flush, so a clean end of input cannot
    // pass over its failure; one never opened has nothing to flush.
    core_.require_not_failed("a flush");
    if (buffer_.empty()) {
        return;
    }
    std::vector<std::string> statements;
    try {
        for (const auto& [kind, items] : buffer_.runs()) {
            auto run =
                kind == ChangeKind::Upsert
                    ? pack_statements(insert_head_, items, upsert_tail_, core_.statement_bytes())
                    : pack_statements(
                          delete_head_, items, std::string{kDeleteTail}, core_.statement_bytes());
            for (auto& s : run) {
                statements.push_back(std::move(s));
            }
        }
    } catch (const std::length_error& e) {
        core_.fail(e.what());
    }
    const auto records = buffer_.size();
    const auto bytes = buffer_.bytes();
    buffer_.clear();
    core_.write(statements, records, bytes);
}

void MysqlJsonUpsertSink::close() {
    buffer_.clear();
    core_.close();
}

}  // namespace clink::mysql
