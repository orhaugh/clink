// PostgreSQL append and ON CONFLICT sink. See postgres_json_sink.hpp.

#include "clink/connectors/postgres_json_sink.hpp"

#include <algorithm>
#include <set>
#include <stdexcept>
#include <utility>

#include "clink/connectors/postgres_sql.hpp"
#include "clink/fault/fault_injection.hpp"

#include "fault_points.hpp"

namespace clink {

namespace {

using postgres::PgSinkMode;

PostgresJsonSinkOptions checked(PostgresJsonSinkOptions o) {
    const auto refuse = [&](const std::string& why) {
        throw std::runtime_error(o.name + ": " + why);
    };
    if (o.conninfo.empty()) {
        refuse("'conninfo' is required");
    }
    if (o.table.empty()) {
        refuse("'table' is required");
    }
    if (o.columns.empty()) {
        refuse("'columns' is required (the projection)");
    }
    // Identifiers are checked here, so a bad one fails the build, not a flush.
    (void)pgsql::quote_table_ident(o.table);
    for (const auto& c : o.columns) {
        (void)pgsql::quote_ident(c);
    }
    if (o.on_conflict == "update" || o.on_conflict == "nothing") {
        if (o.conflict_columns.empty()) {
            refuse("on_conflict requires conflict_columns");
        }
    } else if (!o.on_conflict.empty()) {
        refuse("on_conflict must be 'update' or 'nothing'");
    }
    const auto in_columns = [&](const std::string& c) {
        return std::find(o.columns.begin(), o.columns.end(), c) != o.columns.end();
    };
    for (const auto& c : o.conflict_columns) {
        (void)pgsql::quote_ident(c);
        if (!in_columns(c)) {
            refuse("conflict column '" + c + "' is not one of the columns the sink writes");
        }
    }
    if (o.on_conflict != "update" && !o.update_columns.empty()) {
        refuse("'update_columns' is the SET list of on_conflict='update', which is not set");
    }
    for (const auto& c : o.update_columns) {
        (void)pgsql::quote_ident(c);
        if (!in_columns(c)) {
            refuse("update column '" + c + "' is not one of the columns the sink writes");
        }
    }
    if (o.batch_records == 0) {
        o.batch_records = 1;
    }
    if (o.max_bytes == 0) {
        o.max_bytes = PostgresJsonSink::kDefaultMaxBytes;
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

PgSinkMode mode_of(const PostgresJsonSinkOptions& o) {
    if (o.on_conflict == "update") {
        return PgSinkMode::OnConflictUpdate;
    }
    if (o.on_conflict == "nothing") {
        return PgSinkMode::OnConflictNothing;
    }
    return PgSinkMode::Append;
}

// The DO UPDATE SET list: the requested columns, or every column outside the
// conflict target.
std::vector<std::string> set_columns_of(const PostgresJsonSinkOptions& o) {
    if (!o.update_columns.empty()) {
        return o.update_columns;
    }
    std::vector<std::string> out;
    for (const auto& c : o.columns) {
        if (std::find(o.conflict_columns.begin(), o.conflict_columns.end(), c) ==
            o.conflict_columns.end()) {
            out.push_back(c);
        }
    }
    return out;
}

postgres::PgSinkCoreOptions core_options(const PostgresJsonSinkOptions& o) {
    postgres::PgSinkCoreOptions c;
    c.conninfo = o.conninfo;
    c.target.table = o.table;
    c.target.columns = o.columns;
    c.target.timestamp_columns = o.timestamp_columns;
    c.target.mode = mode_of(o);
    c.target.conflict_columns = o.conflict_columns;
    c.target.update_columns = o.update_columns;
    c.max_statement_bytes = o.max_statement_bytes;
    c.lock_wait_timeout = o.lock_wait_timeout;
    c.max_retries = o.max_retries;
    c.name = o.name;
    c.server_factory = o.server_factory;
    return c;
}

}  // namespace

PostgresJsonSink::PostgresJsonSink(PostgresJsonSinkOptions opts)
    : opts_(checked(std::move(opts))), core_(core_options(opts_)) {}

void PostgresJsonSink::open() {
    pending_.clear();
    pending_bytes_ = 0;
    core_.open(runtime());
    head_ = postgres::pg_insert_head(opts_.table, opts_.columns);
    switch (mode_of(opts_)) {
        case PgSinkMode::OnConflictUpdate:
            tail_ = postgres::pg_update_tail(opts_.conflict_columns, set_columns_of(opts_));
            break;
        case PgSinkMode::OnConflictNothing:
            tail_ = postgres::pg_nothing_tail(opts_.conflict_columns);
            break;
        default:
            tail_.clear();
    }
    const auto fixed = head_.size() + tail_.size();
    if (core_.statement_bytes() <= fixed) {
        core_.fail("a statement of at most " + std::to_string(core_.statement_bytes()) +
                   " bytes cannot hold the INSERT's " + std::to_string(fixed) +
                   " bytes of column names");
    }
    row_capacity_ = std::min(opts_.max_record_bytes, core_.statement_bytes() - fixed);
}

void PostgresJsonSink::on_data(const Batch<std::string>& batch) {
    core_.require_usable("a record");
    const sqljson::EscapeFn esc = [this](std::string_view s) { return core_.escape(s); };
    const bool keyed = !opts_.on_conflict.empty();
    for (const auto& rec : batch) {
        postgres::JsonMembers m;
        if (auto why = postgres::read_json_object(rec.value(), m); !why.empty()) {
            core_.fail("a record cannot be written: " + why);
        }
        Pending p;
        try {
            // A retraction cannot be appended: written as a row it would add
            // what it removes.
            if (postgres::change_kind_of(m) == postgres::ChangeKind::Delete) {
                throw postgres::RecordRefused(
                    "it is a retraction (__row_kind delete or update_before), which this sink "
                    "cannot apply: it writes rows and never removes them, and mode='upsert' "
                    "maintains a table by key");
            }
            if (keyed) {
                p.key = postgres::stream_key(m, opts_.conflict_columns);
            }
            p.row = postgres::render_pg_row(
                m, opts_.columns, core_.table(), opts_.timestamp_columns, esc);
        } catch (const postgres::RecordRefused& e) {
            core_.fail(std::string{"a record cannot be written: "} + e.what());
        }
        if (p.row.size() > row_capacity_) {
            core_.fail("a record's row is " + std::to_string(p.row.size()) +
                       " bytes as SQL, more than the " + std::to_string(row_capacity_) +
                       " one statement holds beside its head (max_record_bytes and "
                       "max_statement_bytes bound it)");
        }
        if (pending_.empty()) {
            first_buffered_at_ = std::chrono::steady_clock::now();
        }
        pending_bytes_ += p.row.size();
        pending_.push_back(std::move(p));
        if (pending_.size() >= opts_.batch_records || pending_bytes_ >= opts_.max_bytes ||
            linger_elapsed_()) {
            flush();
        }
    }
}

void PostgresJsonSink::on_barrier(CheckpointBarrier /*b*/) {
    core_.require_usable("a checkpoint barrier");
    const bool writes = !pending_.empty();
    flush();
    if (writes) {
        (void)CLINK_FAULT_POINT(postgres::points::kSinkAfterCommit);
    }
}

std::vector<postgres::PgStatement> PostgresJsonSink::plan_(bool one_row_each) const {
    // In order; a statement never takes two rows of one key, which ON CONFLICT
    // DO UPDATE refuses, so a key's later row is written after its earlier one.
    std::vector<postgres::PgStatement> out;
    std::string current;
    std::set<std::string> keys;
    const auto close_current = [&] {
        if (!current.empty()) {
            out.push_back(postgres::PgStatement{std::move(current) + tail_, std::nullopt});
            current.clear();
            keys.clear();
        }
    };
    for (const auto& p : pending_) {
        const bool repeats = !p.key.empty() && keys.count(p.key) > 0;
        if (one_row_each || repeats ||
            (!current.empty() &&
             current.size() + 1 + p.row.size() + tail_.size() > core_.statement_bytes())) {
            close_current();
        }
        if (current.empty()) {
            current = head_;
        } else {
            current.push_back(',');
        }
        current += p.row;
        if (!p.key.empty()) {
            keys.insert(p.key);
        }
    }
    close_current();
    return out;
}

void PostgresJsonSink::flush() {
    // A failed sink refuses even an empty flush, so a clean end of input cannot
    // pass over its failure; one never opened has nothing to flush.
    core_.require_not_failed("a flush");
    if (pending_.empty()) {
        return;
    }
    core_.write(
        [this](bool one_row_each) { return plan_(one_row_each); }, pending_.size(), pending_bytes_);
    pending_.clear();
    pending_bytes_ = 0;
}

void PostgresJsonSink::close() {
    pending_.clear();
    pending_bytes_ = 0;
    core_.close();
}

bool PostgresJsonSink::linger_elapsed_() const {
    return opts_.max_age.count() > 0 && !pending_.empty() &&
           std::chrono::steady_clock::now() - first_buffered_at_ >= opts_.max_age;
}

}  // namespace clink
