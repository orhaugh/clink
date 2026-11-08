// PostgreSQL changelog upsert sink. See postgres_json_upsert_sink.hpp.

#include <algorithm>
#include <stdexcept>
#include <utility>

#include "clink/connectors/postgres_json_upsert_sink.hpp"
#include "clink/connectors/postgres_sql.hpp"
#include "clink/fault/fault_injection.hpp"

#include "fault_points.hpp"

namespace clink {

namespace {

PostgresJsonUpsertSinkOptions checked(PostgresJsonUpsertSinkOptions o) {
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
    if (o.key_columns.empty()) {
        refuse("'key_columns' (the primary key) is required");
    }
    (void)pgsql::quote_table_ident(o.table);
    for (const auto& c : o.columns) {
        (void)pgsql::quote_ident(c);
    }
    for (const auto& c : o.key_columns) {
        (void)pgsql::quote_ident(c);
        if (std::find(o.columns.begin(), o.columns.end(), c) == o.columns.end()) {
            refuse("key column '" + c + "' is not one of the columns the sink writes");
        }
    }
    if (o.batch_records == 0) {
        o.batch_records = 1;
    }
    if (o.max_bytes == 0) {
        o.max_bytes = PostgresJsonUpsertSink::kDefaultMaxBytes;
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

postgres::PgSinkCoreOptions core_options(const PostgresJsonUpsertSinkOptions& o) {
    postgres::PgSinkCoreOptions c;
    c.conninfo = o.conninfo;
    c.target.table = o.table;
    c.target.columns = o.columns;
    c.target.timestamp_columns = o.timestamp_columns;
    c.target.mode = postgres::PgSinkMode::Changelog;
    c.target.conflict_columns = o.key_columns;
    c.target.parallelism = o.parallelism;
    c.max_statement_bytes = o.max_statement_bytes;
    c.lock_wait_timeout = o.lock_wait_timeout;
    c.max_retries = o.max_retries;
    c.name = o.name;
    c.server_factory = o.server_factory;
    return c;
}

}  // namespace

PostgresJsonUpsertSink::PostgresJsonUpsertSink(PostgresJsonUpsertSinkOptions opts)
    : opts_(checked(std::move(opts))), core_(core_options(opts_)) {}

void PostgresJsonUpsertSink::open() {
    buffer_.clear();
    core_.open(runtime());
    std::vector<std::string> set;
    for (const auto& c : opts_.columns) {
        if (std::find(opts_.key_columns.begin(), opts_.key_columns.end(), c) ==
            opts_.key_columns.end()) {
            set.push_back(c);
        }
    }
    insert_head_ = postgres::pg_insert_head(opts_.table, opts_.columns);
    upsert_tail_ = postgres::pg_update_tail(opts_.key_columns, set);
    delete_head_ = postgres::pg_delete_head(opts_.table, opts_.key_columns);
    const auto upsert_fixed = insert_head_.size() + upsert_tail_.size();
    const auto delete_fixed = delete_head_.size() + postgres::kPgDeleteTail.size();
    if (core_.statement_bytes() <= std::max(upsert_fixed, delete_fixed)) {
        core_.fail("a statement of at most " + std::to_string(core_.statement_bytes()) +
                   " bytes cannot hold the upsert's or delete's column names");
    }
    row_capacity_ = std::min(opts_.max_record_bytes, core_.statement_bytes() - upsert_fixed);
    key_capacity_ = std::min(opts_.max_record_bytes, core_.statement_bytes() - delete_fixed);
}

void PostgresJsonUpsertSink::on_data(const Batch<std::string>& batch) {
    core_.require_usable("a record");
    const sqljson::EscapeFn esc = [this](std::string_view s) { return core_.escape(s); };
    for (const auto& rec : batch) {
        postgres::JsonMembers m;
        if (auto why = postgres::read_json_object(rec.value(), m); !why.empty()) {
            core_.fail("a changelog record cannot be applied: " + why);
        }
        postgres::ChangelogBuffer::Change change;
        std::string key;
        try {
            change.kind = postgres::change_kind_of(m);
            if (opts_.parallelism > 1) {
                postgres::check_pg_key_spelling(m, opts_.key_columns, core_.table());
            }
            key = postgres::stream_key(m, opts_.key_columns);
            if (change.kind == postgres::ChangeKind::Upsert) {
                change.row = postgres::render_pg_row(
                    m, opts_.columns, core_.table(), opts_.timestamp_columns, esc);
            } else {
                change.key = postgres::render_pg_key(
                    m, opts_.key_columns, core_.table(), opts_.timestamp_columns, esc);
            }
        } catch (const postgres::RecordRefused& e) {
            core_.fail(std::string{"a changelog record cannot be applied: "} + e.what());
        }
        const auto size = change.row.size() + change.key.size();
        const bool upsert = change.kind == postgres::ChangeKind::Upsert;
        const auto capacity = upsert ? row_capacity_ : key_capacity_;
        if (size > capacity) {
            core_.fail("a changelog record's " + std::string{upsert ? "row" : "key"} + " is " +
                       std::to_string(size) + " bytes as SQL, more than the " +
                       std::to_string(capacity) +
                       " one statement holds beside its head (max_record_bytes and "
                       "max_statement_bytes bound it)");
        }
        buffer_.add(key, std::move(change));
        if (buffer_.size() >= opts_.batch_records || buffer_.bytes() >= opts_.max_bytes) {
            flush();
        }
    }
}

void PostgresJsonUpsertSink::on_barrier(CheckpointBarrier /*b*/) {
    core_.require_usable("a checkpoint barrier");
    const bool writes = !buffer_.empty();
    flush();
    if (writes) {
        (void)CLINK_FAULT_POINT(postgres::points::kSinkAfterCommit);
    }
}

std::vector<postgres::PgStatement> PostgresJsonUpsertSink::plan_(bool one_row_each) const {
    std::vector<postgres::PgStatement> out;
    for (const auto& [kind, items] : buffer_.runs()) {
        std::vector<std::string> run;
        if (kind == postgres::ChangeKind::Upsert && one_row_each) {
            for (const auto& item : items) {
                run.push_back(insert_head_ + item + upsert_tail_);
            }
        } else if (kind == postgres::ChangeKind::Upsert) {
            run = postgres::pack_statements(
                insert_head_, items, upsert_tail_, core_.statement_bytes());
        } else {
            run = postgres::pack_statements(
                delete_head_, items, std::string{postgres::kPgDeleteTail}, core_.statement_bytes());
        }
        for (auto& s : run) {
            out.push_back(postgres::PgStatement{std::move(s), std::nullopt});
        }
    }
    return out;
}

void PostgresJsonUpsertSink::flush() {
    // A failed sink refuses even an empty flush, so a clean end of input cannot
    // pass over its failure; one never opened has nothing to flush.
    core_.require_not_failed("a flush");
    if (buffer_.empty()) {
        return;
    }
    core_.write(
        [this](bool one_row_each) { return plan_(one_row_each); }, buffer_.size(), buffer_.bytes());
    buffer_.clear();
}

void PostgresJsonUpsertSink::close() {
    buffer_.clear();
    core_.close();
}

}  // namespace clink
