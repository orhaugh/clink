#pragma once

// PostgresJsonUpsertSink - changelog-aware PostgreSQL sink for mode='upsert'.
//
// Consumes the clink changelog (a "__row_kind" member on each JSON row) and
// maintains a table by its key:
//   insert / update_after (or no __row_kind)  -> INSERT ... ON CONFLICT (<key>) DO UPDATE
//   delete / update_before                    -> DELETE FROM <table> WHERE <key> IN (...)
// Any other __row_kind fails the sink.
//
// Order. Within a flush a key's later change replaces its earlier one, and the
// surviving changes are applied in the order in which each key was last
// changed: consecutive upserts as one INSERT, consecutive deletes as one
// DELETE, the runs in order, all in one transaction. Two keys the stream holds
// apart that the table treats as one row (a char(n) key with trailing spaces,
// a nondeterministic collation, '1' and 1 in an integer column) therefore meet
// the table in the order they were changed, and the row ends as the last change
// left it; when two of them fall in one INSERT, which PostgreSQL refuses, the
// flush is written again a row to a statement.
//
// Identity. The table is checked when the sink opens: a table, not a view; a
// PRIMARY KEY or UNIQUE index on exactly key_columns, over whole values and
// every row, for ON CONFLICT to write by; and, with more than one subtask, key
// columns that PostgreSQL tells apart exactly as the stream does
// (pg_key_identity_exact), each record's key written as its column compares.
// Another unique index is enforced by PostgreSQL: a change that collides on it
// fails the flush.
//
// EFFECTIVELY-ONCE on the table for a stable key, a deterministic defining
// query and one writer per key: a restored job applies again what its last
// checkpoint did not cover, and the rows converge. A replay rewrites rows, and
// can overwrite a change another writer made to them since. NOT two-phase
// commit (for that use delivery_guarantee='exactly_once', the
// postgres_2pc_sink).

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "clink/connectors/sql_json_builder.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/postgres/pg_sink_core.hpp"

namespace clink {

struct PostgresJsonUpsertSinkOptions {
    std::string conninfo;              // libpq conninfo (required)
    std::string table;                 // target table (required)
    std::vector<std::string> columns;  // full projection for the upsert (required)
    // Columns holding a TIMESTAMP, written as timestamp literals rather than
    // the epoch milliseconds the SQL Row path carries (timestamp_columns).
    sqljson::TimeColumns timestamp_columns;
    std::vector<std::string> key_columns;  // the row's identity (required)
    std::size_t batch_records{1000};       // flush at this many buffered keys
    // Flush once the buffered changes' SQL reaches this many bytes; 0 means the
    // default, 16 MiB.
    std::size_t max_bytes{0};
    std::size_t max_record_bytes{1U << 20};     // one record's row or key, as SQL
    std::size_t max_statement_bytes{4U << 20};  // one statement
    std::chrono::seconds lock_wait_timeout{30};
    int max_retries{3};            // after a deadlock, a serialization failure or a lock wait
    std::uint32_t parallelism{1};  // subtasks writing the table
    std::string name{"postgres_upsert_sink"};
    postgres::PgSinkServerFactory server_factory;  // unset: a real server
};

class PostgresJsonUpsertSink : public Sink<std::string> {
public:
    static constexpr std::size_t kDefaultMaxBytes = 16U << 20;

    explicit PostgresJsonUpsertSink(PostgresJsonUpsertSinkOptions opts);

    void open() override;
    void on_data(const Batch<std::string>& batch) override;
    void on_barrier(CheckpointBarrier b) override;
    // Its barrier hook writes out everything it received before the barrier,
    // which is its guarantee only while it owns its chain's checkpoint, so
    // Dag::add_sink refuses another sink beside it.
    [[nodiscard]] bool gates_checkpoint_ack() const noexcept override { return true; }
    void flush() override;
    // Discards what is buffered (a restored job applies it again) and releases
    // the connection. Never throws.
    void close() override;
    std::string name() const override { return opts_.name; }

private:
    [[nodiscard]] std::vector<postgres::PgStatement> plan_(bool one_row_each) const;

    PostgresJsonUpsertSinkOptions opts_;
    postgres::PgSinkCore core_;
    std::string insert_head_;
    std::string upsert_tail_;
    std::string delete_head_;
    std::size_t row_capacity_{0};
    std::size_t key_capacity_{0};
    postgres::ChangelogBuffer buffer_;
};

}  // namespace clink
