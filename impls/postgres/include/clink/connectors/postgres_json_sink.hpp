#pragma once

// PostgreSQL sink: each input record is a JSON-object string (row_to_json_string
// on the SQL path), written as one row of `columns` (a member the record lacks
// is NULL, as a null one is). Three modes:
//   append (default)          INSERT
//   on_conflict='update'      INSERT ... ON CONFLICT (conflict_columns) DO UPDATE
//   on_conflict='nothing'     INSERT ... ON CONFLICT (conflict_columns) DO NOTHING
//
// Rows are applied in the order they arrive: a flush packs them into statements
// no longer than the statement bound, never two rows of one key in a statement,
// and writes the statements as one transaction (see
// clink/postgres/pg_sink_core.hpp for failures, retries and the COMMIT whose
// outcome is unknown). Buffered rows are written at the count, byte and linger
// thresholds and at every checkpoint barrier. Delivery is AT-LEAST-ONCE: a
// restored job writes again what its last checkpoint did not cover, so append
// duplicates those rows and the keyed modes write them again by key. Not
// two-phase commit.
//
// The table is checked when the sink opens: every column must exist and take
// values (not generated, not GENERATED ALWAYS AS IDENTITY), a TIMESTAMP column
// must be timestamp or timestamptz, and the keyed modes need a table (not a
// view) with a PRIMARY KEY or UNIQUE index on exactly conflict_columns, over
// whole values and every row. Records are read strictly (one JSON object,
// numbers kept exactly as written), and a record that cannot be written as
// declared fails the sink, as do a retraction (a __row_kind of delete or
// update_before), an unknown __row_kind and, in the keyed modes, a key member
// that is missing or null.

#include <chrono>
#include <cstddef>
#include <libpq-fe.h>
#include <string>
#include <vector>

#include "clink/connectors/sql_json_builder.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/postgres/pg_sink_core.hpp"

namespace clink {

// Owning deleter for a PGconn.
struct PgconnDeleter {
    void operator()(PGconn* c) const noexcept {
        if (c != nullptr) {
            PQfinish(c);
        }
    }
};

struct PostgresJsonSinkOptions {
    std::string conninfo;              // libpq conninfo (required)
    std::string table;                 // target table (required)
    std::vector<std::string> columns;  // authoritative projection (required)
    // Columns holding a TIMESTAMP, written as timestamp literals rather than
    // the epoch milliseconds the SQL Row path carries (timestamp_columns).
    sqljson::TimeColumns timestamp_columns;
    std::string on_conflict;                    // "" | "update" | "nothing"
    std::vector<std::string> conflict_columns;  // ON CONFLICT target (required if on_conflict set)
    std::vector<std::string> update_columns;    // DO UPDATE SET list (empty = all non-key)
    std::size_t batch_records{1000};            // flush at this many buffered rows
    // Flush once the buffered rows' SQL reaches this many bytes; 0 means the
    // default, 16 MiB.
    std::size_t max_bytes{0};
    std::chrono::milliseconds max_age{0};       // linger (0 = off)
    std::size_t max_record_bytes{1U << 20};     // one record's row, as SQL
    std::size_t max_statement_bytes{4U << 20};  // one statement
    std::chrono::seconds lock_wait_timeout{30};
    int max_retries{3};  // after a deadlock, a serialization failure or a lock wait
    std::string name{"postgres_sink"};
    postgres::PgSinkServerFactory server_factory;  // unset: a real server
};

class PostgresJsonSink : public Sink<std::string> {
public:
    static constexpr std::size_t kDefaultMaxBytes = 16U << 20;

    explicit PostgresJsonSink(PostgresJsonSinkOptions opts);

    void open() override;
    void on_data(const Batch<std::string>& batch) override;
    void on_barrier(CheckpointBarrier b) override;
    // Its barrier hook writes out everything it received before the barrier,
    // which is its guarantee only while it owns its chain's checkpoint, so
    // Dag::add_sink refuses another sink beside it.
    [[nodiscard]] bool gates_checkpoint_ack() const noexcept override { return true; }
    void flush() override;
    // Discards what is buffered (a restored job writes it again) and releases
    // the connection. Never throws.
    void close() override;
    std::string name() const override { return opts_.name; }

private:
    struct Pending {
        std::string key;  // the stream key in the keyed modes; empty when appending
        std::string row;  // the rendered VALUES row
    };

    [[nodiscard]] std::vector<postgres::PgStatement> plan_(bool one_row_each) const;
    [[nodiscard]] bool linger_elapsed_() const;

    PostgresJsonSinkOptions opts_;
    postgres::PgSinkCore core_;
    std::string head_;
    std::string tail_;
    std::size_t row_capacity_{0};
    std::vector<Pending> pending_;
    std::size_t pending_bytes_{0};
    std::chrono::steady_clock::time_point first_buffered_at_{};
};

}  // namespace clink
