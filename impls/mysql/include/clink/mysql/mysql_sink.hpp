#pragma once

// MySQL sink: each input record is a JSON-object string (row_to_json_string on
// the SQL path), written as one row of `columns` (a member the record lacks is
// NULL, as a null one is). Two modes:
//   append (default)           INSERT
//   on_duplicate='update'      INSERT ... ON DUPLICATE KEY UPDATE, by the
//                              table's one PRIMARY KEY or UNIQUE index
//
// Buffered rows are written at the count, byte and linger thresholds and at
// every checkpoint barrier, each flush as one transaction of statements no
// longer than the statement bound (see mysql_sink_core.hpp for failures,
// retries and the COMMIT whose outcome is unknown). Delivery is AT-LEAST-ONCE:
// a restored job writes again what its last checkpoint did not cover, so
// append duplicates those rows and on_duplicate='update' writes them again by
// key. Not two-phase commit.
//
// The table is checked when the sink opens: every column must exist and take
// values (not generated), a TIMESTAMP column must be DATETIME or TIMESTAMP,
// and on_duplicate='update' needs exactly one unique index, all of whose
// columns the sink writes. Records are read strictly (one JSON object, numbers
// kept exactly as written), and a record that cannot be written as declared
// fails the sink, as does a retraction (a __row_kind of delete or
// update_before) or an unknown __row_kind.

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "clink/mysql/mysql_client.hpp"
#include "clink/mysql/mysql_sink_core.hpp"
#include "clink/mysql/mysql_sink_rules.hpp"
#include "clink/mysql/mysql_sql.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink::mysql {

struct MysqlSinkOptions {
    ConnectOptions conn;
    std::string table;                 // target table (required)
    std::vector<std::string> columns;  // authoritative projection (required)
    // Columns holding a TIMESTAMP, written as timestamp literals rather than
    // the epoch milliseconds the SQL Row path carries (timestamp_columns).
    sqljson::TimeColumns timestamp_columns;
    bool upsert{false};                       // on_duplicate='update'
    std::vector<std::string> update_columns;  // its SET list (empty = all columns)
    std::size_t batch_records{1000};          // flush at this many buffered rows
    // Flush once the buffered rows' SQL reaches this many bytes; 0 means the
    // default, 16 MiB.
    std::size_t max_bytes{0};
    std::chrono::milliseconds max_age{0};       // linger (0 = off)
    std::size_t max_record_bytes{1U << 20};     // one record's row, as SQL
    std::size_t max_statement_bytes{4U << 20};  // one statement
    std::chrono::seconds lock_wait_timeout{30};
    int max_retries{3};  // after a deadlock or a lock wait timeout
    std::string name{"mysql_sink"};
    MysqlSinkServerFactory server_factory;  // unset: a real server
};

class MysqlSink : public Sink<std::string> {
public:
    static constexpr std::size_t kDefaultMaxBytes = 16U << 20;

    explicit MysqlSink(MysqlSinkOptions opts);

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
    [[nodiscard]] bool linger_elapsed_() const;

    MysqlSinkOptions opts_;
    SinkCore core_;
    std::string head_;
    std::string tail_;
    std::size_t row_capacity_{0};
    std::vector<std::string> pending_;
    std::size_t pending_bytes_{0};
    std::chrono::steady_clock::time_point first_buffered_at_{};
};

}  // namespace clink::mysql
