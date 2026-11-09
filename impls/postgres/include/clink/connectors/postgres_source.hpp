#pragma once

#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>

#include "clink/connectors/postgres_row.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink {

// PostgresSource emits rows from a SELECT query against a Postgres database.
//
// MVP behaviour: snapshot mode. open() runs the query; produce() batches
// the result rows and emits them; once exhausted the source closes. This is
// sufficient for "read these dimension tables and use them as enrichment
// data" - the case the test scenario exercises.
//
// A future revision will add streaming mode via logical replication slots
// (pgoutput / wal2json) without changing the API on this class - emitted
// rows are PostgresRow either way.
//
// Implementation lives in src/connectors/postgres_source.cpp behind
// `CLINK_HAS_POSTGRES`. When CMake doesn't find PostgreSQL, construction
// throws - same shape as RocksDB, Kafka, S3, ClickHouse.
class PostgresSource final : public Source<PostgresRow> {
public:
    struct Options {
        // libpq connection string ("host=... port=... user=... dbname=...")
        std::string conninfo;
        // The SELECT to run. Single-statement, no trailing semicolon needed.
        std::string query;
        // Rows emitted per produce() call. Smaller batches give more
        // responsive backpressure; larger batches reduce per-batch overhead.
        std::size_t batch_size{256};
        // Reserved for streaming mode. In snapshot mode this is ignored.
        std::optional<std::chrono::milliseconds> refresh_interval{};
    };

    explicit PostgresSource(Options opts);
    ~PostgresSource() override;

    PostgresSource(const PostgresSource&) = delete;
    PostgresSource& operator=(const PostgresSource&) = delete;
    PostgresSource(PostgresSource&&) = delete;
    PostgresSource& operator=(PostgresSource&&) = delete;

    void open() override;
    bool produce(Emitter<PostgresRow>& out) override;
    void close() override;

    // A SELECT materialises a finite result set (BATCH-1).
    [[nodiscard]] bool is_bounded() const noexcept override { return true; }

    // Source replay. The checkpoint holds how many rows were emitted and a hash
    // of them. open() runs the query again and resumes after those rows only
    // when its first rows hash the same, which holds for a deterministically
    // ordered query (ORDER BY on a unique key) over rows unchanged up to there;
    // otherwise, or from an earlier release's checkpoint, which holds no hash,
    // it emits every row again. Rows are repeated rather than skipped: at least
    // once, and exactly once at the source when the rows are the same.
    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId ckpt) override;
    bool restore_offset(StateBackend& backend, OperatorId op_id) override;

    std::string name() const override { return "postgres_source"; }

    static bool is_real_implementation();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink
