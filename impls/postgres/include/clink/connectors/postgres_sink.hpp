#pragma once

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "clink/operators/operator_base.hpp"

namespace clink {

// PostgresSink - the JdbcSink. Inserts
// rows into a Postgres table via a user-supplied parameterized SQL
// statement using libpq's PQexecParams binding.
//
// Each input record is a vector<string> of pre-bound column values,
// one per `$N` placeholder in `opts.sql`. Upstream operators are
// responsible for converting their typed record into the right shape
// (commonly via a MapOperator that knows the schema).
//
// Batching: at most `batch_rows` rows or `batch_interval` worth of
// time per transaction. On flush() we COMMIT; on close() we COMMIT and
// disconnect. Errors during execution propagate as exceptions matching
// the contract of other clink sinks (ClickHouseSink, S3Sink).
//
// Build: requires CLINK_HAS_POSTGRES (libpq). When unavailable the
// constructor throws a configuration error so submission fails loudly.
class PostgresSink final : public Sink<std::vector<std::string>> {
public:
    struct Options {
        // libpq conninfo, same shape as PostgresSource.
        std::string conninfo;
        // INSERT INTO ... VALUES ($1, $2, ...) - the bound vector
        // determines parameter count; mismatched cardinality throws.
        std::string sql;
        // Flush thresholds. Flushed on whichever fires first.
        std::size_t batch_rows{1000};
        std::chrono::milliseconds batch_interval{std::chrono::seconds{1}};
    };

    explicit PostgresSink(Options opts);
    ~PostgresSink() override;

    PostgresSink(const PostgresSink&) = delete;
    PostgresSink& operator=(const PostgresSink&) = delete;
    PostgresSink(PostgresSink&&) = delete;
    PostgresSink& operator=(PostgresSink&&) = delete;

    void open() override;
    void on_data(const Batch<std::vector<std::string>>& batch) override;
    void flush() override;
    void close() override;

    std::string name() const override { return "postgres_sink"; }

    // True when built with CLINK_HAS_POSTGRES. Tests gate themselves
    // on this so the CI matrix without libpq doesn't fail.
    static bool is_real_implementation();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink
