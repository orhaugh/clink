#pragma once

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "clink/operators/operator_base.hpp"
#include "clink/postgres/pg_sink_server.hpp"

namespace clink {

// PostgresSink - inserts rows into a PostgreSQL table through a user-supplied
// parameterised statement, bound with libpq's PQexecParams.
//
// Each input record is a vector<string> of column values, one per `$N`
// placeholder in `opts.sql`, each bound as text (never NULL). Upstream
// operators shape their typed record into it.
//
// Batching: rows are written at `batch_rows` rows, after `batch_interval`, and
// at every checkpoint barrier, each flush one transaction of one statement per
// row (see clink/postgres/pg_sink_core.hpp for failures, retries and the
// COMMIT whose outcome is unknown). Delivery is AT-LEAST-ONCE: a restored job
// writes again what its last checkpoint did not cover. A failure fails the sink
// until open() is called again; close() discards what is buffered and never
// throws.
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
        // How long a statement waits for a lock, and how many times a flush is
        // tried again after a deadlock, a serialization failure or a lock wait.
        std::chrono::seconds lock_wait_timeout{30};
        int max_retries{3};
        postgres::PgSinkServerFactory server_factory;  // unset: a real server
    };

    explicit PostgresSink(Options opts);
    ~PostgresSink() override;

    PostgresSink(const PostgresSink&) = delete;
    PostgresSink& operator=(const PostgresSink&) = delete;
    PostgresSink(PostgresSink&&) = delete;
    PostgresSink& operator=(PostgresSink&&) = delete;

    void open() override;
    void on_data(const Batch<std::vector<std::string>>& batch) override;
    // A checkpoint barrier commits what is buffered, so no row the checkpoint
    // covers is still waiting for a size or time trigger. That is the sink's
    // guarantee only while it owns its chain's checkpoint, so Dag::add_sink
    // refuses another sink beside it.
    void on_barrier(CheckpointBarrier b) override;
    [[nodiscard]] bool gates_checkpoint_ack() const noexcept override { return true; }
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
