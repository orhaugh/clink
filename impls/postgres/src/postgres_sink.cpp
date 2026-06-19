#include "clink/connectors/postgres_sink.hpp"

#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <utility>
#include <vector>

#include "clink/metrics/connector_metrics.hpp"

#ifdef CLINK_HAS_POSTGRES
#include <libpq-fe.h>
#endif

namespace clink {

#ifdef CLINK_HAS_POSTGRES

struct PostgresSink::Impl {
    Options opts;
    PGconn* conn{nullptr};
    // Pending rows held since the last flush. Each entry is one row of
    // bound parameter values (strings); the libpq call binds them into
    // the prepared statement in order.
    std::vector<std::vector<std::string>> pending;
    std::chrono::steady_clock::time_point last_flush{};
    bool in_transaction{false};
};

bool PostgresSink::is_real_implementation() {
    return true;
}

PostgresSink::PostgresSink(Options opts) : impl_(std::make_unique<Impl>()) {
    impl_->opts = std::move(opts);
    if (impl_->opts.sql.empty()) {
        throw std::invalid_argument("PostgresSink: sql must not be empty");
    }
}

PostgresSink::~PostgresSink() {
    if (impl_ && impl_->conn != nullptr) {
        PQfinish(impl_->conn);
    }
}

void PostgresSink::open() {
    impl_->conn = PQconnectdb(impl_->opts.conninfo.c_str());
    if (PQstatus(impl_->conn) != CONNECTION_OK) {
        const std::string err = PQerrorMessage(impl_->conn);
        PQfinish(impl_->conn);
        impl_->conn = nullptr;
        throw std::runtime_error("PostgresSink::open: " + err);
    }
    impl_->last_flush = std::chrono::steady_clock::now();
}

namespace {

void exec_simple(PGconn* conn, const char* sql) {
    PGresult* r = PQexec(conn, sql);
    const auto status = PQresultStatus(r);
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
        const std::string err = PQerrorMessage(conn);
        PQclear(r);
        throw std::runtime_error("PostgresSink: " + std::string{sql} + " failed: " + err);
    }
    PQclear(r);
}

}  // namespace

void PostgresSink::on_data(const Batch<std::vector<std::string>>& batch) {
    for (const auto& rec : batch) {
        impl_->pending.push_back(rec.value());
    }
    const auto now = std::chrono::steady_clock::now();
    const bool size_trigger = impl_->pending.size() >= impl_->opts.batch_rows;
    const bool time_trigger =
        (now - impl_->last_flush) >= impl_->opts.batch_interval && !impl_->pending.empty();
    if (size_trigger || time_trigger) {
        flush();
    }
}

void PostgresSink::flush() {
    if (impl_->pending.empty() || impl_->conn == nullptr) {
        return;
    }
    const auto t0 = std::chrono::steady_clock::now();
    const auto pending_rows = impl_->pending.size();
    if (!impl_->in_transaction) {
        exec_simple(impl_->conn, "BEGIN");
        impl_->in_transaction = true;
    }
    // Execute one parameterized INSERT per row. libpq handles quoting
    // and escaping via the paramValues array - no SQL injection risk
    // from user-supplied values. A future optimisation would COPY into
    // a temp staging table; for v1 we match per-row execute.
    for (const auto& row : impl_->pending) {
        std::vector<const char*> values;
        values.reserve(row.size());
        for (const auto& v : row) {
            values.push_back(v.c_str());
        }
        PGresult* r = PQexecParams(impl_->conn,
                                   impl_->opts.sql.c_str(),
                                   static_cast<int>(values.size()),
                                   /*paramTypes=*/nullptr,
                                   values.data(),
                                   /*paramLengths=*/nullptr,
                                   /*paramFormats=*/nullptr,
                                   /*resultFormat=*/0);
        const auto status = PQresultStatus(r);
        if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
            const std::string err = PQerrorMessage(impl_->conn);
            PQclear(r);
            // Roll back the in-flight transaction so the partial
            // insert doesn't leak; surface the error so the runner
            // converts it into SubtaskFinished{had_error=true}.
            PGresult* rb = PQexec(impl_->conn, "ROLLBACK");
            PQclear(rb);
            impl_->in_transaction = false;
            impl_->pending.clear();
            clink::metrics::connector::error_inc("postgres");
            throw std::runtime_error("PostgresSink::flush: " + err);
        }
        PQclear(r);
    }
    exec_simple(impl_->conn, "COMMIT");
    impl_->in_transaction = false;
    impl_->pending.clear();
    impl_->last_flush = std::chrono::steady_clock::now();
    const auto dt =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0)
            .count();
    clink::metrics::connector::records_out_inc("postgres", pending_rows);
    clink::metrics::connector::commit_latency_observe("postgres", static_cast<std::uint64_t>(dt));
}

void PostgresSink::close() {
    if (impl_->conn == nullptr) {
        return;
    }
    try {
        flush();
    } catch (...) {
        // Best-effort: ensure the connection is closed even if the
        // final flush failed; the runner already has the error.
    }
    PQfinish(impl_->conn);
    impl_->conn = nullptr;
}

#else  // !CLINK_HAS_POSTGRES

struct PostgresSink::Impl {};

bool PostgresSink::is_real_implementation() {
    return false;
}

PostgresSink::PostgresSink(Options /*opts*/) {
    throw std::runtime_error(
        "PostgresSink: built without libpq. Install postgresql or libpq "
        "(e.g. `brew install libpq`) and reconfigure with CLINK_WITH_POSTGRES=ON.");
}

PostgresSink::~PostgresSink() = default;
void PostgresSink::open() {}
void PostgresSink::on_data(const Batch<std::vector<std::string>>& /*batch*/) {}
void PostgresSink::flush() {}
void PostgresSink::close() {}

#endif

}  // namespace clink
