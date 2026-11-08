// PostgresSink: rows bound into a parameterised statement. See postgres_sink.hpp.

#include "clink/connectors/postgres_sink.hpp"

#include <chrono>
#include <stdexcept>
#include <utility>
#include <vector>

#include "clink/fault/fault_injection.hpp"
#include "clink/postgres/pg_sink_core.hpp"

#include "fault_points.hpp"

namespace clink {

namespace {

postgres::PgSinkCoreOptions core_options(const PostgresSink::Options& o) {
    postgres::PgSinkCoreOptions c;
    c.conninfo = o.conninfo;
    c.lock_wait_timeout = o.lock_wait_timeout;
    c.max_retries = o.max_retries;
    c.name = "postgres_sink";
    c.server_factory = o.server_factory;
    return c;
}

}  // namespace

struct PostgresSink::Impl {
    explicit Impl(Options o) : opts(std::move(o)), core(core_options(opts)) {}
    Options opts;
    postgres::PgSinkCore core;
    // Rows held since the last flush, each one row of bound values.
    std::vector<std::vector<std::string>> pending;
    std::chrono::steady_clock::time_point last_flush{};
};

bool PostgresSink::is_real_implementation() {
    return true;
}

PostgresSink::PostgresSink(Options opts) {
    if (opts.sql.empty()) {
        throw std::invalid_argument("PostgresSink: sql must not be empty");
    }
    if (opts.batch_rows == 0) {
        opts.batch_rows = 1;
    }
    if (opts.lock_wait_timeout.count() < 1 || opts.lock_wait_timeout.count() > 3600) {
        throw std::invalid_argument(
            "PostgresSink: the lock wait timeout must be 1 to 3600 seconds");
    }
    if (opts.max_retries < 0 || opts.max_retries > 10) {
        throw std::invalid_argument("PostgresSink: max_retries must be 0 to 10");
    }
    impl_ = std::make_unique<Impl>(std::move(opts));
}

PostgresSink::~PostgresSink() = default;

void PostgresSink::open() {
    impl_->pending.clear();
    impl_->core.open(runtime());
    impl_->last_flush = std::chrono::steady_clock::now();
}

void PostgresSink::on_data(const Batch<std::vector<std::string>>& batch) {
    impl_->core.require_usable("a record");
    for (const auto& rec : batch) {
        impl_->pending.push_back(rec.value());
    }
    const bool size_trigger = impl_->pending.size() >= impl_->opts.batch_rows;
    const bool time_trigger =
        !impl_->pending.empty() &&
        std::chrono::steady_clock::now() - impl_->last_flush >= impl_->opts.batch_interval;
    if (size_trigger || time_trigger) {
        flush();
    }
}

void PostgresSink::on_barrier(CheckpointBarrier /*b*/) {
    impl_->core.require_usable("a checkpoint barrier");
    const bool writes = !impl_->pending.empty();
    flush();
    if (writes) {
        (void)CLINK_FAULT_POINT(postgres::points::kSinkAfterCommit);
    }
}

void PostgresSink::flush() {
    impl_->core.require_not_failed("a flush");
    if (impl_->pending.empty()) {
        return;
    }
    const auto plan = [this](bool /*one_row_each*/) {
        std::vector<postgres::PgStatement> out;
        out.reserve(impl_->pending.size());
        for (const auto& row : impl_->pending) {
            postgres::PgStatement s;
            s.sql = impl_->opts.sql;
            s.params.emplace(row.begin(), row.end());
            out.push_back(std::move(s));
        }
        return out;
    };
    impl_->core.write(plan, impl_->pending.size(), 0);
    impl_->pending.clear();
    impl_->last_flush = std::chrono::steady_clock::now();
}

void PostgresSink::close() {
    impl_->pending.clear();
    impl_->core.close();
}

}  // namespace clink
