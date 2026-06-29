#include "clink/cassandra/cassandra_sink.hpp"

#include <cassandra.h>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "clink/metrics/connector_metrics.hpp"

namespace clink::cassandra {

namespace {
constexpr const char* kLabel = "cassandra";

std::string future_error_message(CassFuture* f) {
    const char* msg = nullptr;
    std::size_t len = 0;
    cass_future_error_message(f, &msg, &len);
    return std::string(msg, len);
}

// Wait for a future and throw if it failed (used for connect / prepare).
void check_future(CassFuture* f, const std::string& ctx) {
    cass_future_wait(f);
    const CassError e = cass_future_error_code(f);
    if (e != CASS_OK) {
        const std::string m = future_error_message(f);
        throw std::runtime_error(ctx + ": " + cass_error_desc(e) + (m.empty() ? "" : " - " + m));
    }
}

void check_err(CassError e, const std::string& ctx) {
    if (e != CASS_OK) {
        throw std::runtime_error(ctx + ": " + cass_error_desc(e));
    }
}
}  // namespace

struct CassandraSink::Impl {
    Options opts;
    CassCluster* cluster{nullptr};
    CassSession* session{nullptr};
    const CassPrepared* prepared{nullptr};
    std::vector<CassFuture*> pending;  // async INSERTs awaited at flush()
    explicit Impl(Options o) : opts(std::move(o)) {}
};

CassandraSink::CassandraSink(Options opts) : impl_(std::make_unique<Impl>(std::move(opts))) {
    if (impl_->opts.keyspace.empty() || impl_->opts.table.empty()) {
        throw std::runtime_error(impl_->opts.name + ": 'keyspace' and 'table' are required");
    }
}

CassandraSink::~CassandraSink() {
    for (CassFuture* f : impl_->pending) {
        cass_future_free(f);  // no wait in the dtor; the driver completes them in the background
    }
    impl_->pending.clear();
    if (impl_->prepared != nullptr) {
        cass_prepared_free(impl_->prepared);
    }
    if (impl_->session != nullptr) {
        cass_session_free(impl_->session);
    }
    if (impl_->cluster != nullptr) {
        cass_cluster_free(impl_->cluster);
    }
}

void CassandraSink::open() {
    const auto& c = impl_->opts.conn;
    impl_->cluster = cass_cluster_new();
    check_err(cass_cluster_set_contact_points(impl_->cluster, c.contact_points.c_str()),
              impl_->opts.name + ": contact_points");
    check_err(cass_cluster_set_port(impl_->cluster, c.port), impl_->opts.name + ": port");
    cass_cluster_set_connect_timeout(impl_->cluster, static_cast<unsigned>(c.connect_timeout_ms));
    if (!c.username.empty()) {
        cass_cluster_set_credentials(impl_->cluster, c.username.c_str(), c.password.c_str());
    }
    impl_->session = cass_session_new();
    CassFuture* cf = cass_session_connect(impl_->session, impl_->cluster);
    check_future(cf, impl_->opts.name + ": connect");
    cass_future_free(cf);

    // Cassandra's native JSON insert maps the JSON object's fields to columns by name.
    const std::string query =
        "INSERT INTO " + impl_->opts.keyspace + "." + impl_->opts.table + " JSON ?";
    CassFuture* pf = cass_session_prepare(impl_->session, query.c_str());
    check_future(pf, impl_->opts.name + ": prepare");
    impl_->prepared = cass_future_get_prepared(pf);
    cass_future_free(pf);
}

void CassandraSink::on_data(const Batch<std::string>& batch) {
    if (impl_->prepared == nullptr) {
        throw std::runtime_error(impl_->opts.name + ": on_data before open()");
    }
    std::size_t bytes = 0;
    for (const auto& rec : batch) {
        const std::string& payload = rec.value();
        CassStatement* st = cass_prepared_bind(impl_->prepared);
        const CassError be = cass_statement_bind_string_n(st, 0, payload.data(), payload.size());
        if (be != CASS_OK) {
            cass_statement_free(st);
            clink::metrics::connector::error_inc(kLabel, "sink");
            throw std::runtime_error(impl_->opts.name + ": bind: " + cass_error_desc(be));
        }
        CassFuture* f = cass_session_execute(impl_->session, st);
        cass_statement_free(st);  // the future owns the execution; the statement can be freed now
        impl_->pending.push_back(f);
        bytes += payload.size();
    }
    if (!batch.empty()) {
        clink::metrics::connector::records_out_inc(kLabel, batch.size());
        clink::metrics::connector::bytes_out_inc(kLabel, bytes);
    }
}

void CassandraSink::on_barrier(CheckpointBarrier /*b*/) {
    flush();  // align durable delivery to the checkpoint (at-least-once)
}

void CassandraSink::flush() {
    if (impl_->pending.empty()) {
        return;
    }
    const auto t0 = std::chrono::steady_clock::now();
    CassError first = CASS_OK;
    std::string first_msg;
    for (CassFuture* f : impl_->pending) {
        cass_future_wait(f);
        const CassError e = cass_future_error_code(f);
        if (e != CASS_OK && first == CASS_OK) {
            first = e;
            first_msg = future_error_message(f);
        }
        cass_future_free(f);
    }
    impl_->pending.clear();
    if (first != CASS_OK) {
        clink::metrics::connector::error_inc(kLabel, "sink");
        throw std::runtime_error(impl_->opts.name + ": insert: " + cass_error_desc(first) +
                                 (first_msg.empty() ? "" : " - " + first_msg));
    }
    const auto dt =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0)
            .count();
    clink::metrics::connector::commit_latency_observe(kLabel, static_cast<std::uint64_t>(dt));
}

void CassandraSink::close() {
    if (impl_->session != nullptr) {
        flush();  // confirm anything inserted since the last barrier (may throw)
    }
    if (impl_->prepared != nullptr) {
        cass_prepared_free(impl_->prepared);
        impl_->prepared = nullptr;
    }
    if (impl_->session != nullptr) {
        CassFuture* cf = cass_session_close(impl_->session);
        cass_future_wait(cf);
        cass_future_free(cf);
        cass_session_free(impl_->session);
        impl_->session = nullptr;
    }
    if (impl_->cluster != nullptr) {
        cass_cluster_free(impl_->cluster);
        impl_->cluster = nullptr;
    }
}

std::string CassandraSink::name() const {
    return impl_->opts.name;
}

}  // namespace clink::cassandra
