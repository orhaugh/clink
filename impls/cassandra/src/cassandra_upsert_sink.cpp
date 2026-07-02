#include "clink/cassandra/cassandra_upsert_sink.hpp"

#include <cassandra.h>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <map>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "clink/config/json.hpp"
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

// Format a JSON scalar as a CQL literal for a WHERE clause. Cassandra coerces the
// literal to the column's declared type, so a bare number works for int/bigint
// and a quoted string for text. A primary-key cell is never null.
std::string cql_literal(const clink::config::JsonValue& v, const std::string& ctx) {
    if (v.is_bool()) {
        return v.as_bool() ? "true" : "false";
    }
    if (v.is_number()) {
        const double n = v.as_number();
        if (n == static_cast<double>(static_cast<std::int64_t>(n))) {
            return std::to_string(static_cast<std::int64_t>(n));
        }
        char buf[32];
        std::snprintf(buf, sizeof(buf), "%.17g", n);
        return std::string{buf};
    }
    if (v.is_string()) {
        std::string out = "'";
        for (char c : v.as_string()) {
            if (c == '\'') {
                out += "''";  // CQL escapes a single quote by doubling it
            } else {
                out += c;
            }
        }
        out += "'";
        return out;
    }
    throw std::runtime_error(ctx + ": primary-key value is not a scalar");
}
}  // namespace

struct CassandraUpsertSink::Impl {
    Options opts;
    CassCluster* cluster{nullptr};
    CassSession* session{nullptr};
    const CassPrepared* insert_prepared{nullptr};
    std::string table_fqn;  // "<keyspace>.<table>"

    struct Entry {
        bool is_delete{false};
        std::string json;
    };
    std::map<std::string, Entry> netted_;  // primary-key tuple -> latest op

    explicit Impl(Options o) : opts(std::move(o)) {}
};

CassandraUpsertSink::CassandraUpsertSink(Options opts)
    : impl_(std::make_unique<Impl>(std::move(opts))) {
    if (impl_->opts.keyspace.empty() || impl_->opts.table.empty()) {
        throw std::runtime_error(impl_->opts.name + ": 'keyspace' and 'table' are required");
    }
    if (impl_->opts.key_columns.empty()) {
        throw std::runtime_error(impl_->opts.name +
                                 ": 'key_columns' (the primary key) is required");
    }
    impl_->table_fqn = impl_->opts.keyspace + "." + impl_->opts.table;
}

CassandraUpsertSink::~CassandraUpsertSink() {
    if (impl_->insert_prepared != nullptr) {
        cass_prepared_free(impl_->insert_prepared);
    }
    if (impl_->session != nullptr) {
        cass_session_free(impl_->session);
    }
    if (impl_->cluster != nullptr) {
        cass_cluster_free(impl_->cluster);
    }
}

void CassandraUpsertSink::open() {
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

    const std::string query = "INSERT INTO " + impl_->table_fqn + " JSON ?";
    CassFuture* pf = cass_session_prepare(impl_->session, query.c_str());
    check_future(pf, impl_->opts.name + ": prepare");
    impl_->insert_prepared = cass_future_get_prepared(pf);
    cass_future_free(pf);
}

void CassandraUpsertSink::on_data(const Batch<std::string>& batch) {
    if (impl_->insert_prepared == nullptr) {
        throw std::runtime_error(impl_->opts.name + ": on_data before open()");
    }
    for (const auto& rec : batch) {
        const std::string& json = rec.value();
        auto j = clink::config::parse(json);
        if (!j.is_object()) {
            throw std::runtime_error(impl_->opts.name + ": sink row is not a JSON object: " + json);
        }
        auto& obj = j.as_object();

        bool is_delete = false;
        if (auto it = obj.find("__row_kind"); it != obj.end() && it->second.is_string()) {
            const std::string& k = it->second.as_string();
            is_delete = (k == "delete" || k == "update_before");
        }

        std::string key;
        for (const auto& kc : impl_->opts.key_columns) {
            auto it = obj.find(kc);
            if (it == obj.end() || it->second.is_null()) {
                throw std::runtime_error(
                    impl_->opts.name + ": changelog row missing primary key '" + kc + "': " + json);
            }
            key += it->second.serialize(0);
            key.push_back('\x1f');
        }
        // Strip the synthetic marker: `INSERT INTO t JSON ?` rejects a JSON object
        // with a field that is not a table column (__row_kind is not one). The
        // primary-key columns remain for the DELETE path.
        obj.erase("__row_kind");
        impl_->netted_[std::move(key)] = Impl::Entry{is_delete, j.serialize(0)};
    }
}

void CassandraUpsertSink::on_barrier(CheckpointBarrier /*b*/) {
    flush();
}

void CassandraUpsertSink::flush() {
    if (impl_->netted_.empty()) {
        return;
    }
    const auto t0 = std::chrono::steady_clock::now();
    const std::size_t n = impl_->netted_.size();
    std::vector<CassFuture*> pending;
    pending.reserve(n);
    try {
        for (auto& [key, entry] : impl_->netted_) {
            (void)key;
            if (entry.is_delete) {
                // CQL has no multi-row IN over a composite key: one DELETE per key.
                const auto j = clink::config::parse(entry.json);
                const auto& obj = j.as_object();
                std::string cql = "DELETE FROM " + impl_->table_fqn + " WHERE ";
                for (std::size_t i = 0; i < impl_->opts.key_columns.size(); ++i) {
                    const auto& kc = impl_->opts.key_columns[i];
                    cql += kc + "=" + cql_literal(obj.at(kc), impl_->opts.name);
                    if (i + 1 < impl_->opts.key_columns.size()) {
                        cql += " AND ";
                    }
                }
                CassStatement* st = cass_statement_new(cql.c_str(), 0);
                pending.push_back(cass_session_execute(impl_->session, st));
                cass_statement_free(st);
            } else {
                CassStatement* st = cass_prepared_bind(impl_->insert_prepared);
                const CassError be =
                    cass_statement_bind_string_n(st, 0, entry.json.data(), entry.json.size());
                if (be != CASS_OK) {
                    cass_statement_free(st);
                    throw std::runtime_error(impl_->opts.name + ": bind: " + cass_error_desc(be));
                }
                pending.push_back(cass_session_execute(impl_->session, st));
                cass_statement_free(st);
            }
        }
    } catch (...) {
        for (CassFuture* f : pending) {
            cass_future_free(f);
        }
        impl_->netted_.clear();
        clink::metrics::connector::error_inc(kLabel, "sink");
        throw;
    }

    CassError first = CASS_OK;
    std::string first_msg;
    for (CassFuture* f : pending) {
        cass_future_wait(f);
        const CassError e = cass_future_error_code(f);
        if (e != CASS_OK && first == CASS_OK) {
            first = e;
            first_msg = future_error_message(f);
        }
        cass_future_free(f);
    }
    impl_->netted_.clear();
    if (first != CASS_OK) {
        clink::metrics::connector::error_inc(kLabel, "sink");
        throw std::runtime_error(impl_->opts.name + ": apply: " + cass_error_desc(first) +
                                 (first_msg.empty() ? "" : " - " + first_msg));
    }
    clink::metrics::connector::records_out_inc(kLabel, n);
    const auto dt =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0)
            .count();
    clink::metrics::connector::commit_latency_observe(kLabel, static_cast<std::uint64_t>(dt));
}

void CassandraUpsertSink::close() {
    if (impl_->session != nullptr) {
        flush();
    }
    if (impl_->insert_prepared != nullptr) {
        cass_prepared_free(impl_->insert_prepared);
        impl_->insert_prepared = nullptr;
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

std::string CassandraUpsertSink::name() const {
    return impl_->opts.name;
}

}  // namespace clink::cassandra
