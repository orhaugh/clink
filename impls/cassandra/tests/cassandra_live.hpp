#pragma once

// What the Cassandra live tests share: the server they are pointed at, and a
// session of their own over the driver, independent of the sinks, to create
// tables and to read back whole rows.
//
//   CLINK_CASSANDRA_TEST_CONTACT_POINTS  the server's host (the live tests skip without it)
//   CLINK_CASSANDRA_TEST_PORT            its CQL port (default 9042)
//   CLINK_CASSANDRA_TEST_PRODUCT         "cassandra" or "scylladb", as the gate names it
//   CLINK_CASSANDRA_TEST_VERSION         the version the gate started, checked by a test

#include <cassandra.h>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "clink/cassandra/connection_params.hpp"
#include "clink/config/json.hpp"

namespace clink::cassandra::live {

inline std::string env_or(const char* name, std::string fallback = {}) {
    const char* v = std::getenv(name);
    return v == nullptr || *v == '\0' ? fallback : std::string{v};
}

inline bool configured() {
    return !env_or("CLINK_CASSANDRA_TEST_CONTACT_POINTS").empty();
}
inline std::string contact_points() {
    return env_or("CLINK_CASSANDRA_TEST_CONTACT_POINTS");
}
inline int port() {
    return std::stoi(env_or("CLINK_CASSANDRA_TEST_PORT", "9042"));
}
inline std::string product() {
    return env_or("CLINK_CASSANDRA_TEST_PRODUCT", "cassandra");
}
inline bool scylladb() {
    return product() == "scylladb";
}

// Connection settings for a sink under test, pointed at the live server.
inline CassandraConnParams conn() {
    CassandraConnParams c;
    c.contact_points = contact_points();
    c.port = port();
    c.connect_timeout_ms = 10000;
    return c;
}

// A driver session of the test's own.
class Session {
public:
    explicit Session(std::string hosts = contact_points(), int p = port()) {
        cluster_ = cass_cluster_new();
        cass_cluster_set_contact_points(cluster_, hosts.c_str());
        cass_cluster_set_port(cluster_, p);
        cass_cluster_set_connect_timeout(cluster_, 10000);
        cass_cluster_set_request_timeout(cluster_, 30000);
        session_ = cass_session_new();
        CassFuture* f = cass_session_connect(session_, cluster_);
        cass_future_wait(f);
        const CassError e = cass_future_error_code(f);
        cass_future_free(f);
        if (e != CASS_OK) {
            cass_session_free(session_);
            cass_cluster_free(cluster_);
            throw std::runtime_error(std::string{"live session: cannot connect: "} +
                                     cass_error_desc(e));
        }
    }
    ~Session() {
        CassFuture* f = cass_session_close(session_);
        cass_future_wait(f);
        cass_future_free(f);
        cass_session_free(session_);
        cass_cluster_free(cluster_);
    }
    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;

    // Runs one statement; returns the driver's error description, empty on success.
    std::string exec(const std::string& cql) {
        CassStatement* st = cass_statement_new(cql.c_str(), 0);
        CassFuture* f = cass_session_execute(session_, st);
        cass_future_wait(f);
        std::string err;
        if (const CassError e = cass_future_error_code(f); e != CASS_OK) {
            const char* msg = nullptr;
            std::size_t len = 0;
            cass_future_error_message(f, &msg, &len);
            err = std::string{cass_error_desc(e)} + ": " + std::string(msg, len);
        }
        cass_future_free(f);
        cass_statement_free(st);
        return err;
    }

    void must(const std::string& cql) {
        if (const auto err = exec(cql); !err.empty()) {
            throw std::runtime_error("live session: " + cql + ": " + err);
        }
    }

    // The first column of every row of `cql`, as text: a text column's value,
    // or a bigint's decimal digits; nullopt for null.
    std::vector<std::optional<std::string>> column(const std::string& cql) {
        CassStatement* st = cass_statement_new(cql.c_str(), 0);
        cass_statement_set_paging_size(st, -1);
        CassFuture* f = cass_session_execute(session_, st);
        cass_future_wait(f);
        std::vector<std::optional<std::string>> out;
        if (const CassError e = cass_future_error_code(f); e != CASS_OK) {
            cass_future_free(f);
            cass_statement_free(st);
            throw std::runtime_error("live session: " + cql + ": " + cass_error_desc(e));
        }
        const CassResult* res = cass_future_get_result(f);
        CassIterator* it = cass_iterator_from_result(res);
        while (cass_iterator_next(it) == cass_true) {
            const CassValue* v = cass_row_get_column(cass_iterator_get_row(it), 0);
            if (cass_value_is_null(v) == cass_true) {
                out.emplace_back(std::nullopt);
                continue;
            }
            const char* s = nullptr;
            std::size_t n = 0;
            cass_int64_t i64 = 0;
            if (cass_value_get_string(v, &s, &n) == CASS_OK) {
                out.emplace_back(std::string(s, n));
            } else if (cass_value_get_int64(v, &i64) == CASS_OK) {
                out.emplace_back(std::to_string(i64));
            } else {
                out.emplace_back("<unreadable>");
            }
        }
        cass_iterator_free(it);
        cass_result_free(res);
        cass_future_free(f);
        cass_statement_free(st);
        return out;
    }

    // Every row of a table, each as the server writes it with SELECT JSON.
    std::vector<clink::config::JsonValue> rows(const std::string& table) {
        std::vector<clink::config::JsonValue> out;
        for (const auto& r : column("SELECT JSON * FROM " + table)) {
            if (r) {
                out.push_back(clink::config::parse(*r));
            }
        }
        return out;
    }

private:
    CassCluster* cluster_{nullptr};
    CassSession* session_{nullptr};
};

inline constexpr const char* kKeyspace = "clink_it";

// Creates the shared keyspace (replication factor 1) and a fresh table.
inline void fresh_table(Session& s, const std::string& table, const std::string& columns) {
    s.must(std::string{"CREATE KEYSPACE IF NOT EXISTS "} + kKeyspace +
           " WITH replication = {'class':'SimpleStrategy','replication_factor':1}");
    s.must(std::string{"DROP TABLE IF EXISTS "} + kKeyspace + "." + table);
    s.must(std::string{"CREATE TABLE "} + kKeyspace + "." + table + " (" + columns + ")");
}

}  // namespace clink::cassandra::live
