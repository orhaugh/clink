#pragma once

// Postgres JSON sink (M4): each input record is a JSON-object string
// (row_to_json_string on the SQL path); buffered records are written as ONE
// batched multi-row INSERT via libpq. `columns` (required, or derived from the
// table schema on the SQL path) is the authoritative projection; a column absent
// from / null in a row becomes SQL NULL. on_conflict='update' (with
// conflict_columns) appends ON CONFLICT (...) DO UPDATE for idempotent
// insert-or-update by key; on_conflict='nothing' skips conflicts.
//
// Delivery is AT-LEAST-ONCE: a replay after a failed checkpoint re-runs the
// INSERT (append duplicates; ON CONFLICT-by-key is idempotent = effectively-once).
// This is the newer string-channel + JSON pattern (cf. mysql_sink), distinct from
// the older PostgresSink (a vector<string> + parameterised-$N JDBC sink).

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <libpq-fe.h>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "clink/connectors/postgres_sql.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink {

struct PostgresJsonSinkOptions {
    std::string conninfo;                       // libpq conninfo (required)
    std::string table;                          // target table (required)
    std::vector<std::string> columns;           // authoritative projection (required)
    std::string on_conflict;                    // "" | "update" | "nothing"
    std::vector<std::string> conflict_columns;  // ON CONFLICT target (required if on_conflict set)
    std::vector<std::string> update_columns;    // DO UPDATE SET list (empty = all non-key)
    std::size_t batch_records{1000};
    std::size_t max_bytes{0};              // byte-based flush threshold (0 = off)
    std::chrono::milliseconds max_age{0};  // linger (0 = off)
    std::string name{"postgres_sink"};
};

class PostgresJsonSink : public Sink<std::string> {
public:
    explicit PostgresJsonSink(PostgresJsonSinkOptions opts) : opts_(std::move(opts)) {
        if (opts_.conninfo.empty()) {
            throw std::runtime_error(opts_.name + ": 'conninfo' is required");
        }
        if (opts_.table.empty()) {
            throw std::runtime_error(opts_.name + ": 'table' is required");
        }
        if (opts_.columns.empty()) {
            throw std::runtime_error(opts_.name + ": 'columns' is required (the projection)");
        }
        if (opts_.batch_records == 0) {
            opts_.batch_records = 1;
        }
        // Fail fast on bad identifiers / on_conflict config (build_insert_sql also
        // validates, but at first flush).
        (void)pgsql::quote_ident(opts_.table);
        for (const auto& c : opts_.columns) {
            (void)pgsql::quote_ident(c);
        }
        if (opts_.on_conflict == "update" || opts_.on_conflict == "nothing") {
            if (opts_.conflict_columns.empty()) {
                throw std::runtime_error(opts_.name + ": on_conflict requires conflict_columns");
            }
            for (const auto& c : opts_.conflict_columns) {
                (void)pgsql::quote_ident(c);
            }
        } else if (!opts_.on_conflict.empty()) {
            throw std::runtime_error(opts_.name + ": on_conflict must be 'update' or 'nothing'");
        }
        for (const auto& c : opts_.update_columns) {
            (void)pgsql::quote_ident(c);
        }
    }

    void open() override {
        conn_ = PQconnectdb(opts_.conninfo.c_str());
        if (PQstatus(conn_) != CONNECTION_OK) {
            const std::string err = PQerrorMessage(conn_);
            PQfinish(conn_);
            conn_ = nullptr;
            throw std::runtime_error(opts_.name + ": connect failed: " + err);
        }
    }

    void on_data(const Batch<std::string>& batch) override {
        for (const auto& rec : batch) {
            if (pending_.empty()) {
                first_buffered_at_ = std::chrono::steady_clock::now();
            }
            pending_bytes_ += rec.value().size();
            pending_.push_back(rec.value());
            if (pending_.size() >= opts_.batch_records ||
                (opts_.max_bytes > 0 && pending_bytes_ >= opts_.max_bytes) || linger_elapsed_()) {
                flush();
            }
        }
    }

    void on_barrier(CheckpointBarrier /*b*/) override { flush(); }

    void flush() override {
        if (pending_.empty()) {
            return;
        }
        if (conn_ == nullptr) {
            throw std::runtime_error(opts_.name + ": flush() before open()");
        }
        const std::size_t n = pending_.size();
        const std::size_t bytes = pending_bytes_;
        const auto t0 = std::chrono::steady_clock::now();
        try {
            const std::string sql =
                pgsql::build_insert_sql(opts_.table,
                                        opts_.columns,
                                        opts_.on_conflict,
                                        opts_.conflict_columns,
                                        opts_.update_columns,
                                        pending_,
                                        [this](std::string_view s) { return escape_(s); });
            PGresult* res = PQexec(conn_, sql.c_str());
            const bool ok = res != nullptr && PQresultStatus(res) == PGRES_COMMAND_OK;
            if (!ok) {
                const std::string err =
                    res != nullptr ? PQresultErrorMessage(res) : std::string(PQerrorMessage(conn_));
                PQclear(res);
                throw std::runtime_error(opts_.name + ": INSERT failed: " + err);
            }
            PQclear(res);
        } catch (...) {
            clink::metrics::connector::error_inc("postgres", "sink");
            pending_.clear();
            pending_bytes_ = 0;
            PQfinish(conn_);  // drop a possibly-broken connection; re-open reconnects
            conn_ = nullptr;
            throw;  // job replays from the last checkpoint (at-least-once)
        }
        const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
        clink::metrics::connector::records_out_inc("postgres", n);
        clink::metrics::connector::bytes_out_inc("postgres", bytes);
        clink::metrics::connector::commit_latency_observe("postgres",
                                                          static_cast<std::uint64_t>(dt));
        pending_.clear();
        pending_bytes_ = 0;
    }

    void close() override {
        if (conn_ != nullptr) {
            flush();
            PQfinish(conn_);
            conn_ = nullptr;
        }
    }

    std::string name() const override { return opts_.name; }

private:
    // Charset-correct escaping of a string VALUE for inside single quotes
    // (PQescapeStringConn respects the connection's standard_conforming_strings).
    std::string escape_(std::string_view in) {
        std::string out(in.size() * 2 + 1, '\0');
        int err = 0;
        const std::size_t len = PQescapeStringConn(conn_, out.data(), in.data(), in.size(), &err);
        out.resize(len);
        return out;  // even on err the output is a safe escaped string (per libpq)
    }

    bool linger_elapsed_() const {
        return opts_.max_age.count() > 0 && !pending_.empty() &&
               std::chrono::steady_clock::now() - first_buffered_at_ >= opts_.max_age;
    }

    PostgresJsonSinkOptions opts_;
    PGconn* conn_{nullptr};
    std::vector<std::string> pending_;
    std::size_t pending_bytes_{0};
    std::chrono::steady_clock::time_point first_buffered_at_{};
};

}  // namespace clink
