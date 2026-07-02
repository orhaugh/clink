#pragma once

// PostgresJsonUpsertSink - changelog-aware Postgres sink for mode='upsert'.
//
// Consumes the clink changelog convention (a "__row_kind" field on each JSON
// row) and maintains a Postgres table by PRIMARY KEY:
//   insert / update_after  -> INSERT ... ON CONFLICT (<pk>) DO UPDATE  (upsert)
//   delete / update_before -> DELETE FROM <table> WHERE <pk> IN (...)  (tombstone)
//   (a row with no __row_kind is an implicit insert)
//
// Within a flush interval the changelog is netted by primary key (last op wins),
// then the surviving upserts and deletes are applied together in one
// transaction. This is EFFECTIVELY-ONCE on the sink table for a stable primary
// key and a deterministic defining query: every applied statement is keyed and
// idempotent, so a replay after a failed checkpoint converges the table to the
// same final state. It is NOT two-phase-commit (for that use
// delivery_guarantee='exactly_once', the postgres_2pc_sink).
//
// This is what lets a RETRACTING SQL query (GROUP BY, TOP-N, outer join) maintain
// a Postgres table: the append-only postgres_sink would drop the delete/retract
// records; this sink applies them.
//
// Decoupled from the SQL layer: it reads the "__row_kind" wire field from the
// JSON directly rather than depending on clink/sql/row_kind.hpp.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <libpq-fe.h>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/connectors/postgres_json_sink.hpp"  // PgconnDeleter
#include "clink/connectors/postgres_sql.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink {

struct PostgresJsonUpsertSinkOptions {
    std::string conninfo;                  // libpq conninfo (required)
    std::string table;                     // target table (required)
    std::vector<std::string> columns;      // full projection for the upsert (required)
    std::vector<std::string> key_columns;  // PRIMARY KEY - conflict target + delete key (required)
    std::size_t batch_records{1000};
    std::string name{"postgres_upsert_sink"};
};

class PostgresJsonUpsertSink : public Sink<std::string> {
public:
    explicit PostgresJsonUpsertSink(PostgresJsonUpsertSinkOptions opts) : opts_(std::move(opts)) {
        if (opts_.conninfo.empty()) {
            throw std::runtime_error(opts_.name + ": 'conninfo' is required");
        }
        if (opts_.table.empty()) {
            throw std::runtime_error(opts_.name + ": 'table' is required");
        }
        if (opts_.columns.empty()) {
            throw std::runtime_error(opts_.name + ": 'columns' is required (the projection)");
        }
        if (opts_.key_columns.empty()) {
            throw std::runtime_error(opts_.name + ": 'key_columns' (the primary key) is required");
        }
        if (opts_.batch_records == 0) {
            opts_.batch_records = 1;
        }
        (void)pgsql::quote_ident(opts_.table);
        for (const auto& c : opts_.columns) {
            (void)pgsql::quote_ident(c);
        }
        for (const auto& c : opts_.key_columns) {
            (void)pgsql::quote_ident(c);
        }
    }

    void open() override {
        PGconn* c = PQconnectdb(opts_.conninfo.c_str());
        if (PQstatus(c) != CONNECTION_OK) {
            const std::string err = PQerrorMessage(c);
            PQfinish(c);
            throw std::runtime_error(opts_.name + ": connect failed: " + err);
        }
        conn_.reset(c);
    }

    void on_data(const Batch<std::string>& batch) override {
        for (const auto& rec : batch) {
            net_(rec.value());
        }
        if (netted_.size() >= opts_.batch_records) {
            flush();
        }
    }

    void on_barrier(CheckpointBarrier /*b*/) override { flush(); }

    void flush() override {
        if (netted_.empty()) {
            return;
        }
        if (!conn_) {
            throw std::runtime_error(opts_.name + ": flush() before open()");
        }
        std::vector<std::string> upserts;
        std::vector<std::string> deletes;
        for (auto& [key, entry] : netted_) {
            (void)key;
            if (entry.is_delete) {
                deletes.push_back(std::move(entry.json));
            } else {
                upserts.push_back(std::move(entry.json));
            }
        }
        const std::size_t n = netted_.size();
        const auto t0 = std::chrono::steady_clock::now();
        try {
            exec_("BEGIN");
            // A netted key is in exactly one bucket, so upserts and deletes touch
            // disjoint keys - statement order between them does not matter.
            if (!upserts.empty()) {
                exec_(pgsql::build_insert_sql(opts_.table,
                                              opts_.columns,
                                              /*on_conflict=*/"update",
                                              /*conflict_columns=*/opts_.key_columns,
                                              /*update_columns=*/{},
                                              upserts,
                                              [this](std::string_view s) { return escape_(s); }));
            }
            if (!deletes.empty()) {
                exec_(pgsql::build_delete_sql(
                    opts_.table, opts_.key_columns, deletes, [this](std::string_view s) {
                        return escape_(s);
                    }));
            }
            exec_("COMMIT");
        } catch (...) {
            (void)PQexec(conn_.get(), "ROLLBACK");
            clink::metrics::connector::error_inc("postgres", "sink");
            netted_.clear();
            conn_.reset();  // drop a possibly-broken connection; re-open reconnects
            throw;          // job replays from the last checkpoint (keyed ops are idempotent)
        }
        const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
        clink::metrics::connector::records_out_inc("postgres", n);
        clink::metrics::connector::commit_latency_observe("postgres",
                                                          static_cast<std::uint64_t>(dt));
        netted_.clear();
    }

    void close() override {
        if (conn_) {
            flush();
            conn_.reset();
        }
    }

    std::string name() const override { return opts_.name; }

private:
    struct Entry {
        bool is_delete{false};
        std::string json;
    };

    // Net one changelog row into the pending map by its primary-key tuple.
    void net_(const std::string& json) {
        const auto j = clink::config::parse(json);
        if (!j.is_object()) {
            throw std::runtime_error(opts_.name + ": sink row is not a JSON object: " + json);
        }
        const auto& obj = j.as_object();

        // Classify the changelog kind (absent -> insert).
        bool is_delete = false;
        if (auto it = obj.find("__row_kind"); it != obj.end() && it->second.is_string()) {
            const std::string& k = it->second.as_string();
            is_delete = (k == "delete" || k == "update_before");
        }

        // Build the netting key from the primary-key columns.
        std::string key;
        for (const auto& kc : opts_.key_columns) {
            auto it = obj.find(kc);
            if (it == obj.end() || it->second.is_null()) {
                throw std::runtime_error(opts_.name + ": changelog row missing primary key '" + kc +
                                         "': " + json);
            }
            key += it->second.serialize(0);
            key.push_back('\x1f');  // unit separator - unambiguous tuple join
        }
        netted_[std::move(key)] = Entry{is_delete, json};
    }

    void exec_(const std::string& sql) {
        PGresult* r = PQexec(conn_.get(), sql.c_str());
        const auto st = r != nullptr ? PQresultStatus(r) : PGRES_FATAL_ERROR;
        if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK) {
            const std::string err =
                r != nullptr ? PQresultErrorMessage(r) : std::string(PQerrorMessage(conn_.get()));
            PQclear(r);
            throw std::runtime_error(opts_.name + ": " + sql + " failed: " + err);
        }
        PQclear(r);
    }

    std::string escape_(std::string_view in) {
        std::string out(in.size() * 2 + 1, '\0');
        int err = 0;
        const std::size_t len =
            PQescapeStringConn(conn_.get(), out.data(), in.data(), in.size(), &err);
        out.resize(len);
        return out;
    }

    PostgresJsonUpsertSinkOptions opts_;
    std::unique_ptr<PGconn, PgconnDeleter> conn_;
    std::map<std::string, Entry> netted_;  // primary-key tuple -> latest op
};

}  // namespace clink
