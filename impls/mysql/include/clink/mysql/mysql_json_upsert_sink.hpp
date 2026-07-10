#pragma once

// MysqlJsonUpsertSink - changelog-aware MySQL sink for mode='upsert'.
//
// The MySQL analogue of PostgresJsonUpsertSink. Consumes the clink changelog (a
// "__row_kind" field on each JSON row) and maintains a table by PRIMARY KEY:
//   insert / update_after  -> INSERT ... ON DUPLICATE KEY UPDATE  (upsert)
//   delete / update_before -> DELETE FROM <table> WHERE <pk> IN (...)
//   (a row with no __row_kind is an implicit insert)
//
// Within a flush the changelog is netted by primary key (last op wins) and the
// surviving upserts + deletes are applied in one transaction. EFFECTIVELY-ONCE
// on the sink table for a stable primary key and a deterministic defining query
// (keyed idempotent statements converge on replay). NOT two-phase commit.
//
// Lets a retracting SQL query (GROUP BY, TOP-N, outer join) maintain a MySQL
// table; the append-only mysql_sink drops the retract records.
//
// The upsert uses the target table's declared PRIMARY KEY / UNIQUE index (ON
// DUPLICATE KEY UPDATE); key_columns names the same key for the DELETE path.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "clink/connectors/sql_json_builder.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/mysql/mysql_client.hpp"
#include "clink/mysql/mysql_sql.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink::mysql {

struct MysqlJsonUpsertSinkOptions {
    ConnectOptions conn;
    std::string table;                        // target table (required)
    std::vector<std::string> columns;         // full projection for the upsert (required)
    std::vector<std::string> key_columns;     // PRIMARY KEY - the DELETE key (required)
    std::vector<std::string> update_columns;  // upsert SET list (empty = all columns)
    std::size_t batch_records{1000};
    std::string name{"mysql_upsert_sink"};
};

class MysqlJsonUpsertSink : public Sink<std::string> {
public:
    explicit MysqlJsonUpsertSink(MysqlJsonUpsertSinkOptions opts) : opts_(std::move(opts)) {
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
        (void)quote_ident(opts_.table);
        for (const auto& c : opts_.columns) {
            (void)quote_ident(c);
        }
        for (const auto& c : opts_.key_columns) {
            (void)quote_ident(c);
        }
    }

    void open() override { conn_ = std::make_unique<Connection>(opts_.conn); }

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
        if (conn_ == nullptr) {
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
            conn_->exec("START TRANSACTION");
            // A netted key is in exactly one bucket, so upserts and deletes touch
            // disjoint keys - statement order between them does not matter.
            if (!upserts.empty()) {
                conn_->exec(
                    build_insert_sql(opts_.table,
                                     opts_.columns,
                                     /*upsert=*/true,
                                     opts_.update_columns,
                                     upserts,
                                     [this](std::string_view s) { return conn_->escape(s); }));
            }
            if (!deletes.empty()) {
                conn_->exec(sqljson::build_delete_by_keys_sql(
                    opts_.table,
                    opts_.key_columns,
                    deletes,
                    [this](std::string_view s) { return conn_->escape(s); },
                    sqljson::kMysql));
            }
            conn_->exec("COMMIT");
        } catch (...) {
            clink::metrics::connector::error_inc("mysql", "sink");
            netted_.clear();
            conn_.reset();  // drop a possibly-broken connection; re-open reconnects
            throw;          // job replays from the last checkpoint (keyed ops are idempotent)
        }
        const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
        clink::metrics::connector::records_out_inc("mysql", n);
        clink::metrics::connector::commit_latency_observe("mysql", static_cast<std::uint64_t>(dt));
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

    void net_(const std::string& json) {
        const auto j = clink::config::parse(json);
        if (!j.is_object()) {
            throw std::runtime_error(opts_.name + ": sink row is not a JSON object: " + json);
        }
        const auto& obj = j.as_object();

        bool is_delete = false;
        if (auto it = obj.find("__row_kind"); it != obj.end() && it->second.is_string()) {
            const std::string& k = it->second.as_string();
            is_delete = (k == "delete" || k == "update_before");
        }

        std::string key;
        for (const auto& kc : opts_.key_columns) {
            auto it = obj.find(kc);
            if (it == obj.end() || it->second.is_null()) {
                throw std::runtime_error(opts_.name + ": changelog row missing primary key '" + kc +
                                         "': " + json);
            }
            it->second.serialize_into(key);
            key.push_back('\x1f');
        }
        netted_[std::move(key)] = Entry{is_delete, json};
    }

    MysqlJsonUpsertSinkOptions opts_;
    std::unique_ptr<Connection> conn_;
    std::map<std::string, Entry> netted_;
};

}  // namespace clink::mysql
