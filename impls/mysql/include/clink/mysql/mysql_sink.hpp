#pragma once

// MySQL sink: each input record is a JSON-object string (row_to_json_string on
// the SQL path); buffered records are written as ONE batched multi-row INSERT.
// `columns` (required) is the authoritative projection: each column is looked up
// by name in the row's JSON object (absent/null -> SQL NULL). mode='upsert'
// routes to INSERT ... ON DUPLICATE KEY UPDATE (needs a PRIMARY KEY / UNIQUE
// index on the target).
//
// Delivery is AT-LEAST-ONCE: a replay after a failed checkpoint re-runs the
// INSERT (append duplicates; upsert-by-key is idempotent = effectively-once).
// Flushed on count/byte/linger thresholds and every checkpoint barrier.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "clink/metrics/connector_metrics.hpp"
#include "clink/mysql/mysql_client.hpp"
#include "clink/mysql/mysql_sql.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink::mysql {

struct MysqlSinkOptions {
    ConnectOptions conn;
    std::string table;                        // target table (required)
    std::vector<std::string> columns;         // authoritative projection (required)
    bool upsert{false};                       // ON DUPLICATE KEY UPDATE
    std::vector<std::string> update_columns;  // upsert SET list (empty = all columns)
    std::size_t batch_records{1000};
    std::size_t max_bytes{0};              // byte-based flush threshold (0 = off)
    std::chrono::milliseconds max_age{0};  // linger (0 = off)
    std::string name{"mysql_sink"};
};

class MysqlSink : public Sink<std::string> {
public:
    explicit MysqlSink(MysqlSinkOptions opts) : opts_(std::move(opts)) {
        if (opts_.table.empty()) {
            throw std::runtime_error(opts_.name + ": 'table' is required");
        }
        if (opts_.columns.empty()) {
            throw std::runtime_error(opts_.name + ": 'columns' is required (the projection)");
        }
        if (opts_.batch_records == 0) {
            opts_.batch_records = 1;
        }
        // Validate identifiers up front (fail fast at build, not at first flush).
        (void)quote_ident(opts_.table);
        for (const auto& c : opts_.columns) {
            (void)quote_ident(c);
        }
        for (const auto& c : opts_.update_columns) {
            (void)quote_ident(c);
        }
    }

    void open() override { conn_ = std::make_unique<Connection>(opts_.conn); }

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
                build_insert_sql(opts_.table,
                                 opts_.columns,
                                 opts_.upsert,
                                 opts_.update_columns,
                                 pending_,
                                 [this](std::string_view s) { return conn_->escape(s); });
            conn_->exec(sql);
        } catch (...) {
            clink::metrics::connector::error_inc("mysql", "sink");
            pending_.clear();
            pending_bytes_ = 0;
            conn_.reset();  // drop a possibly-broken connection; re-open reconnects
            throw;          // job replays from the last checkpoint (at-least-once)
        }
        const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
        clink::metrics::connector::records_out_inc("mysql", n);
        clink::metrics::connector::bytes_out_inc("mysql", bytes);
        clink::metrics::connector::commit_latency_observe("mysql", static_cast<std::uint64_t>(dt));
        pending_.clear();
        pending_bytes_ = 0;
    }

    void close() override {
        flush();
        conn_.reset();
    }

    std::string name() const override { return opts_.name; }

private:
    bool linger_elapsed_() const {
        return opts_.max_age.count() > 0 && !pending_.empty() &&
               std::chrono::steady_clock::now() - first_buffered_at_ >= opts_.max_age;
    }

    MysqlSinkOptions opts_;
    std::unique_ptr<Connection> conn_;
    std::vector<std::string> pending_;
    std::size_t pending_bytes_{0};
    std::chrono::steady_clock::time_point first_buffered_at_{};
};

}  // namespace clink::mysql
